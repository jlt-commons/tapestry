(ns tapestry.experimental.scope
  "Experimental structured concurrency primitives for Tapestry.

  Provides `with-scope` for structured lifecycle management of fibers,
  unifying timeout, max-parallelism, and shutdown policies into a single
  scope construct."
  (:require [tapestry.core :as tc]
            [clojure.core.async :as a])
  (:import [clojure.lang PersistentQueue]))

(set! *warn-on-reflection* true)

;; ---------------------------------------------------------------------------
;; Structured scopes
;; ---------------------------------------------------------------------------

(defrecord ^:no-doc Scope [shutdown-policy fibers first-result first-error])

(defn ^:no-doc make-scope
  "Create a new scope with the given shutdown policy."
  [shutdown-policy]
  (->Scope shutdown-policy (atom PersistentQueue/EMPTY) (promise) (promise)))

(defn- deliver-first
  "Deliver `v` to `p` only if it has not already been delivered."
  [p v]
  (when-not (realized? p) (deliver p v)))

(defn- interrupt-siblings
  "Interrupt every other alive fiber in the scope."
  [scope self]
  (doseq [f @(:fibers scope)]
    (when (and (not (identical? f self)) (tc/alive? f))
      (tc/interrupt! f))))

(defn ^:no-doc register-fiber!
  "Register a fiber with the scope. Completion is handled by `notify-fiber!`,
  which the fiber captures from `*scope-notify!*` when it is spawned (Jolt
  promises are not watchable)."
  [scope fiber]
  (swap! (:fibers scope) conj fiber)
  ;; If the scope has already shut down (an earlier fiber completed before this
  ;; one was registered), interrupt immediately.
  (case (:shutdown-policy scope)
    :on-success (when (realized? (:first-result scope)) (tc/interrupt! fiber))
    :on-failure (when (realized? (:first-error scope))  (tc/interrupt! fiber))
    nil))

(defn ^:no-doc notify-fiber!
  "Called by a fiber (via `*scope-notify!*`) when it completes, with the fiber
  and its `[:ok v]` / `[:err Throwable]` outcome."
  [scope fiber outcome]
  (let [[tag val] outcome]
    (case (:shutdown-policy scope)
      :on-failure
      (when (= :err tag)
        (deliver-first (:first-error scope) val)
        (interrupt-siblings scope fiber))

      :on-success
      (if (= :ok tag)
        (do (deliver-first (:first-result scope) val)
            (interrupt-siblings scope fiber))
        ;; Record the failure so callers like `alts` can surface it when no
        ;; operation succeeds; siblings keep running (one may still succeed).
        (deliver-first (:first-error scope) val))

      nil)))

(defn ^:no-doc await-all!
  "Wait for every registered fiber's thread to finish, including fibers that
  running fibers register while we wait."
  [scope]
  (loop [joined 0]
    (let [fibers @(:fibers scope)]
      (when (< joined (count fibers))
        (doseq [^tapestry.core.Fiber fiber (drop joined fibers)]
          (.join ^Thread (.thread fiber)))
        (recur (count fibers))))))

(defn ^:no-doc shutdown-all!
  "Interrupt all fibers that are still alive in the scope."
  [scope]
  (doseq [fiber @(:fibers scope)]
    (when (tc/alive? fiber) (tc/interrupt! fiber))))

(defn ^:no-doc throw-if-failed!
  "If the scope has a recorded error and the policy is :on-failure, throw it."
  [scope]
  (when (= :on-failure (:shutdown-policy scope))
    (let [err (:first-error scope)]
      (when (realized? err)
        (let [e @err]
          (throw (if (instance? Throwable e)
                   e
                   (ex-info "Scope fiber failed" {:error e}))))))))

(defmacro with-scope
  "Execute body within a structured scope that manages fiber lifecycles.

  Takes an options map with the following keys:
    :shutdown        - Shutdown policy: :on-failure or :on-success (optional)
    :timeout         - Timeout in millis or a Duration applied to all fibers (optional)
    :max-parallelism - Maximum number of fibers running in parallel (optional)

  All fibers created with `tapestry.core/fiber` within the body are
  automatically registered with the scope.

  On scope exit:
    - All registered fibers are awaited (their results settle)
    - If :shutdown is :on-failure and any fiber failed, the error is propagated
    - If :shutdown is :on-success, the first successful result is available

  Example:
    (with-scope {:timeout 5000 :shutdown :on-failure :max-parallelism 4}
      (let [a (fiber (do-a))
            b (fiber (do-b))]
        {:a @a :b @b}))"
  [opts & body]
  `(let [opts#      ~opts
         shutdown#  (:shutdown opts#)
         timeout#   (:timeout opts#)
         max-par#   (:max-parallelism opts#)
         scope#     (make-scope shutdown#)
         register#  (fn [fiber#] (register-fiber! scope# fiber#))
         notify#    (fn [fiber# outcome#] (notify-fiber! scope# fiber# outcome#))
         sem#       (when max-par#
                       (let [n# (int max-par#)]
                         (when-not (pos? n#)
                           (throw (ex-info "max-parallelism must be a positive integer"
                                           {:max-parallelism n#})))
                         (let [c# (a/chan n#)]
                           (dotimes [_# n#] (a/>!! c# :permit))
                           c#)))]
     (binding [tc/*scope*           scope#
               tc/*scope-register!* register#
               tc/*scope-notify!*   notify#
               tc/*local-timeout*   (or timeout# tc/*local-timeout*)]
       (binding [tc/*local-semaphore* (or sem# tc/*local-semaphore*)]
         (try
           (let [result# (do ~@body)]
             (await-all! scope#)
             (throw-if-failed! scope#)
             result#)
           (catch Throwable t#
             ;; If a fiber had already failed, prefer its error: the caught
             ;; throwable is often just the cascade from derefing a sibling
             ;; that failure cancelled. Check before shutting down, since the
             ;; interrupts shutdown-all! delivers are recorded as failures too.
             (let [failed?# (and (= :on-failure shutdown#)
                                 (realized? (:first-error scope#)))]
               (shutdown-all! scope#)
               (await-all! scope#)
               (throw (if failed?# @(:first-error scope#) t#)))))))))
