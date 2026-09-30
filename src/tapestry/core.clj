(ns tapestry.core
  "Core namespace of Tapestry — structured concurrency over core.async fibers.

  Tapestry models a unit of concurrent work as a `fiber`: a derefable handle
  whose body runs on its own thread. Results, errors, timeouts, and
  cancellation all flow through that handle.

  On Jolt there is no JVM thread interruption, so `interrupt!`/`timeout!`
  deliver a cancellation to the fiber's result (a `deref` then sees it) but
  cannot forcibly stop a body blocked on `Thread/sleep`. Cooperative bodies —
  those that park on channel operations or check a cancellation flag — stop
  promptly; a body pinned in a blocking call runs to completion in the
  background while its result is reported as cancelled."
  (:require [clojure.core.async :as a])
  (:refer-clojure :exclude [send]))

(set! *warn-on-reflection* true)

(def ^{:dynamic true :no-doc true} *local-semaphore*
  "A core.async channel of permits used to coordinate max-parallelism, or nil."
  nil)

(def ^{:dynamic true :no-doc true} *local-timeout*
  "A timeout (number of millis or `java.time.Duration`) applied to newly
  spawned fibers, or nil."
  nil)

(def ^{:dynamic true :no-doc true} *scope*
  "The current structured scope, if any. Set by `tapestry.experimental/with-scope`."
  nil)

(def ^{:dynamic true :no-doc true} *scope-register!*
  "A function called to register a fiber with the current scope.
  Set by `tapestry.experimental/with-scope`. Called with a single Fiber argument."
  nil)

(def ^{:dynamic true :no-doc true} *scope-notify!*
  "A function called when a fiber completes, with [fiber outcome] where outcome
  is `[:ok v]` or `[:err Throwable]`. Set by `tapestry.experimental/with-scope`.
  Jolt promises are not watchable, so the fiber reports its own completion."
  nil)

(def ^:no-doc on-error
  "The function that will be called when an error is encountered.

  Called with the signature of: `e msg`"
  println)

(defn set-stream-error-handler!
  "Set a function to be called when an error occurs in a tapestry
  returned stream.

  By default will println. Set to `nil` to do nothing

  Calls `(f err msg)`."
  [f]
  (alter-var-root #'on-error (constantly f)))

;; ---------------------------------------------------------------------------
;; Concurrency primitives (replace java.util.concurrent)
;; ---------------------------------------------------------------------------

;; A counting semaphore built from a buffer-n channel prefilled with permits.
;; `acquire` takes a permit (blocking when none remain), `release` returns one.
(defn ^:no-doc make-semaphore
  [n]
  (when-not (pos? n)
    (throw (ex-info "max-parallelism must be a positive integer"
                    {:max-parallelism n})))
  (let [permits (a/chan n)]
    (dotimes [_ n] (a/>!! permits :permit))
    permits))

(defn ^:no-doc acquire-semaphore [sem] (a/<!! sem))
(defn ^:no-doc release-semaphore [sem] (a/>!! sem :permit))

;; The JVM's `TimeoutException`/`InterruptedException` have no constructors on
;; Jolt's shim, so cancellation surfaces as an `ex-info` with a `:type` tag.
;; Callers catch `clojure.lang.ExceptionInfo` and inspect `ex-data`.
(defn- interrupted-ex []
  (ex-info "Fiber interrupted" {:type ::interrupted}))
(defn- timeout-ex []
  (ex-info "Fiber timed out" {:type ::timeout}))

;; A millis value from a number or a `java.time.Duration`. `java.time.Duration`
;; is shimmed in Jolt core, so `.toMillis` works on either runtime.
(defn- ^long ->ms [t]
  (long
    (cond (number? t)                          t
          (instance? java.time.Duration t)     (.toMillis ^java.time.Duration t)
          (nil? t)                             0
          :else                                t)))

(def ^:private ^:no-doc not-delivered
  "Sentinel returned by a timed `deref` of a `promise` that never delivered."
  (Object.))

;; ---------------------------------------------------------------------------
;; Fiber
;; ---------------------------------------------------------------------------

(deftype ^:no-doc Fiber
    [result        ;; clojure.core/promise: delivered [:ok v] | [:err Throwable]
     alive*        ;; atom: true while the body's thread is running
     err*          ;; atom: Throwable once the fiber has errored/been cancelled
     settled*]     ;; atom: false until exactly one settlement claims the fiber
  clojure.lang.IDeref
  (deref [_]
    (let [[tag val] @result]
      (when (= :err tag) (throw ^Throwable val))
      val))
  clojure.lang.IBlockingDeref
  (deref [_ ms default]
    (let [r (deref result ms not-delivered)]
      (if (identical? not-delivered r)
        default
        (let [[tag val] r]
          (when (= :err tag) (throw ^Throwable val))
          val))))
  clojure.lang.IPending
  (isRealized [_]
    (realized? result)))

(defmethod print-method Fiber [^Fiber v ^java.io.Writer w]
  (let [done? (realized? (.result v))]
    (.write w "#tapestry/fiber {")
    (.write w (str ":is-alive " (boolean @(.alive* v))))
    (when done?
      (let [[tag val] @(.result v)]
        (when (and (= :ok tag) (some? val))
          (.write w " :val ")
          (print-method val w))
        (when (= :err tag)
          (.write w " :error ")
          (print-method val w))))
    (.write w "}")))

(defn ^:no-doc settle!
  "Settle `fiber` with `outcome` exactly once. The winner of the CAS runs
  fiber state updates and scope bookkeeping BEFORE delivering the result
  promise, so waiters (deref, scope await-all!) never observe a settled
  result whose scope state (first-error/first-result) is not yet recorded."
  [^Fiber fiber [tag val :as outcome]]
  (when (compare-and-set! (.settled* fiber) false true)
    (when (= :err tag)
      (swap! (.err* fiber) (fn [old] (or old val))))
    (when *scope-notify!* (*scope-notify!* fiber outcome))
    (deliver (.result fiber) outcome)))

(defn alive?
  "Return whether the provided `fiber` is still running."
  [^Fiber fiber]
  @(.alive* fiber))

(defn errored?
  "Return whether the provided `fiber` has errored (or been cancelled)."
  [^Fiber fiber]
  (some? @(.err* fiber)))

(defn fiber-error
  "Return the error of the provided `fiber` if it has errored, otherwise nil."
  [^Fiber fiber]
  (when-not (alive? fiber) @(.err* fiber)))

(defn interrupt!
  "Cancel the provided `fiber`. A subsequent `deref` throws an
  `InterruptedException`; callbacks registered on the fiber fire with the
  cancellation.

  On Jolt the cancellation is delivered to the result, but a body blocked on a
  non-cooperative call (e.g. `Thread/sleep`) is not forcibly stopped — it runs
  to completion in the background while its result is reported as cancelled.

  Returns the provided `fiber` for chaining."
  [^Fiber fiber]
  (settle! fiber [:err (interrupted-ex)])
  fiber)

(defn timeout!
  "Set the provided `timeout` on the `fiber`. When it elapses the fiber is
  cancelled (see `interrupt!`).

  Without a `default`, a `deref` after the timeout throws
  `java.util.concurrent.TimeoutException`. With a `default`, the `deref`
  returns `default` instead.

  Accepts either a number of millis or a `java.time.Duration`.

  Returns the provided `fiber` for chaining."
  ([^Fiber fiber timeout]
   (let [ms (->ms timeout)]
      (a/thread
        (a/<!! (a/timeout ms))
        (settle! fiber [:err (timeout-ex)]))
      fiber))
  ([^Fiber fiber timeout default]
   (let [ms (->ms timeout)]
     (a/thread
       (a/<!! (a/timeout ms))
       (settle! fiber [:ok default]))
     fiber)))

(defmacro fiber
  "Execute `body` on its own thread, returning a derefable `Fiber`.

  Honors any active `with-max-parallelism` semaphore and `with-timeout`, and
  registers the fiber with the current scope (`with-scope`) if one is active."
  [& body]
  `(let [result# (promise)
         alive?# (atom true)
         err*#   (atom nil)
         settled*# (atom false)
         fiber*# (promise)]
     (a/thread
       (when *local-semaphore* (acquire-semaphore *local-semaphore*))
       (let [outcome# (try
                        [:ok (do ~@body)]
                        (catch Throwable e#
                          (swap! err*# (fn [old#] (or old# e#)))
                          [:err e#])
                         (finally
                           (when *local-semaphore* (release-semaphore *local-semaphore*))
                           (reset! alive?# false)))]
         (settle! @fiber*# outcome#)))
     (let [fiber# (Fiber. result# alive?# err*# settled*#)]
       (deliver fiber*# fiber#)
       (when *scope-register!* (*scope-register!* fiber#))
       (when *local-timeout* (timeout! fiber# *local-timeout*))
       fiber#)))

(defmacro with-max-parallelism
  "Executes the provided body such that at most `n` fibers spawned within it
  will run in parallel."
  [n & body]
  `(binding [*local-semaphore* (make-semaphore (int ~n))]
     ~@body))

(defmacro with-timeout
  "Executes all newly spawned fibers with the provided `timeout`.

  Accepts either a number (used as millis) or `java.time.Duration`."
  [timeout & body]
  `(binding [*local-timeout* ~timeout]
     ~@body))

(defmacro fiber-loop
  "Execute a body inside a loop."
  [bindings & body]
  `(fiber (loop ~bindings ~@body)))

(defmacro seq->stream
  "Runs an expression that returns a (presumably lazy) sequence on a dedicated
  thread and returns a channel onto which the results are put. The channel is
  closed when the sequence is exhausted."
  [expr]
  `(let [out# (a/chan)]
     (a/thread
       (try
         (run! #(a/>!! out# %) ~expr)
         (finally (a/close! out#))))
     out#))

(defmacro pfor
  "Behaves identically to `clojure.core.for` but runs the body in parallel
  using fibers.

  Note that bindings in `:let` and `:when` will not be evaluated in parallel.

  Forces evaluation of the sequence (ie. this is no longer lazy)."
  [seq-exprs body-expr]
  `(->> (for ~seq-exprs
          (fiber
            ~body-expr))
        (doall)
        (map deref)
        (doall)))

(defn periodically
  "Return a channel that emits `(f)` every `period` millis, starting after an
  optional `initial-delay`. The channel closes when it is consumed to
  completion or `f` throws.

  Accepts numbers (millis) or `java.time.Duration` for `period` and
  `initial-delay`. With no initial delay, runs immediately."
  ([period f] (periodically period nil f))
  ([period initial-delay f]
   (let [initial-ms (->ms initial-delay)
         poll-ms    (->ms period)
         out        (a/chan)]
     (a/thread
       (try
         (a/<!! (a/timeout initial-ms))
         (loop []
           (when (a/>!! out (f))
             (a/<!! (a/timeout poll-ms))
             (recur)))
         (catch Exception e#
           (when on-error (on-error e# "Error in periodically f")))
         (finally (a/close! out))))
     out)))

;; ---------------------------------------------------------------------------
;; asyncly — concurrent, order-independent map
;; ---------------------------------------------------------------------------

(defn- ^:no-doc asyncly-seq
  "Unbounded parallelism over a seqable `s`; returns a seq."
  [f s]
  (let [result (a/chan)
        error* (promise)
        src    (a/to-chan s)
        procs  (atom [])]
    (a/thread
      (loop []
        (when-some [item (a/<!! src)]
          (if (realized? error*)
            (a/close! src)
            (let [p (a/thread
                      (try
                        (when-not (realized? error*)
                          (when-some [v (f item)]
                            (a/>!! result v)))
                        (catch Exception e#
                          (when on-error (on-error e# "Exception in asyncly function"))
                          (deliver error* e#)
                          (a/close! src))))]
              (swap! procs conj p)
              (recur)))))
      (run! a/<!! @procs)
      (a/close! result))
    (concat (a/<!! (a/into [] result))
            (lazy-seq (when (realized? error*) (throw (deref error* 0 nil)))))))

(defn- ^:no-doc asyncly-stream
  "Unbounded parallelism over a channel `s`; returns a result channel."
  [f s]
  (let [result   (a/chan)
        err-atom (atom nil)]
    (a/thread
      (let [procs (atom [])]
        (loop []
          (when-some [item (a/<!! s)]
            (when-not @err-atom
              (let [p (a/thread
                        (try
                          (when-not @err-atom (when-some [v (f item)] (a/>!! result v)))
                          (catch Exception e#
                            (when on-error (on-error e# "Exception in asyncly function"))
                            (reset! err-atom e#)
                            (a/close! s)
                            (a/close! result))))]
                (swap! procs conj p)))
            (recur)))
        ;; Wait for every spawned worker to finish before closing, so an
        ;; in-flight worker's put is never dropped by an early close.
        (run! a/<!! @procs))
      (a/close! result))
    result))

(defn- ^:no-doc asyncly-seq-n
  "Bounded (`n`) parallelism over a seqable `s`; returns a seq."
  [n f s]
  (let [result  (a/chan (a/buffer (max 1 n)))
        error*  (promise)
        src     (a/to-chan s)
        work    (a/chan (max 1 n))
        workers (atom n)]
    (dotimes [_ n]
      (a/thread
        (loop []
          (when-some [v (a/<!! work)]
            (try
              (when-not (realized? error*) (when-some [v (f v)] (a/>!! result v)))
              (catch Exception e#
                (when on-error (on-error e# "Error in asyncly callback"))
                (deliver error* e#)
                (a/close! work)
                (a/close! result)))
            (recur)))
        (when (zero? (swap! workers dec))
          (a/close! result))))
    (a/thread
      (loop []
        (when-some [v (a/<!! src)]
          (when-not (realized? error*)
            (a/>!! work v)
            (recur))))
      (a/close! work))
    (concat (a/<!! (a/into [] result))
            (lazy-seq (when (realized? error*) (throw (deref error* 0 nil)))))))

(defn- ^:no-doc asyncly-stream-n
  "Bounded (`n`) parallelism over a channel `s`; returns a result channel."
  [n f s]
  (let [result   (a/chan)
        err-atom (atom nil)
        work     (a/chan (max 1 n))
        workers  (atom n)]
    (dotimes [_ n]
      (a/thread
        (loop []
          (when-some [v (a/<!! work)]
            (try
              (when-not @err-atom (when-some [v (f v)] (a/>!! result v)))
              (catch Exception e#
                (when on-error (on-error e# "Error in asyncly callback"))
                (reset! err-atom e#)
                (a/close! work)
                (a/close! s)))
            (recur)))
        (when (zero? (swap! workers dec))
          (a/close! result))))
    (a/thread
      (loop []
        (when-some [v (a/<!! s)]
          (when-not @err-atom
            (a/>!! work v)
            (recur))))
      (a/close! work))
    result))

(defn asyncly
  "Applies mapping function `f` over the provided channel or seq `s`.

  Returns a channel (when `s` is a channel) or a seq (when `s` is a seqable)
  in which items are emitted after `f` finishes, in any order.

  With one arity, uses unbounded parallelism (or the max parallelism set via
  `with-max-parallelism`). With a numeric `n`, limits to `n` concurrent calls."
  ([f s]
   (if (seqable? s) (asyncly-seq f s) (asyncly-stream f s)))
  ([n f s]
   (if (seqable? s) (asyncly-seq-n n f s) (asyncly-stream-n n f s))))

(defn parallelly
  "Maps `f` over the channel or seq `s` with up to `n` items occurring in
  parallel, preserving order.

  With one arity, uses unbounded parallelism (or the max parallelism set via
  `with-max-parallelism`). Returns a channel when `s` is a channel, else a seq."
  ([f s]
   (let [stream? (not (seqable? s))
         items   (if stream? (a/<!! (a/into [] s)) (seq s))
         results (->> items (mapv #(fiber (f %))) (mapv deref))]
     (if stream? (a/to-chan results) results)))
  ([n f s]
   (let [seq?    (seqable? s)
         items   (if seq? (seq s) (a/<!! (a/into [] s)))
         sem     (make-semaphore (max 1 n))
         results (binding [*local-semaphore* sem]
                   (->> items (mapv #(fiber (f %))) (mapv deref)))]
     (if seq? results (a/to-chan results)))))

(defn send
  "Dispatch an agent action via a dedicated thread (the Jolt analog of a loom
  virtual thread). See `clojure.core/send`."
  [a f & args]
  (apply clojure.core/send a f args))
