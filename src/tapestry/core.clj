(ns tapestry.core
  "Core namespace of Tapestry — structured concurrency over core.async fibers.

  Tapestry models a unit of concurrent work as a `fiber`: a derefable handle
  whose body runs on its own thread. Results, errors, timeouts, and
  cancellation all flow through that handle.

  `interrupt!` and `timeout!` settle the fiber's result and interrupt its
  thread, so a body blocked in `Thread/sleep`, a deref, or another
  interruptible wait throws `InterruptedException`."
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
  is `[:ok v]` or `[:err Throwable]`. Set by `tapestry.experimental/with-scope`
  and captured by each fiber at spawn time. Jolt promises are not watchable,
  so the fiber reports its own completion."
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
;; A fiber takes a permit before running its body and puts it back after.
(defn ^:no-doc make-semaphore
  [n]
  (when-not (pos? n)
    (throw (ex-info "max-parallelism must be a positive integer"
                    {:max-parallelism n})))
  (let [permits (a/chan n)]
    (dotimes [_ n] (a/>!! permits :permit))
    permits))


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
     alive*        ;; atom: true until the body's thread is done with the body
     err*          ;; atom: Throwable once the fiber has errored/been cancelled
     settled*      ;; atom: false until exactly one settlement claims the fiber
     done          ;; core.async channel closed on settlement
     on-settle     ;; the scope's completion callback at spawn time, or nil
     thread]       ;; the java.lang.Thread running the body
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
  "Settle `fiber` with `outcome` exactly once, returning true for the caller
  that won. The winner records fiber state and notifies the fiber's scope
  BEFORE delivering the result promise, so waiters (deref, `alts`) never
  observe a settled result whose scope state (first-error/first-result) is not
  yet recorded."
  [^Fiber fiber [tag val :as outcome]]
  (when (compare-and-set! (.settled* fiber) false true)
    (when (= :err tag)
      (swap! (.err* fiber) (fn [old] (or old val))))
    (when-let [on-settle (.on-settle fiber)] (on-settle fiber outcome))
    (a/close! (.done fiber))
    (deliver (.result fiber) outcome)
    true))

(defn- cancel!
  "Settle `fiber` with `outcome` and, if that settled it, interrupt its thread."
  [^Fiber fiber outcome]
  (when (settle! fiber outcome)
    (.interrupt ^Thread (.thread fiber))))

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
  "Cancel the provided `fiber` and interrupt its thread, so a blocking call in
  the body throws `InterruptedException`. A subsequent `deref` throws
  `ExceptionInfo` with `{:type :tapestry.core/interrupted}`. No effect on a
  fiber that has already completed.

  Returns the provided `fiber` for chaining."
  [^Fiber fiber]
  (cancel! fiber [:err (interrupted-ex)])
  fiber)

(defn timeout!
  "Set the provided `timeout` on the `fiber`. If the fiber has not completed
  when it elapses, it is cancelled and its thread interrupted (see
  `interrupt!`).

  Without a `default`, a `deref` after the timeout throws `ExceptionInfo` with
  `{:type :tapestry.core/timeout}`. With a `default`, the `deref` returns
  `default` instead.

  Accepts either a number of millis or a `java.time.Duration`.

  Returns the provided `fiber` for chaining."
  ([fiber timeout]
   (timeout! fiber timeout ::no-default))
  ([^Fiber fiber timeout default]
   (let [ms   (->ms timeout)
         done (.done fiber)]
     (a/thread
       (let [[_ port] (a/alts!! [done (a/timeout ms)] :priority true)]
         (when-not (identical? port done)
           (cancel! fiber (if (identical? ::no-default default)
                            [:err (timeout-ex)]
                            [:ok default])))))
     fiber)))

(defn- acquire-permit
  "Take a permit from `sem`, giving up if `fiber` settles first. Returns true
  when a permit was taken."
  [^Fiber fiber sem]
  (let [done (.done fiber)
        [_ port] (a/alts!! [done sem] :priority true)]
    (not (identical? port done))))

(defn ^:no-doc run-fiber!
  "Body of a fiber's thread: wait for a permit if `sem` is set, run `f`, and
  settle `fiber` with the outcome. A fiber cancelled before it starts, or
  while queued for a permit, never runs `f`."
  [^Fiber fiber sem f]
  (let [permit? (try
                  (and (not @(.settled* fiber))
                       (or (nil? sem) (acquire-permit fiber sem)))
                  (catch Throwable _ false))]
    (if-not permit?
      (reset! (.alive* fiber) false)
      (let [outcome (try
                      [:ok (f)]
                      (catch Throwable e
                        (swap! (.err* fiber) (fn [old] (or old e)))
                        [:err e])
                      (finally
                        ;; put! never blocks, so an interrupt flag left set by
                        ;; the body can't abort returning the permit.
                        (when sem (a/put! sem :permit))
                        (reset! (.alive* fiber) false)))]
        (settle! fiber outcome)))))

(defn ^:no-doc spawn-fiber!
  "Start `f` on a new daemon thread and return its `Fiber`. The fiber is
  registered with the current scope and has any `with-timeout` applied before
  its thread starts."
  [f]
  (let [sem    *local-semaphore*
        ;; Not a promise: a fiber cancelled before its thread starts has the
        ;; interrupt flag set, and a promise deref would throw on it.
        ;; `.start` publishes the write to the new thread.
        holder (volatile! nil)
        thread (Thread. ^Runnable (bound-fn* (fn [] (run-fiber! @holder sem f))))
        fiber  (Fiber. (promise) (atom true) (atom nil) (atom false)
                       (a/chan) *scope-notify!* thread)]
    (vreset! holder fiber)
    (.setDaemon thread true)
    (when *scope-register!* (*scope-register!* fiber))
    (when *local-timeout* (timeout! fiber *local-timeout*))
    (.start thread)
    fiber))

(defmacro fiber
  "Execute `body` on its own thread, returning a derefable `Fiber`.

  Honors any active `with-max-parallelism` semaphore and `with-timeout`, and
  registers the fiber with the current scope (`with-scope`) if one is active."
  [& body]
  `(spawn-fiber! (fn [] ~@body)))

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
