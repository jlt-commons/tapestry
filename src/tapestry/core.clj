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
  while queued for a permit, never runs `f`. Calls `on-exit`, if given, last
  and in every case."
  [^Fiber fiber sem f on-exit]
  (try
    (let [permit? (try
                    (and (not @(.settled* fiber))
                         (or (nil? sem) (acquire-permit fiber sem)))
                    (catch Throwable _ false))]
      (cond
        (not permit?)
        (do (reset! (.alive* fiber) false)
            (settle! fiber [:err (interrupted-ex)]))

        ;; cancelled while the permit was being handed over
        @(.settled* fiber)
        (do (when sem (a/put! sem :permit))
            (reset! (.alive* fiber) false))

        :else
        (let [outcome (try
                        [:ok (f)]
                        (catch Throwable e
                          (swap! (.err* fiber) (fn [old] (or old e)))
                          [:err e])
                        (finally
                          ;; put! never blocks, so an interrupt flag left set
                          ;; by the body can't abort returning the permit.
                          (when sem (a/put! sem :permit))
                          (reset! (.alive* fiber) false)))]
          (settle! fiber outcome))))
    (finally
      (when on-exit (on-exit)))))

(defn ^:no-doc spawn-fiber!
  "Start `f` on a new daemon thread and return its `Fiber`. The fiber is
  registered with the current scope and has any `with-timeout` applied before
  its thread starts. `on-exit`, if given, is called on the fiber's thread
  when it is done, whether or not `f` ran."
  ([f] (spawn-fiber! f nil))
  ([f on-exit]
   (let [sem    *local-semaphore*
         ;; Not a promise: a fiber cancelled before its thread starts has the
         ;; interrupt flag set, and a promise deref would throw on it.
         ;; `.start` publishes the write to the new thread.
         holder (volatile! nil)
         thread (Thread. ^Runnable (bound-fn* (fn [] (run-fiber! @holder sem f on-exit))))
         fiber  (Fiber. (promise) (atom true) (atom nil) (atom false)
                        (a/chan) *scope-notify!* thread)]
     (vreset! holder fiber)
     (.setDaemon thread true)
     (when *scope-register!* (*scope-register!* fiber))
     (when *local-timeout* (timeout! fiber *local-timeout*))
     (.start thread)
     fiber)))

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
  closed when the sequence is exhausted, or after an error realizing it is
  passed to the stream error handler."
  [expr]
  `(let [out# (a/chan)]
     (a/thread
       (try
         (run! #(a/>!! out# %) ~expr)
         (catch Exception e#
           (when on-error (on-error e# "Error in seq->stream")))
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

;; A work group lets asyncly/parallelly stop their calls on the first error.
;; Calls register their thread only once their body starts, so a fiber still
;; queued for a permit is never interrupted: it would settle as cancelled,
;; which an enclosing :on-failure scope would take for the real error.
;; Instead it sees the group stopped when it starts and skips its body.

(defn- work-group []
  {:stopped (atom false) :running (atom #{})})

(defn- run-in-group
  "Run `(f)` on the calling thread as part of `group`, unless the group has
  stopped, in which case return `stopped`."
  [{:keys [stopped running]} f stopped-val]
  (let [t (Thread/currentThread)]
    (swap! running conj t)
    (try
      (if @stopped stopped-val (f))
      (finally (swap! running disj t)))))

(defn- stop-group!
  "Stop `group` and interrupt its running calls, other than the caller's.
  Returns true for the caller that stopped it."
  [{:keys [stopped running]}]
  (when (compare-and-set! stopped false true)
    (doseq [^Thread t @running]
      (when-not (identical? t (Thread/currentThread))
        (.interrupt t)))
    true))

;; ---------------------------------------------------------------------------
;; asyncly — concurrent, order-independent map
;; ---------------------------------------------------------------------------

(defn- run-asyncly!
  "Map `f` over channel `src` onto channel `result` on fibers: `n` workers
  pulling from `src`, or a fiber per item when `n` is nil. On the first error,
  reports it to `on-error`, closes `src` and `result`, and interrupts the
  in-flight calls. Otherwise `result` closes once every fiber has exited.
  Returns an atom holding the first error, if any."
  [n f src result]
  (let [err    (atom nil)
        group  (work-group)
        ;; the dispatcher plus every fiber that hasn't exited
        live   (atom 1)
        exit!  #(when (zero? (swap! live dec)) (a/close! result))
        fail!  (fn [e]
                 (when (compare-and-set! err nil e)
                   (when on-error (on-error e "Exception in asyncly function"))
                   (a/close! src)
                   (a/close! result)
                   (stop-group! group)))
        call!  (fn [item]
                 (when-some [v (f item)] (a/>!! result v)))
        ;; Errors after the first, including the interrupts fail! causes, are
        ;; dropped, so the fibers themselves always settle cleanly.
        spawn! (fn [body]
                 (swap! live inc)
                 (spawn-fiber! #(try (run-in-group group body nil)
                                     (catch Throwable e (when-not @err (fail! e))))
                               exit!))]
    (if n
      (do (dotimes [_ (max 1 n)]
            (spawn! #(loop []
                       (when-some [item (a/<!! src)]
                         (when-not @err
                           (call! item)
                           (recur))))))
          (exit!))
      (a/thread
        (loop []
          (when-some [item (a/<!! src)]
            (when-not @err
              (spawn! #(call! item))
              (recur))))
        (exit!)))
    err))

(defn- asyncly-seq [n f s]
  (let [result (a/chan)
        err    (run-asyncly! n f (a/to-chan s) result)]
    (concat (a/<!! (a/into [] result))
            (lazy-seq (when-let [e @err] (throw e))))))

(defn- asyncly-stream [n f s]
  (let [result (a/chan)]
    (run-asyncly! n f s result)
    result))

(defn asyncly
  "Applies mapping function `f` over the provided channel or seq `s`.

  Returns a channel (when `s` is a channel) or a seq (when `s` is a seqable)
  in which items are emitted after `f` finishes, in any order.

  With one arity, uses unbounded parallelism (or the max parallelism set via
  `with-max-parallelism`). With a numeric `n`, limits to `n` concurrent calls."
  ([f s]
   (if (seqable? s) (asyncly-seq nil f s) (asyncly-stream nil f s)))
  ([n f s]
   (if (seqable? s) (asyncly-seq n f s) (asyncly-stream n f s))))

(defn- guarded
  "A thunk calling `(f x)` as part of `group` that returns `[:ok v]` or
  `[:err e]` rather than throwing (nil if the group stopped first)."
  [group f x]
  (fn []
    (try (run-in-group group (fn [] [:ok (f x)]) nil)
         (catch Throwable e [:err e]))))

(defn- outcome
  "The tagged outcome of a `guarded` fiber, or `[:err e]` if the fiber itself
  was cancelled."
  [fiber]
  (try @fiber (catch Throwable e [:err e])))

(defn- spawn-in-order!
  "Start a `guarded` fiber per item, in input order, putting each fiber onto
  `pending`; `take-item` returns the next item or `::done`. With `n`, at most
  `n` run at once, and the permit for an item is taken here, before its fiber
  starts. If the fibers raced for permits themselves, a later item could run
  ahead of the earlier one the caller is waiting on, and with every permit held
  by later items the call stalls. The permit goes back when the fiber exits,
  whether or not its body ran. Stops when the group stops or `pending` closes,
  then closes `pending`."
  [n f take-item pending group]
  (let [sem (when n (make-semaphore (max 1 n)))]
    (a/thread
      ;; bounded calls take their own permits, so the fibers mustn't also wait
      ;; on an enclosing with-max-parallelism
      (binding [*local-semaphore* (if sem nil *local-semaphore*)]
        (loop []
          (let [item (take-item)]
            (when-not (or (identical? ::done item) @(:stopped group))
              (when sem (a/<!! sem))
              (let [fiber (spawn-fiber! (guarded group f item)
                                        (when sem #(a/put! sem :permit)))]
                (when (a/>!! pending fiber)
                  (recur)))))))
      (a/close! pending))))

(defn- parallelly-seq [n f s]
  (let [group   (work-group)
        items   (atom (seq s))
        ;; only the spawner thread takes items
        take    #(if-let [[x & more] @items]
                   (do (reset! items more) x)
                   ::done)
        pending (a/chan (if n (max 1 n) Integer/MAX_VALUE))]
    (spawn-in-order! n f take pending group)
    (loop [acc (transient [])]
      (if-some [fiber (a/<!! pending)]
        (let [[tag v] (outcome fiber)]
          (if (= :ok tag)
            (recur (conj! acc v))
            (do (stop-group! group)
                (a/close! pending)
                (throw v))))
        (persistent! acc)))))

(defn- parallelly-stream [n f src]
  (let [out     (a/chan)
        ;; Fibers in input order. Bounds read-ahead to `n` for bounded calls.
        pending (a/chan (if n (max 1 n) Integer/MAX_VALUE))
        group   (work-group)
        stop!   (fn [e]
                  (when (stop-group! group)
                    (when (and e on-error) (on-error e "Exception in parallelly function"))
                    (a/close! src)
                    (a/close! out)
                    (a/close! pending)))]
    (spawn-in-order! n f #(let [v (a/<!! src)] (if (nil? v) ::done v)) pending group)
    (a/thread
      (loop []
        (when-some [fiber (a/<!! pending)]
          (let [[tag v] (outcome fiber)]
            (cond
              (= :err tag)                        (stop! v)
              (and (some? v) (not (a/>!! out v))) (stop! nil)
              :else                               (recur)))))
      (a/close! out))
    out))

(defn parallelly
  "Maps `f` over the channel or seq `s` with up to `n` items occurring in
  parallel, preserving order.

  With one arity, uses unbounded parallelism (or the max parallelism set via
  `with-max-parallelism`).

  Over a seq, returns a vector; the first error is thrown after interrupting
  the remaining calls. Over a channel, returns a channel that emits results as
  they become available in order (nil results are dropped); the first error
  is passed to the stream error handler, the remaining calls are interrupted,
  and both channels are closed. Closing the returned channel also stops the
  work."
  ([f s]
   (if (seqable? s) (parallelly-seq nil f s) (parallelly-stream nil f s)))
  ([n f s]
   (if (seqable? s) (parallelly-seq n f s) (parallelly-stream n f s))))

(defn send
  "Dispatch an agent action via a dedicated thread (the Jolt analog of a loom
  virtual thread). See `clojure.core/send`."
  [a f & args]
  (apply clojure.core/send a f args))
