(ns tapestry.core-test
  (:require [tapestry.core :as sut]
            [clojure.core.async :as a]
            [clojure.test :refer [deftest testing is]]))

;; core.async channels expose no `closed?`, so tests probe by attempting a
;; blocking put: it returns false exactly when the channel is closed.
(defn chan-closed? [ch]
  (not (a/>!! ch ::probe)))

(defn drain [ch]
  (a/<!! (a/into [] ch)))

(deftest with-max-parallelism-test
  (testing "with-max-parallism limits parallel execution"
    (let [state        (atom {:running 0 :max-seen 0 :count 0})
          update-state (fn [{:keys [count running max-seen]}]
                         {:count    (inc count)
                          :running  (inc running)
                          :max-seen (max max-seen (inc running))})]
      (is (= (range 100)
             (sut/with-max-parallelism 10
               (->> (range 100)
                    (mapv (fn [x]
                            (sut/fiber
                              (swap! state update-state)
                              (Thread/sleep 1)
                              (swap! state update :running dec)
                              x)))
                    (mapv deref)))))

      (is (= 100 (:count @state)))
      (is (zero? (:running @state)))
      (is (<= (:max-seen @state) 10))))

  (testing "with-max-parallelism can be nested"
    (let [state        (atom {:running 0 :max-seen 0 :count 0})
          update-state (fn [{:keys [count running max-seen]}]
                         {:count    (inc count)
                          :running  (inc running)
                          :max-seen (max max-seen (inc running))})]
      (is (= (range 100)
             (sut/with-max-parallelism 10
               (flatten
                 (->> (range 10)
                      (mapv (fn [x]
                              (sut/with-max-parallelism 10
                                (->> (range 10)
                                     (mapv (fn [y]
                                             (sut/fiber
                                               (swap! state update-state)
                                               (Thread/sleep 2)
                                               (swap! state update :running dec)
                                               (+ (* 10 x) y))))
                                     (mapv deref))))))))))
      (is (= 100 (:count @state)))
      (is (zero? (:running @state)))
      (is (<= 10 (:max-seen @state) 100)))))

(deftest asyncly-test
  (testing "unbounded concurrency"
    (is (= [2 3 4]
           (->> (a/to-chan [1 2 3])
                (sut/asyncly inc)
                (drain)
                (sort)))))

  (testing "handling nil"
    (is (= '() (sut/asyncly inc nil))))
  (testing "seq mode"
    (is (= [2 3 4]
           (->> [1 2 3]
                (sut/asyncly inc)
                sort))))
  (testing "bounded concurrency"
    (let [state        (atom {:running 0 :max-seen 0})
          update-state (fn [{:keys [running max-seen]}]
                         {:running  (inc running)
                          :max-seen (max (inc running) max-seen)})]
      (is (= (range 10)
             (->> (a/to-chan (range 10))
                  (sut/asyncly 3 #(do (swap! state update-state)
                                      (Thread/sleep 2)
                                      (swap! state update :running dec)
                                      %))
                  (drain)
                  (sort))))
      (is (zero? (:running @state)))
      (is (<= (:max-seen @state) 3))))

  (testing "unbounded - seq mode throws on error"
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (let [boom (ex-info "boom" {})]
        (is (thrown-with-msg?
              clojure.lang.ExceptionInfo #"boom"
              ;; must force the lazy seq to realize the throw
              (doall (sut/asyncly (fn [x] (when (= x 2) (throw boom)) x)
                                  [1 2 3])))))
      (finally
        (sut/set-stream-error-handler! println))))

  (testing "unbounded - no new fibers dispatched after exception"
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (let [call-count (atom 0)]
        (is (thrown-with-msg?
              clojure.lang.ExceptionInfo #"boom"
              (doall (sut/asyncly
                       (fn [x]
                         (swap! call-count inc)
                         (when (= x 0)
                           (throw (ex-info "boom" {})))
                         x)
                       (range 100)))))
        ;; Dispatch stops once the error fires: far fewer than 100 items run.
        (is (< @call-count 50)))
      (finally
        (sut/set-stream-error-handler! println))))

  (testing "unbounded - stream mode closes result stream on error (no throw)"
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (let [result (sut/asyncly #(throw (ex-info "oops" {}))
                                (a/to-chan [1 2 3]))]
        (drain result)                       ;; drains cleanly, does not throw
        (Thread/sleep 20)
        (is (chan-closed? result)))
      (finally
        (sut/set-stream-error-handler! println))))

  (testing "unbounded - stream mode closes source stream on error"
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (let [source (a/chan 2)]
        (a/>!! source 1)
        (a/>!! source 2)
        (let [result (sut/asyncly #(throw (ex-info "oops" {})) source)]
          (drain result)
          (Thread/sleep 20)
          (is (chan-closed? source))))
      (finally
        (sut/set-stream-error-handler! println))))

  (testing "bounded - seq mode throws on error"
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (let [boom (ex-info "bounded-boom" {})]
        (is (thrown-with-msg?
              clojure.lang.ExceptionInfo #"bounded-boom"
              (doall (sut/asyncly 2
                                  (fn [x] (when (= x 2) (throw boom)) x)
                                  [1 2 3])))))
      (finally
        (sut/set-stream-error-handler! println))))

  (testing "bounded - error not lost when other workers produce nil results (race condition)"
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"boom"
            (doall (sut/asyncly 4
                                (fn [x] (when (= x 5) (throw (ex-info "boom" {}))) nil)
                                (range 100)))))
      (finally
        (sut/set-stream-error-handler! println))))

  (testing "bounded - error propagates despite blocked workers"
    ;; On Jolt, Thread/sleep cannot be forcibly interrupted, so blocked workers
    ;; run to completion in the background while the error is reported promptly
    ;; (the result stream closes on error). The call must throw well before the
    ;; 30s sleeps would finish.
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (let [result* (promise)]
        (sut/fiber
          (try
            (doall (sut/asyncly 4
                                (fn [x]
                                  (when (= x 0)
                                    (throw (ex-info "interrupted-boom" {})))
                                  (Thread/sleep 30000))
                                (range 10)))
            (catch Exception e (deliver result* e))))
        (let [outcome (deref result* 10000 ::timeout)]
          (is (not= ::timeout outcome) "error did not propagate — timed out after 10s")
          (is (instance? clojure.lang.ExceptionInfo outcome))
          (is (re-find #"interrupted-boom" (ex-message outcome)))))
      (finally
        (sut/set-stream-error-handler! println))))

  (testing "bounded - stream mode closes result stream on error (no throw)"
    (sut/set-stream-error-handler! (fn [& _]))
    (try
      (let [result (sut/asyncly 2
                                #(throw (ex-info "oops" {}))
                                (a/to-chan [1 2 3]))]
        (drain result)
        (Thread/sleep 20)
        (is (chan-closed? result)))
      (finally
        (sut/set-stream-error-handler! println)))))

(deftest periodically-test
  (let [ch (sut/periodically 50 50 (constantly true))]
    (is (nil? (a/poll! ch)))                                ;; nothing immediately
    (is (true? (first (a/alts!! [ch (a/timeout 500)]))))    ;; wait for first tick
    (is (nil? (a/poll! ch)))                                ;; nothing immediately
    (is (true? (first (a/alts!! [ch (a/timeout 500)]))))    ;; wait for next tick
    (a/close! ch)))


(deftest parallely-test
  (testing "stream mode"
    (is (= [2 3 4 5 6 7]
           (->> (a/to-chan [1 2 3 4 5 6])
                (sut/parallelly 2 inc)
                (drain))))
    (is (= [2 3 4]
           (->> (a/to-chan [1 2 3])
                (sut/parallelly inc)
                (drain)))))

  (testing "handles nil"
    (is (= '() (sut/parallelly inc nil))))

  (testing "seq mode"
    (is (= [2 3 4 5 6]
           (sut/parallelly 2 inc [1 2 3 4 5])))
    (is (= [2 3 4]
           (sut/parallelly inc [1 2 3]))))

  (testing "unbounded parallelism"
    (is (= [2 3 4 5]
           (sut/parallelly inc [1 2 3 4]))))

  (testing "propagates errors with bounded parallelism over a seq"
    (let [boom   (fn [x] (if (= x 3) (throw (ex-info "boom" {:x x})) (inc x)))
          result (future (try
                           (doall (sut/parallelly 2 boom [1 2 3 4 5]))
                           ::no-throw
                           (catch clojure.lang.ExceptionInfo e
                             (ex-message e))))]
      (is (= "boom" (deref result 5000 ::timed-out)))))

  (testing "a stream-mode error goes to the error handler and closes the result"
    (let [seen   (promise)
          boom   (fn [x] (if (= x 3) (throw (ex-info "boom" {:x x})) (inc x)))]
      (sut/set-stream-error-handler! (fn [e _] (deliver seen (ex-message e))))
      (try
        (is (= [2 3] (drain (sut/parallelly 2 boom (a/to-chan [1 2 3 4 5])))))
        (is (= "boom" (deref seen 1000 ::none)))
        (finally (sut/set-stream-error-handler! println))))))

(deftest locking-test
  (testing "locking works"
    (let [resource (atom false)
          locked   (promise)]
      (sut/fiber
        (locking resource
          (deliver locked true)
          (Thread/sleep 10)
          (reset! resource true)))
      @locked
      (locking resource
        (is (true? @resource))))))

(deftest fiber-error-test
  (testing "a fiber that throws records its error"
    (let [die? (promise)
          err  (ex-info "Boom" {})
          f    (sut/fiber
                 @die?
                 (throw err))]
      (is (nil? (sut/fiber-error f)))
      (deliver die? true)
      (Thread/sleep 20)                       ;; let the fiber die
      (is (some? (sut/fiber-error f)))
      (is (sut/errored? f))
      (is (thrown? clojure.lang.ExceptionInfo @f)))))

(deftest pfor-test
  (testing "works"
    (is (= '(1 2 3)
           (sut/pfor [x (range 3)] (inc x)))))
  (testing "is eager"
    (is (realized? (sut/pfor [x (range 3)] (inc x))))))

(deftest interrupt-test
  (testing "interrupt! cancels the fiber's result"
    (let [f (sut/fiber (Thread/sleep 10000))]
      (sut/interrupt! f)
      (is (thrown? clojure.lang.ExceptionInfo @f))
      (is (sut/errored? f))))
  (testing "interrupt! on an already-completed fiber is a no-op"
    (let [f (sut/fiber :done)]
      (is (= :done @f))                      ;; wait for completion
      (sut/interrupt! f)
      (is (= :done @f)))))                    ;; result unchanged

(deftest cancel-interrupts-thread-test
  (testing "interrupting a running fiber marks it errored"
    (let [f (sut/fiber (Thread/sleep 30000))]
      (Thread/sleep 50)
      (sut/interrupt! f)
      (is (sut/errored? f))
      (is (thrown? clojure.lang.ExceptionInfo @f))))
  (testing "interrupt on already-completed fiber leaves the result intact"
    (let [f (sut/fiber :done)]
      (is (= :done @f))
      (sut/interrupt! f)
      (is (= :done @f)))))

(deftest alive?-test
  (testing "a fiber is alive while its body runs and dead once it returns"
    (let [f (sut/fiber (Thread/sleep 2000))]
      (Thread/sleep 10)
      (is (sut/alive? f))
      @f
      (is (not (sut/alive? f))))))

(deftest timeout!-test
  (testing "simple timeout"
    (let [f (sut/fiber (Thread/sleep 30000))]
      (sut/timeout! f 10)
      (is (thrown? clojure.lang.ExceptionInfo @f))
      (is (sut/errored? f))))
  (testing "binding-based timeout"
    (let [f (sut/with-timeout 10
               (sut/fiber (Thread/sleep 30000)))]
      (is (thrown? clojure.lang.ExceptionInfo @f))))

  (testing "binding and explicit defaults to explicit"
    (let [f (sut/with-timeout 100
               (sut/fiber (Thread/sleep 30000)))]
      (is (= :explicit
             @(sut/timeout! f 10 :explicit)))))

  (testing "default value"
    (let [f (sut/timeout! (sut/fiber (Thread/sleep 30000))
                          10
                          :default)]
      (is (= :default @f)))))

(deftest fiber-deref-protocols-test
  (testing "deref returns the body value"
    (is (= 7 @(sut/fiber (+ 3 4)))))
  (testing "nil and false results are preserved"
    (is (nil? @(sut/fiber nil)))
    (is (false? @(sut/fiber false))))
  (testing "IPending: realized transitions from false to true"
    (let [gate (promise)
          f    (sut/fiber @gate :done)]
      (is (not (realized? f)))
      (deliver gate true)
      @f
      (is (realized? f))))
  (testing "IBlockingDeref returns default on timeout"
    (let [gate (promise)
          f    (sut/fiber @gate)]
      (is (= :timed-out (deref f 10 :timed-out)))
      (deliver gate :late)
      (is (= :late @f)))))

(deftest send-test
  (let [a (agent 0)]
    (testing "without arguments"
      (sut/send a inc)
      (await a)
      (is (= 1 @a)))
    (testing "with argument"
      (sut/send a (constantly 0))
      (sut/send a + 2 3)
      (await a)
      (is (= 5 @a)))
    (testing "with multiple arguments"
      (sut/send a (constantly 0))
      (sut/send a + 1 2 3 4)
      (await a)
      (is (= 10 @a)))))

(deftest fiber-interrupt-after-settle-test
  (testing "interrupting a fiber that already settled does not mark it errored"
    (let [f (sut/fiber :done)]
      (is (= :done @f))
      (Thread/sleep 20)
      (is (false? (sut/alive? f)))
      (sut/interrupt! f)
      (is (false? (sut/errored? f)) "a settled fiber must not report errored?")
      (is (nil? (sut/fiber-error f)))
      (is (= :done @f)))))

(deftest with-max-parallelism-invalid-test
  (testing "with-max-parallelism 0 is rejected instead of deadlocking"
    (is (thrown-with-msg? clojure.lang.ExceptionInfo #"max-parallelism"
          (sut/with-max-parallelism 0
            (sut/fiber :x))))))

(defn- await-dead
  "Poll until `f` is no longer alive, or `ms` elapse. Returns true if dead."
  [f ms]
  (let [deadline (+ (System/currentTimeMillis) ms)]
    (loop []
      (cond (not (sut/alive? f))                      true
            (> (System/currentTimeMillis) deadline)  false
            :else                                    (do (Thread/sleep 5) (recur))))))

(deftest cancellation-stops-body-test
  (testing "interrupt! throws InterruptedException into a blocked body"
    (let [seen (promise)
          f    (sut/fiber
                 (try (Thread/sleep 30000)
                      (catch InterruptedException e (deliver seen :interrupted) (throw e))))]
      (Thread/sleep 50)
      (sut/interrupt! f)
      (is (= :interrupted (deref seen 2000 :not-interrupted)))
      (is (await-dead f 2000))
      (is (= ::sut/interrupted (:type (ex-data (sut/fiber-error f)))))))

  (testing "timeout! stops the body"
    (let [f (sut/timeout! (sut/fiber (Thread/sleep 30000)) 20)]
      (is (await-dead f 2000))
      (is (thrown? clojure.lang.ExceptionInfo @f))))

  (testing "timeout! with a default stops the body"
    (let [f (sut/timeout! (sut/fiber (Thread/sleep 30000)) 20 :default)]
      (is (= :default @f))
      (is (await-dead f 2000))))

  (testing "a body that ignores the interrupt keeps its cancelled result"
    (let [f (sut/fiber (try (Thread/sleep 30000) (catch InterruptedException _ :swallowed)))]
      (Thread/sleep 20)
      (sut/interrupt! f)
      (is (await-dead f 2000))
      (is (thrown? clojure.lang.ExceptionInfo @f)))))

(deftest cancelled-while-queued-test
  (testing "a fiber cancelled while waiting for a permit never runs its body"
    (let [gate    (promise)
          holding (promise)
          ran     (atom false)]
      (sut/with-max-parallelism 1
        (let [holder (sut/fiber (deliver holding true) @gate)
              _      @holding
              queued (sut/fiber (reset! ran true))]
          (Thread/sleep 20)
          (sut/interrupt! queued)
          (is (await-dead queued 1000) "queued fiber should exit without a permit")
          (deliver gate :go)
          @holder
          (Thread/sleep 50)
          (is (false? @ran)))))))

(deftest with-timeout-does-not-hold-completed-fibers-test
  (testing "a fiber that finishes before its timeout keeps its value"
    (let [f (sut/with-timeout 30000 (sut/fiber :quick))]
      (is (= :quick @f))
      (Thread/sleep 20)
      (is (= :quick @f))
      (is (not (sut/errored? f))))))

(defn- track-concurrency
  "Wrap `f` so `state` records the peak number of concurrent calls."
  [state f]
  (fn [x]
    (swap! state (fn [{:keys [running peak]}]
                   {:running (inc running) :peak (max (or peak 0) (inc running))}))
    (try (f x) (finally (swap! state update :running dec)))))

(deftest asyncly-max-parallelism-test
  (testing "unbounded asyncly honors with-max-parallelism"
    (doseq [[mode run] [[:seq    #(doall (sut/asyncly %1 %2))]
                        [:stream #(drain (sut/asyncly %1 (a/to-chan %2)))]]]
      (let [state (atom {:running 0 :peak 0})
            f     (track-concurrency state #(do (Thread/sleep 5) %))]
        (is (= (range 30) (sort (sut/with-max-parallelism 3 (run f (range 30))))))
        (is (<= (:peak @state) 3) (str mode " peak " (:peak @state)))))))

(deftest asyncly-error-interrupts-workers-test
  (doseq [[label n] [["unbounded" nil] ["bounded" 4]]]
    (testing (str label " seq mode throws promptly and interrupts in-flight calls")
      (sut/set-stream-error-handler! (fn [& _]))
      (try
        (let [interrupted (atom 0)
              f           (fn [x]
                            (if (= x 3)
                              (do (Thread/sleep 50) (throw (ex-info "boom" {})))
                              (try (Thread/sleep 30000)
                                   (catch InterruptedException e
                                     (swap! interrupted inc) (throw e)))))
              result      (future
                            (try (doall (if n (sut/asyncly n f (range 4)) (sut/asyncly f (range 4))))
                                 (catch clojure.lang.ExceptionInfo e (ex-message e))))]
          (is (= "boom" (deref result 5000 ::timed-out)))
          (Thread/sleep 100)
          (is (= 3 @interrupted)))
        (finally (sut/set-stream-error-handler! println))))

    (testing (str label " stream mode closes the result promptly and interrupts in-flight calls")
      (sut/set-stream-error-handler! (fn [& _]))
      (try
        (let [interrupted (atom 0)
              f           (fn [x]
                            (if (= x 3)
                              (do (Thread/sleep 50) (throw (ex-info "boom" {})))
                              (try (Thread/sleep 30000)
                                   (catch InterruptedException e
                                     (swap! interrupted inc) (throw e)))))
              src         (a/to-chan (range 4))
              result      (if n (sut/asyncly n f src) (sut/asyncly f src))]
          (is (= [] (first (a/alts!! [(a/into [] result) (a/timeout 5000)]))))
          (Thread/sleep 100)
          (is (= 3 @interrupted)))
        (finally (sut/set-stream-error-handler! println))))))


(deftest seq->stream-test
  (testing "emits the seq and closes"
    (is (= [0 1 2] (drain (sut/seq->stream (range 3))))))
  (testing "reports an error realizing the seq and closes the channel"
    (let [seen (promise)]
      (sut/set-stream-error-handler! (fn [e _] (deliver seen (ex-message e))))
      (try
        ;; reduce realizes the tail before emitting the head, so the error
        ;; may cut the output short by one item.
        (is (#{[0] [0 1]} (drain (sut/seq->stream
                                   (concat [0 1] (lazy-seq (throw (ex-info "seq-boom" {}))))))))
        (is (= "seq-boom" (deref seen 1000 ::none)))
        (finally (sut/set-stream-error-handler! println))))))

(deftest parallelly-streaming-test
  (testing "stream mode emits before the source is exhausted"
    (let [out (sut/parallelly 2 inc (a/to-chan (range)))]
      (is (= [1 2 3] (repeatedly 3 #(first (a/alts!! [out (a/timeout 2000)])))))
      (a/close! out))
    (let [src (a/chan)
          out (sut/parallelly inc src)]
      (a/>!! src 1)
      (is (= 2 (first (a/alts!! [out (a/timeout 2000)]))))
      (a/close! src)
      (is (nil? (first (a/alts!! [out (a/timeout 2000)]))))))

  (testing "stream mode preserves order and bounds parallelism"
    (let [state (atom {:running 0 :peak 0})
          f     (track-concurrency state #(do (Thread/sleep (rand-int 10)) %))]
      (is (= (range 30) (drain (sut/parallelly 3 f (a/to-chan (range 30))))))
      (is (<= (:peak @state) 3))))

  (testing "nil results are dropped in stream mode and kept in seq mode"
    (is (= [1 3] (drain (sut/parallelly 2 #(when (odd? %) %) (a/to-chan [1 2 3])))))
    (is (= [1 nil 3] (sut/parallelly 2 #(when (odd? %) %) [1 2 3])))))

(deftest parallelly-error-interrupts-test
  (doseq [n [nil 2]]
    (testing (str "n=" n " seq mode interrupts the remaining calls")
      (let [interrupted (atom 0)
            f           (fn [x]
                          (if (= x 0)
                            (do (Thread/sleep 50) (throw (ex-info "boom" {})))
                            (try (Thread/sleep 30000)
                                 (catch InterruptedException e (swap! interrupted inc) (throw e)))))
            result      (future (try (if n (sut/parallelly n f (range 3)) (sut/parallelly f (range 3)))
                                     (catch clojure.lang.ExceptionInfo e (ex-message e))))]
        (is (= "boom" (deref result 5000 ::timed-out)))
        (Thread/sleep 100)
        ;; with n=2 the third call may start on the failed call's permit
        ;; before the error is seen; it is then interrupted too
        (is ((if n #{1 2} #{2}) @interrupted))))

    (testing (str "n=" n " stream mode interrupts in-flight calls")
      (sut/set-stream-error-handler! (fn [& _]))
      (try
        (let [interrupted (atom 0)
              f           (fn [x]
                            (if (= x 0)
                              (do (Thread/sleep 50) (throw (ex-info "boom" {})))
                              (try (Thread/sleep 30000)
                                   (catch InterruptedException e (swap! interrupted inc) (throw e)))))
              src         (a/to-chan (range 3))
              out         (if n (sut/parallelly n f src) (sut/parallelly f src))]
          (is (= [] (first (a/alts!! [(a/into [] out) (a/timeout 5000)]))))
          (Thread/sleep 100)
          (is ((if n #{1 2} #{2}) @interrupted)))
        (finally (sut/set-stream-error-handler! println))))))
