(ns tapestry.experimental.scope-test
  (:require [tapestry.experimental.scope :as sut]
            [tapestry.core :as tc]
            [clojure.core.async :as a]
            [clojure.test :refer [deftest testing is]]))

(deftest with-scope-basic-test
  (testing "fibers run and results are collected"
    (is (= {:a 1 :b 2}
           (sut/with-scope {}
             (let [a (tc/fiber 1)
                   b (tc/fiber 2)]
               {:a @a :b @b})))))

  (testing "returns body value when no fibers are spawned"
    (is (= 42 (sut/with-scope {} 42))))

  (testing "nil results are returned correctly"
    (is (nil? (sut/with-scope {}
                (let [f (tc/fiber nil)]
                  @f)))))

  (testing "false results are returned correctly"
    (is (false? (sut/with-scope {}
                  (let [f (tc/fiber false)]
                    @f))))))

(deftest with-scope-fiber-registration-test
  (testing "fibers are automatically registered with the scope"
    (let [fiber-count (atom 0)]
      (sut/with-scope {}
        (tc/fiber :a)
        (tc/fiber :b)
        (tc/fiber :c)
        (reset! fiber-count (count @(:fibers tc/*scope*))))
      (is (= 3 @fiber-count))))

  (testing "fibers outside a scope are not registered"
    (let [f (tc/fiber :no-scope)]
      (is (= :no-scope @f))
      (is (nil? tc/*scope*)))))

(deftest shutdown-on-failure-test
  (testing "error in one fiber cancels siblings"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (let [slow-ref (atom nil)]
        (is (thrown-with-msg?
              clojure.lang.ExceptionInfo #"boom"
              (sut/with-scope {:shutdown :on-failure}
                (reset! slow-ref (tc/fiber (Thread/sleep 30000)))
                (tc/fiber (throw (ex-info "boom" {}))))))
        (is (tc/errored? @slow-ref))
        (is (not (tc/alive? @slow-ref)) "scope exit waits for the interrupted sibling"))
      (finally
        (tc/set-stream-error-handler! println))))

  (testing "error propagated at scope exit even if not deref'd"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"unobserved"
          (sut/with-scope {:shutdown :on-failure}
            (tc/fiber (throw (ex-info "unobserved" {})))
            (Thread/sleep 50)
            :body-result))))

  (testing "first error wins when multiple fibers fail"
    (tc/set-stream-error-handler! (fn [& _]))
    (let [started? (promise)]
      (try
        (sut/with-scope {:shutdown :on-failure}
          (tc/fiber
            (deliver started? true)
            (throw (ex-info "first" {})))
          @started?
          (tc/fiber
            (Thread/sleep 10)
            (throw (ex-info "second" {})))
          (Thread/sleep 100))
        (catch clojure.lang.ExceptionInfo e
          (is (= "first" (ex-message e))))
        (finally
          (tc/set-stream-error-handler! println)))))

  (testing "successful scope with :on-failure returns body result"
    (is (= {:a 1 :b 2}
           (sut/with-scope {:shutdown :on-failure}
             (let [a (tc/fiber 1)
                   b (tc/fiber 2)]
               {:a @a :b @b}))))))

(deftest shutdown-on-success-test
  (testing "first success cancels siblings"
    (let [slow-ref (atom nil)]
      (sut/with-scope {:shutdown :on-success}
        (reset! slow-ref (tc/fiber (Thread/sleep 30000)))
        (tc/fiber :fast-result))
      (is (tc/errored? @slow-ref))))

  (testing "the first successful result is recorded"
    (let [scope-ref (atom nil)]
      (sut/with-scope {:shutdown :on-success}
        (reset! scope-ref tc/*scope*)
        (tc/fiber (Thread/sleep 30000))
        (tc/fiber :winner))
      (is (= :winner (deref (:first-result @scope-ref) 1000 :timeout))))))

(deftest scope-timeout-test
  (testing "timeout applies to fibers spawned in the scope"
    (is (thrown? clojure.lang.ExceptionInfo
                 (sut/with-scope {:timeout 50 :shutdown :on-failure}
                   (let [f (tc/fiber (Thread/sleep 30000))]
                     @f)))))

  (testing "timeout cancels the fiber's result"
    (let [fib-ref (atom nil)]
      (try
        (sut/with-scope {:timeout 50 :shutdown :on-failure}
          (reset! fib-ref (tc/fiber (Thread/sleep 30000)))
          ;; give the timeout a chance to fire before the scope returns
          (Thread/sleep 80))
        (catch Exception _))
      (is (tc/errored? @fib-ref)))))

(deftest scope-max-parallelism-test
  (testing "max-parallelism limits concurrent fibers"
    (let [state        (atom {:running 0 :max-seen 0})
          update-state (fn [{:keys [running max-seen]}]
                         {:running  (inc running)
                          :max-seen (max (inc running) max-seen)})]
      (sut/with-scope {:max-parallelism 3}
        (->> (range 20)
             (mapv (fn [_]
                     (tc/fiber
                       (swap! state update-state)
                       (Thread/sleep 5)
                       (swap! state update :running dec))))
             (mapv deref)))
      (is (zero? (:running @state)))
      (is (<= (:max-seen @state) 3)))))

(deftest scope-combined-options-test
  (testing "timeout + max-parallelism + shutdown together"
    (let [state        (atom {:running 0 :max-seen 0})
          update-state (fn [{:keys [running max-seen]}]
                         {:running  (inc running)
                          :max-seen (max (inc running) max-seen)})]
      (is (= (range 1 6)
             (sut/with-scope {:max-parallelism 2 :timeout 5000 :shutdown :on-failure}
               (->> (range 1 6)
                    (mapv (fn [x]
                            (tc/fiber
                              (swap! state update-state)
                              (Thread/sleep 2)
                              (swap! state update :running dec)
                              x)))
                    (mapv deref)))))
      (is (<= (:max-seen @state) 2))))

  (testing "shutdown + timeout: timeout triggers shutdown"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (let [slow-ref (atom nil)]
        (try
          (sut/with-scope {:timeout 50 :shutdown :on-failure}
            (reset! slow-ref (tc/fiber (Thread/sleep 30000)))
            (tc/fiber (Thread/sleep 30000))
            (Thread/sleep 80))
          (catch Exception _))
        (is (tc/errored? @slow-ref)
            "timeout should cancel sibling results"))
      (finally
        (tc/set-stream-error-handler! println)))))

(deftest scope-nested-test
  (testing "inner scope fibers don't leak to outer scope"
    (let [inner-fiber-count (atom 0)
          outer-fiber-count (atom 0)]
      (sut/with-scope {:shutdown :on-failure}
        (let [a (tc/fiber
                  (sut/with-scope {:shutdown :on-failure}
                    (let [x (tc/fiber 10)
                          y (tc/fiber 20)]
                      (reset! inner-fiber-count (count @(:fibers tc/*scope*)))
                      (+ @x @y))))
              b (tc/fiber 3)]
          (reset! outer-fiber-count (count @(:fibers tc/*scope*)))
          (is (= 33 (+ @a @b)))))
      (is (= 2 @outer-fiber-count) "outer scope should have 2 fibers (a and b)")
      (is (= 2 @inner-fiber-count) "inner scope should have 2 fibers (x and y)")))

  (testing "inner scope failure doesn't propagate to outer scope directly"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (is (= :caught
             (sut/with-scope {:shutdown :on-failure}
               (let [a (tc/fiber
                         (try
                           (sut/with-scope {:shutdown :on-failure}
                             (tc/fiber (throw (ex-info "inner-boom" {})))
                             (Thread/sleep 50))
                           (catch Exception _
                             :caught)))]
                 @a))))
      (finally
        (tc/set-stream-error-handler! println)))))

(deftest scope-no-shutdown-policy-test
  (testing "scope without shutdown policy still awaits fibers"
    (let [completed (atom 0)]
      (sut/with-scope {}
        (dotimes [_ 5]
          (tc/fiber
            (Thread/sleep 10)
            (swap! completed inc))))
      (is (= 5 @completed))))

  (testing "scope without shutdown policy ignores fiber errors"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (is (= :ok
             (sut/with-scope {}
               (tc/fiber (throw (ex-info "ignored" {})))
               (Thread/sleep 20)
               :ok)))
      (finally
        (tc/set-stream-error-handler! println)))))

(deftest scope-body-exception-shuts-down-fibers-test
  (testing "exception in body (not from fiber) shuts down all fibers"
    (let [slow-ref (atom nil)]
      (try
        (sut/with-scope {}
          (reset! slow-ref (tc/fiber (Thread/sleep 30000)))
          (throw (ex-info "body-error" {})))
        (catch Exception _))
      (is (tc/errored? @slow-ref)
          "fibers should be cancelled when body throws"))))

(deftest scope-captured-at-spawn-test
  (testing "interrupting a scoped fiber from an unscoped thread still reaches its scope"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"interrupted"
          (sut/with-scope {:shutdown :on-failure}
            (let [f (tc/fiber (Thread/sleep 30000))
                  t (Thread. (fn [] (tc/interrupt! f)))]
              (.start t)
              (.join t))))))

  (testing "interrupting an outer fiber from inside a nested scope does not fail the nested scope"
    (let [inner-result (atom nil)]
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"interrupted"
            (sut/with-scope {:shutdown :on-failure}
              (let [outer (tc/fiber (Thread/sleep 30000))]
                (reset! inner-result
                        (sut/with-scope {:shutdown :on-failure}
                          (tc/interrupt! outer)
                          @(tc/fiber :inner-ok)))))))
      (is (= :inner-ok @inner-result)))))

(deftest scope-waits-for-bodies-test
  (testing "scope exit waits for interrupted bodies to finish their cleanup"
    (let [cleaned (atom false)
          started (promise)]
      (sut/with-scope {:shutdown :on-success}
        (tc/fiber (try (deliver started true)
                       (Thread/sleep 30000)
                       (finally (Thread/sleep 50) (reset! cleaned true))))
        ;; otherwise :fast can win before the slow body starts, and a fiber
        ;; cancelled before it starts never runs its body
        @started
        (tc/fiber :fast))
      (is (true? @cleaned))))

  (testing "fibers spawned by scoped fibers are awaited too"
    (let [grandchild-done (atom false)]
      (sut/with-scope {}
        (tc/fiber
          (tc/fiber (Thread/sleep 100) (reset! grandchild-done true))
          :child))
      (is (true? @grandchild-done)))))

(deftest asyncly-inside-scope-test
  (testing "an asyncly error inside an :on-failure scope surfaces as the real error"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (doseq [n [nil 2]]
        (is (thrown-with-msg?
              clojure.lang.ExceptionInfo #"real-error"
              (sut/with-scope {:shutdown :on-failure}
                (let [f (fn [x]
                          (if (= x 0)
                            (do (Thread/sleep 20) (throw (ex-info "real-error" {})))
                            (Thread/sleep 30000)))]
                  (doall (if n (tc/asyncly n f (range 4)) (tc/asyncly f (range 4)))))))
            (str "n=" n)))
      (finally (tc/set-stream-error-handler! println)))))

(deftest body-error-not-masked-by-shutdown-test
  (testing "the body's own error is rethrown, not the interrupts from shutting down"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"body-error"
          (sut/with-scope {:shutdown :on-failure}
            (tc/fiber (Thread/sleep 30000))
            (Thread/sleep 20)
            (throw (ex-info "body-error" {})))))))

(deftest scope-exit-with-channel-wait-test
  (if-not (force @(requiring-resolve 'tapestry.core-test/channel-ops-interruptible?))
    (println "skipping scope-exit-with-channel-wait-test: this jolt can't interrupt <!!")
    (testing "an :on-success scope exits when a sibling is parked on a channel"
      (let [result (future (sut/with-scope {:shutdown :on-success}
                             (tc/fiber (a/<!! (a/chan)))
                             (tc/fiber :fast)
                             :done))]
        (is (= :done (deref result 3000 ::hung)))))))

(deftest asyncly-queued-workers-inside-scope-test
  (testing "with fibers queued for permits, the real asyncly error still surfaces"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (is (thrown-with-msg?
            clojure.lang.ExceptionInfo #"real-error"
            (sut/with-scope {:shutdown :on-failure}
              (tc/with-max-parallelism 1
                (doall (tc/asyncly (fn [x]
                                     (when (= x 0)
                                       (Thread/sleep 20)
                                       (throw (ex-info "real-error" {}))))
                                   (range 5)))))))
      (finally (tc/set-stream-error-handler! println)))))
