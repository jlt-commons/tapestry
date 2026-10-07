(ns tapestry.experimental-test
  (:require [tapestry.experimental :as sut]
            [tapestry.core :as tc]
            [clojure.test :refer [deftest testing is]]))

(deftest with-scope-basic-test
  (testing "fibers run and results are collected"
    (is (= {:a 1 :b 2}
           (sut/with-scope {}
             (let [a (tc/fiber 1)
                   b (tc/fiber 2)]
               {:a @a :b @b})))))

  (testing "returns body value when no fibers are spawned"
    (is (= 42 (sut/with-scope {} 42)))))

(deftest with-scope-shutdown-on-failure-test
  (testing "error in one fiber cancels siblings and propagates"
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

  (testing "error propagated even if not deref'd"
    (is (thrown-with-msg?
          clojure.lang.ExceptionInfo #"unobserved"
          (sut/with-scope {:shutdown :on-failure}
            (tc/fiber (throw (ex-info "unobserved" {})))
            (Thread/sleep 50)
            :body-result)))))

(deftest with-scope-shutdown-on-success-test
  (testing "first success cancels siblings"
    (let [slow-ref (atom nil)]
      (sut/with-scope {:shutdown :on-success}
        (reset! slow-ref (tc/fiber (Thread/sleep 30000)))
        (tc/fiber :fast-result))
      (is (tc/errored? @slow-ref))
      (is (not (tc/alive? @slow-ref)))))

  (testing "the first successful result is recorded"
    (let [scope-ref (atom nil)]
      (sut/with-scope {:shutdown :on-success}
        (reset! scope-ref tc/*scope*)
        (tc/fiber (Thread/sleep 30000))
        (tc/fiber :fast-result))
      (is (= :fast-result (deref (:first-result @scope-ref) 1000 :timeout))))))

(deftest with-scope-timeout-test
  (testing "timeout cancels a fiber in the scope"
    (is (thrown? clojure.lang.ExceptionInfo
                 (sut/with-scope {:timeout 50 :shutdown :on-failure}
                   (let [f (tc/fiber (Thread/sleep 30000))]
                     @f))))))

(deftest with-scope-max-parallelism-test
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

(deftest with-scope-combined-options-test
  (testing "timeout + max-parallelism + shutdown together"
    (let [state        (atom {:running 0 :max-seen 0})
          update-state (fn [{:keys [running max-seen]}]
                         {:running  (inc running)
                          :max-seen (max (inc running) max-seen)})]
      (is (= [1 2 3 4 5]
             (sut/with-scope {:max-parallelism 2 :timeout 5000 :shutdown :on-failure}
               (->> (range 1 6)
                    (mapv (fn [x]
                            (tc/fiber
                              (swap! state update-state)
                              (Thread/sleep 2)
                              (swap! state update :running dec)
                              x)))
                    (mapv deref)))))
      (is (<= (:max-seen @state) 2)))))

(deftest with-scope-nested-test
  (testing "inner scope fibers don't leak to outer scope"
    (let [inner-count (atom 0)
          outer-count (atom 0)]
      (sut/with-scope {:shutdown :on-failure}
        (swap! outer-count (fn [_] (count @(:fibers tc/*scope*))))
        (let [a (tc/fiber
                  (sut/with-scope {:shutdown :on-failure}
                    (swap! inner-count (fn [_] (count @(:fibers tc/*scope*))))
                    (let [x (tc/fiber 10)
                          y (tc/fiber 20)]
                      (+ @x @y))))
              b (tc/fiber 3)]
          (is (= 33 (+ @a @b)))))
      (is (= 0 @outer-count) "outer scope had no fibers before a and b")
      (is (<= @inner-count 2) "inner scope should track its own fibers"))))

(deftest alts-test
  (testing "returns first successful result"
    (is (= :fast
           (sut/alts
             (do (Thread/sleep 30000) :slow)
             :fast))))

  (testing "with options map"
    (is (= :result
           (sut/alts {:timeout 5000}
             :result
             (Thread/sleep 30000)))))

  (testing "throws the first error rather than hanging when all operations fail"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (let [f (tc/fiber (sut/alts (throw (ex-info "boom" {}))
                                  (throw (ex-info "boom2" {}))))]
        (is (thrown-with-msg? clojure.lang.ExceptionInfo #"boom"
                              (deref f 2000 :hung))))
      (finally
        (tc/set-stream-error-handler! println))))

  (testing "timeout expiring with no success throws instead of hanging"
    (let [f (tc/fiber (sut/alts {:timeout 50}
                                (do (Thread/sleep 30000) :slow1)
                                (do (Thread/sleep 30000) :slow2)))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"timed out"
                            (deref f 2000 :hung)))))

(deftest alts-settlement-race-stress-test
  ;; Regression: scope bookkeeping (first-error/first-result) used to run
  ;; AFTER the result delivery that wakes await-all!, so alts could observe
  ;; neither promise realized and return nil instead of throwing.
  (testing "timeout outcome is reported on every iteration, never nil"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (dotimes [_ 150]
        (let [f (tc/fiber (sut/alts {:timeout 20}
                                    (do (Thread/sleep 5000) :slow1)
                                    (do (Thread/sleep 5000) :slow2)))]
          (is (thrown-with-msg? clojure.lang.ExceptionInfo #"timed out"
                                (deref f 3000 :hung)))))
      (finally
        (tc/set-stream-error-handler! println))))
  (testing "all-fail outcome is reported on every iteration, never nil"
    (tc/set-stream-error-handler! (fn [& _]))
    (try
      (dotimes [_ 150]
        (let [f (tc/fiber (sut/alts (throw (ex-info "boom" {}))
                                    (throw (ex-info "boom2" {}))))]
          (is (thrown? clojure.lang.ExceptionInfo (deref f 3000 :hung)))))
      (finally
        (tc/set-stream-error-handler! println))))))

(deftest with-scope-invalid-max-parallelism-test
  (testing "with-scope :max-parallelism 0 is rejected instead of deadlocking"
    (let [f (tc/fiber (sut/with-scope {:max-parallelism 0}
                       (tc/fiber 1)
                       :ok))]
      (is (thrown-with-msg? clojure.lang.ExceptionInfo #"max-parallelism"
                            (deref f 2000 :hung))))))
