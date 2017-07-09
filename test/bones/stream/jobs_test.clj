(ns bones.stream.jobs-test
  (:require [bones.stream
             [jobs :as jobs]
             [kafka :as kafka]]
            [clojure.test :refer [deftest is are testing]]
            [clojure.core.async :as a]
            [com.stuartsierra.component :as component]
            [onyx api
             [test-helper :refer [load-config with-test-env]]]
            [onyx-local-rt.api :as api]
            [onyx.plugin.kafka :as ok]
            [bones.stream.protocols :as p]))

(defn abc [segment]
  segment)

(deftest task-builders
  (testing "kafka-input-task with topic option in conf"
    (let [{:keys [workflow catalog lifecycles]} ((jobs/input {:bones/input {:kafka/topic "abc"}}
                                                             :kafka)
                                                 {:workflow []})]
      (is (= "abc" (:kafka/topic (first catalog))))))
  (testing "kafka-input-task with topic option"
    (let [{:keys [workflow catalog lifecycles]} ((jobs/input {} :kafka {:kafka/topic "abc"})
                                                 {:workflow []})]
      (is (= [[:bones/input]] workflow))
      ;; this is good:
      (is (= "abc" (:kafka/topic (first catalog))))))
  (testing "kafka-input-task"
    (let [{:keys [workflow catalog lifecycles]} ((jobs/input {} :kafka)
                                                 {:workflow []})]
      (is (= [[:bones/input]] workflow))
      ;; this is bad: this will need to be in the docs
      (is (= nil (:kafka/topic (first catalog))))))
  (testing "redis-output-task with channel option in conf"
    (let [{:keys [workflow catalog lifecycles]} ((jobs/output {:bones/output {:redis/channel "cba"}}
                                                              :redis)
                                                 {:workflow []})]
      (is (= [[:bones/output]] workflow))
      (is (= "cba" (:redis/channel (first catalog))))))
  (testing "redis-output-task with channel option"
    (let [{:keys [workflow catalog lifecycles]} ((jobs/output {} :redis {:redis/channel "cba"})
                                                 {:workflow []})]
      (is (= [[:bones/output]] workflow))
      (is (= "cba" (:redis/channel (first catalog))))))
  (testing "redis-output-task"
    (let [{:keys [workflow catalog lifecycles]} ((jobs/output {} :redis)
                                                 {:workflow []})]
      ;; this is bad:
      (is (= nil (:redis/channel (first catalog)))))))


(deftest helpers
  (testing "append-task"
    (are [wf task result] (= result (jobs/append-task wf task))
      nil       :a #_> [[:a]]
      []        :a #_> [[:a]]
      [[]]      :a #_> [[:a]]
      [[:a]]    :b #_> [[:a :b]]
      [[:a :b]] :c #_> [[:a :b]
                        [:b :c]]
      [[:a :b]
       [:b :c]] :d #_> [[:a :b]
                        [:b :c]
                        [:c :d]])))

(defn my-inc [segment]
  (update-in segment [:n] inc))

(deftest job-builders
  (testing "single function job (41 becomes 42)"
    (let [job (jobs/series-> {}
                              (jobs/input :kafka)
                              (jobs/function ::my-inc)
                              (jobs/function ::my-other-inc)
                              (jobs/output :dummy))]

      (is (= {:next-action :lifecycle/start-task?
              :tasks {:bones/output {:inbox []
                                     :outputs [{:n 42} {:n 85}]}
                      :bones.stream.jobs-test/my-inc {:inbox []}
                      :bones/input {:inbox []}}}
             (-> (api/init job)
                 (api/new-segment :bones/input {:n 41})
                 (api/new-segment :bones/input {:n 84})
                 (api/drain)
                 (api/stop)
                 (api/env-summary)))))))








(comment  ;; WIP

  (deftest job
    (let [redis (component/start (bones.stream.redis/map->Redis {}))
          config (load-config "dev-config.edn")
          topic "test"

          env-config  (:env-config config)
          peer-config (assoc (:peer-config config)
                             :onyx.peer/fn-params {:bones/output [redis]})


          command-ns ::abc
          job {:workflow (jobs/bare-workflow command-ns)
               :catalog (jobs/bare-catalog command-ns topic)
               :lifecycles (jobs/bare-lifecycles command-ns)
               :task-scheduler :onyx.task-scheduler/balanced}
          bones-job (component/start (jobs/map->Job {:onyx-job job
                                                      :conf conf}))]

      (with-test-env [test-env [3 env-config peer-config]]
        (onyx.api/submit-job peer-config job)
        (bones.stream/submit bones-job)
        ;; time needed to start the job and start the consumer
        (Thread/sleep 8000)
        (p/input bones-job {:topic "test"
                            :key "125"
                            :message {:hi "yo" :ya "watsup"}})

        ;; way more time than needed to allow the segment to flow
        (Thread/sleep 1000)
        ;; value is the same as message
        (is (= {:key "125", :value {"hi" "yo", "ya" "watsup"}}
               (first @(.fetch-all (bones.stream.redis/map->Redis {}) "test"))))
        ))))
