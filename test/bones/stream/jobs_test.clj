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
  (testing "adds metadata about which service a task uses"
    (are [result service] (-> (meta result)
                              (get :bones/service)
                              (= service))
      (jobs/kafka-input-task {}) :kafka
      (jobs/redis-output-task {}) :redis))
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
      (is (= "bones-input" (:kafka/topic (first catalog))))))
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
      (is (= "bones-output" (:redis/channel (first catalog)))))))


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
                              (jobs/input :kafka {:serialization-format :json-plain})
                              (jobs/function ::my-inc)
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
