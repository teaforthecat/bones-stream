(ns bones.stream.jobs-test
  (:require [bones.stream
             [jobs :as jobs]
             [kafka :as kafka]]
            [clojure.test :refer [deftest is testing]]
            [com.stuartsierra.component :as component]
            [onyx api
             [test-helper :refer [load-config with-test-env]]]
            [onyx.plugin.kafka :as ok]
            [bones.stream.protocols :as p]))

(defn abc [segment]
  segment)

(deftest workflow
  (testing "bare bones"
    (let [wf (jobs/bare-workflow)]
      (is (= [[:bones/input :bones/output]]
             wf))))
  (testing "single function builds a connected pipeline"
    (let [wf (jobs/single-function-workflow ::abc)]
      (is (= [[:bones/input ::abc]
              [::abc :bones/output]])))))

(deftest catalog
  (testing "bare bones"
    (let [c (jobs/bare-catalog ::abc "test")]
      (is (= [{:onyx/name :bones/input
                :onyx/type :input
                :onyx/fn :bones.stream.kafka/fix-key
                :onyx/medium :kafka
                :onyx/plugin :onyx.plugin.kafka/read-messages
                :onyx/max-peers 1
                :onyx/batch-size 1
                :kafka/zookeeper "localhost:2181"
                :kafka/topic "test"
                :kafka/deserializer-fn :bones.stream.jobs/unserfun
                :kafka/offset-reset :latest
                :kafka/wrap-with-metadata? true
                }
               {:onyx/name :bones/output
                :onyx/type :output
                :onyx/fn :bones.stream.redis/redis-write
                :onyx/medium :function
                :onyx/plugin :onyx.peer.function/function
                :bones.stream.jobs/channel "test"
                :onyx/params [:bones.stream.jobs/channel]
                :onyx/batch-size 1}]
             c))))
  (testing "add-fn-task"
    (let [c (jobs/bare-catalog ::abc "test")
          [_ _ cl] (jobs/add-fn-task c ::abc)]
      (is (= {:onyx/name ::abc
              :onyx/type :function
              :onyx/batch-size 1
              :onyx/fn ::abc}
             cl)))))

(deftest lifecycles
  (testing "bare bones"
    (let [lc (jobs/bare-lifecycles :abc)]
      (is (= [{:lifecycle/task :bones/input
               :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}]
             lc)))))

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
