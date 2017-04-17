(ns bones.stream.kafka-test
  (:require [clojure.test :refer [deftest is testing]]
            [onyx.plugin.kafka :as ok]
            [onyx.api]
            ;; [onyx.plugin.redis :as oredi] ;; not 1.9 compatible
            [franzy.clients.producer.protocols :refer [send-async! send-sync!]]
            [bones.stream.serializer :as serializer]
            [bones.stream.kafka :as kafka]
            [com.stuartsierra.component :as component]
            [onyx.test-helper :refer [with-test-env load-config]]
            [manifold.stream :as ms]
            [manifold.deferred :as d])
  (:import [franzy.clients.producer.types ProducerRecord]
           [java.util UUID]))

(defn abc [segment]
  segment)

(deftest workflow
  (testing "bare bones"
    (let [wf (kafka/bare-workflow ::abc)]
      (is (= [[:bones/input :processor]
              [:processor :bones/output]]
             wf)))))

(deftest catalog
  (testing "bare bones"
    (let [c (kafka/bare-catalog ::abc "test")]
      (is (= [{:onyx/name :bones/input
                :onyx/type :input
                :onyx/fn :bones.stream.kafka/fix-key
                :onyx/medium :kafka
                :onyx/plugin :onyx.plugin.kafka/read-messages
                :onyx/max-peers 1
                :onyx/batch-size 1
                :kafka/zookeeper "localhost:2181"
                :kafka/topic "test"
                :kafka/deserializer-fn :bones.stream.kafka/unserfun
                :kafka/offset-reset :latest
                :kafka/wrap-with-metadata? true
                }

               {:onyx/name :processor
                :onyx/type :function
                :onyx/max-peers 1
                :onyx/batch-size 1
                :onyx/fn ::abc}

               {:onyx/name :bones/output
                :onyx/type :output
                :onyx/fn :bones.stream.kafka/redis-write
                :onyx/medium :function
                :onyx/plugin :onyx.peer.function/function
                :bones.stream.kafka/channel "test"
                :onyx/params [:bones.stream.kafka/channel]
                :onyx/batch-size 1}]
             c)))))

(deftest lifecycles
  (testing "bare bones"
    (let [lc (kafka/bare-lifecycles :abc)]
      (is (= [{:lifecycle/task :bones/input
               :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}]
             lc)))))


(deftest job
  (let [redis (component/start (bones.stream.redis/map->Redis {}))
        config (load-config "dev-config.edn")
        topic "test"

        env-config  (:env-config config)
        peer-config (assoc (:peer-config config)
                           :onyx.peer/fn-params {:bones/output [redis]})


        command-ns ::abc
        job {:workflow (kafka/bare-workflow command-ns)
             :catalog (kafka/bare-catalog command-ns topic)
             :lifecycles (kafka/bare-lifecycles command-ns)
             :task-scheduler :onyx.task-scheduler/balanced}
        bones-job (component/start (kafka/map->Job {:onyx-job job
                                                    :conf conf}))]

    (with-test-env [test-env [3 env-config peer-config]]
      (onyx.api/submit-job peer-config job)
      (bones.stream/submit bones-job)
      ;; time needed to start the job and start the consumer
      (Thread/sleep 8000)
      (kafka/input bones-job {:topic "test"
                              :key "125"
                              :message {:hi "yo" :ya "watsup"}})

      ;; way more time than needed to allow the segment to flow
      (Thread/sleep 1000)
      ;; value is the same as message
      (is (= {:key "125", :value {"hi" "yo", "ya" "watsup"}}
             (first @(.fetch-all (bones.stream.redis/map->Redis {}) "test"))))
      )))

;; TODO next set serialization-format
;; (def sys (atom {}))
;; (swap! sys assoc :conf (conf/map->Conf {:files ["conf/test.edn"]
;;                                         :stream {:serialization-format :json-plain}}))
