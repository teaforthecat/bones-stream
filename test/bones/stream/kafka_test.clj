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

;; waiting on onyx-redis
;; (defn abc [segment]
;;   [{:op :set :args ["test1" segment]}
;;    {:op :sadd :args ["test" "test1"]}
;;    {:op :publish :args ["test" segment]}] )

(defn abc [segment]
  (println "hello from abc")
  segment)

(deftest workflow
  (testing "bare bones"
    (let [wf (kafka/bare-workflow ::abc)]
      (is (= [[:kafka-reader :processor]
              [:processor :redis-writer]]
             wf)))))

(deftest catalog
  (testing "bare bones"
    (let [c (kafka/bare-catalog ::abc)]
      (is (= [{:onyx/name :bones/input
               :onyx/type :input
               :onyx/medium :kafka
               :onyx/plugin :onyx.plugin.kafka/read-messages
               :onyx/max-peers 1 ;; for read exactly once
               :onyx/batch-size 50
               :kafka/zookeeper "localhost:2181"
               :kafka/topic "test"
               :kafka/deserializer-fn ::kafka/unserfun
               :kafka/offset-reset :earliest}

              {:onyx/name :processor
               :onyx/type :function
               :onyx/max-peers 1
               :onyx/batch-size 1 ;; turns 1 segment to 2
               :onyx/fn ::abc}

              {:onyx/name :bones/output
               :onyx/type :function
               :onyx/max-peers 1
               :onyx/batch-size 1 ;; turns 1 segment to 2
               :onyx/fn ::abc}

               ;; waiting on onyx-redis
              #_{:onyx/name :redis-writer
               :onyx/plugin :onyx.plugin.redis/writer
               :onyx/type :output
               :onyx/medium :redis
               :redis/uri "redis://127.0.0.1:6379"
               :redis/allowed-commands [:publish :srem :del :sadd :set]
               ;; 2 segments at a time, one for publish, one for set
               :onyx/batch-size 2}
              ])))))

(deftest lifecycles
  (testing "bare bones"
    (let [lc (kafka/bare-lifecycles :abc)]
      (is (= [{:lifecycle/task :kafka-reader
               :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}
              ;; no lifecycle needed for redis
              ])))))


(deftest job
  (let [
        redis (component/start (bones.stream.redis/map->Redis {}))
        config (load-config "dev-config.edn")
        topic "test"

        tenancy-id (UUID/randomUUID)
        env-config (assoc (:env-config config) :onyx/tenancy-id tenancy-id)
        peer-config (assoc (:peer-config config) :onyx/tenancy-id tenancy-id
                           :onyx.peer/fn-params {:bones/output [redis]})


        command-ns ::abc
        job {:workflow (kafka/bare-workflow command-ns)
             :catalog (kafka/bare-catalog command-ns)
             :lifecycles (kafka/bare-lifecycles command-ns)
             :task-scheduler :onyx.task-scheduler/balanced}
        bones-job (component/start (kafka/map->Job {:onyx-job job}))]

    (with-test-env [test-env [3 env-config peer-config]]
      (onyx.api/submit-job peer-config job)
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


;; (ns bones.stream.kafka-test
;;   (:require [bones.conf :as conf]
;;             [bones.stream.kafka :as kafka]
;;             [clojure.test :refer [deftest is run-tests testing use-fixtures]]
;;             [com.stuartsierra.component :as component]
;;             [manifold.stream :as ms]
;;             [manifold.deferred :as d]))

;; (defn producer-stub [record]
;;   ;; really returns something like this:
;;   ;; {:topic "topic" :partition 0 :offset 0}
;;   ;; but here we'll return the arg to test the serialization
;;   (future record))

;; ;; a mock KafkaMessage
;; (def consumer-stub (constantly (lazy-seq [{:topic "topic" :offset 0 :partition 0
;;                                             :key (.getBytes "456")
;;                                             :value (.getBytes "{\"abc\": 123}")}])))
;; ;; helper function
;; (defn to-s [bytearray]
;;   (apply str (map char bytearray)))

;; (def sys (atom {}))
;; (swap! sys assoc :conf (conf/map->Conf {:files ["conf/test.edn"]
;;                                         :stream {:serialization-format :json-plain}}))
;; (swap! sys assoc :producer (component/using
;;                             (kafka/map->Producer {:conn producer-stub})
;;                             [:conf]))
;; (swap! sys assoc :consumer (component/using
;;                             (kafka/map->Consumer {:conn consumer-stub})
;;                             [:conf]))
;; (swap! sys component/start-system [:conf])
;; (swap! sys component/start-system [:producer])
;; (swap! sys component/start-system [:consumer])

;; (def messages (atom []))

;; (defn topic-handler [message]
;;   (swap! messages conj message))

;; (deftest serialization
;;   (testing "send to kafka - the serializer"
;;     (let [result @(.produce (:producer @sys) "topic" "key" {:data true})]
;;       (is (= "{\"data\":true}" (apply str (map char (.value result)))))))
;;   (testing "retreiving from kafka - the deserializer"
;;     (let [fut (.consume (:consumer @sys) "topic" topic-handler)]
;;       (let [ ;blocking deref for tests only, this should not be called in the application
;;             f @fut
;;             msg (first @messages)]
;;         (is (= nil f)) ;; (doseq returns nil)
;;         (is (= 0           (:offset msg)))
;;         (is (= 0           (:partition msg)))
;;         (is (= "topic"     (:topic msg)))
;;         (is (= 456         (:key msg)))
;;         (is (= {"abc" 123} (:value msg)))))))
