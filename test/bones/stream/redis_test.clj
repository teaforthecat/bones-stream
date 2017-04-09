(ns bones.stream.redis-test
  (:require [bones.conf :as conf]
            [bones.stream.redis :as redis]
            [clojure.test :refer [deftest testing is use-fixtures run-tests]]
            [manifold.stream :as ms]
            [com.stuartsierra.component :as component]
            [bones.stream.kafka :as kafka]))


(def sys (atom {}))

(defn setup [test]
  (swap! sys assoc :conf (conf/map->Conf {:files ["conf/test.edn"]}))
  (swap! sys assoc :redis (component/using
                           (redis/map->Redis {})
                           [:conf]))
  (swap! sys component/start-system [:conf])
  (swap! sys component/start-system [:redis])
  (test)
  (swap! sys component/stop-system)
  ;; todo clear data from redis
  )

(use-fixtures :once setup)

(deftest pubsub
  (testing "pubsub serialization"
    (let [stream (ms/stream)
          _ (.subscribe (:redis @sys) "123" stream)
          _ (.publish (:redis @sys) "123" {:abc 123})
          result (ms/take! stream)]
      (is (= {:abc 123} @result)))))


(deftest materialized-view
  (testing "write a value, read a value"
    (let [r (:redis @sys)
          _ @(redis/write r "test" 123 {:abc 123})
          m @(redis/fetch r 123)]
      (is (= {:key 123, :value {:abc 123}} m))))
  (testing "fetches a list of keys"
    (let [r (:redis @sys)
          _ @(redis/write r "test" 123 {:abc 123})]
      (is (= ["123"] @(redis/fetch-keys r "test" )))))
  (testing "read a set of values"
    (let [r (:redis @sys)
          _ @(redis/write r "test" 123 {:abc 123})
          ms @(redis/fetch-all r "test")]
      ;; notice the keys are turned into strings
      (is (= [{:key "123" :value {:abc 123}}] ms)))))
