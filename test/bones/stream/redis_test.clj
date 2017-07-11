(ns bones.stream.redis-test
  (:require [bones.stream.redis :as redis]
            [clojure.test :refer [deftest testing is use-fixtures run-tests]]
            [manifold.stream :as ms]
            [com.stuartsierra.component :as component]
            [bones.stream.protocols :as p]))


;; the default spec could be left here out but this provides an example
(def redi (redis/map->Redis {:spec {:host "127.0.0.1" :port 6379}}))

(deftest pubsub
  (testing "pubsub serialization"
    (let [stream (ms/stream)
          _ (p/subscribe redi "test123" stream)
          _ (p/publish redi "test123" {:abc 123})
          result (ms/take! stream)]
      (is (= {:abc 123} @result)))))


(deftest materialized-view
  (testing "write a value, read a value"
    (let [_ @(p/write redi "test" 123 {:abc 123})
          m @(p/fetch redi 123)]
      (is (= {:key 123, :value {:abc 123}} m))))
  (testing "fetches a list of keys"
    (let [_ @(p/write redi "test" 123 {:abc 123})]
      (is (= ["123"] @(p/fetch-keys redi "test" )))))
  (testing "read a set of values"
    (let [_ @(p/write redi "test" 123 {:abc 123})
          ms @(p/fetch-all redi "test")]
      ;; notice the keys are turned into strings
      (is (= [{:key "123" :value {:abc 123}}] ms)))))
