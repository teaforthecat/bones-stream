(ns bones.stream.serializer-test
  (:require [bones.stream.serializer :as serializer]
            [clojure.test :refer [deftest is run-tests testing use-fixtures]]))

;; helper function
(defn to-s [bytearray]
  (apply str (map char bytearray)))

(deftest serialization
  (testing "json format"
    (let [data {:abc 123}
          ser (serializer/encoder :json)
          ser-data (ser data)]
      (is (= "[\"^ \",\"~:abc\",123]" (to-s ser-data)))))
  (testing "json-verbose format"
    (let [data {:abc 123}
          ser (serializer/encoder :json-verbose)
          ser-data (ser data)]
      (is (= "{\"~:abc\":123}" (to-s ser-data)))))
  (testing "msgpack format"
    (let [data {:abc 123}
          ser (serializer/encoder :msgpack)
          deser (serializer/decoder :msgpack)
          ser-data (ser data)]
      ;; msgpack is binary
      (is (= {:abc 123} (deser ser-data)))))
  (testing "json-plain format"
    (let [data {:abc 123}
          ser (serializer/encoder :json-plain)
          ser-data (ser data)]
      (is (= "{\"abc\":123}" (to-s ser-data))))))
