(ns bones.stream.jobs
  (:require [bones.stream.serializer :as serializer]
            [bones.stream.redis :as redis]
            [bones.stream.kafka :as k]))



;; not sure if this will work
;; where/when to call this? user only?
;; 1. the ::serfun needs to resolve
;; 2. they need to match
;; 3. provide :kafka/serializer-fn to override
(defn serialization-format [fmt]
  {:pre (some #{fmt} #{:json :json-verbose :msgpack :json-plain})}
  (def serfun (serializer/encoder fmt))
  (def unserfun (serializer/decoder fmt)))

(defn bare-workflow []
  [[:bones/input :bones/output]])

(defn input-task [topic conf]
  (merge {:onyx/name :bones/input
          :onyx/type :input
          :onyx/fn ::k/fix-key ;; preprocessor
          :onyx/medium :kafka
          :onyx/plugin :onyx.plugin.kafka/read-messages
          :onyx/max-peers 1 ;; for read exactly once
          :onyx/batch-size 1
          :kafka/zookeeper "localhost:2181"
          :kafka/topic topic
          :kafka/deserializer-fn ::unserfun
          :kafka/offset-reset :latest
          :kafka/wrap-with-metadata? true
          }
         conf))

(defn output-task [topic conf]
  (merge {:onyx/name :bones/output
          :onyx/type :output
          :onyx/fn ::redis/redis-write
          :onyx/medium :function
          :onyx/plugin :onyx.peer.function/function
          ;; the SECOND param sent to ::redis-write
          ::channel topic
          :onyx/params [::channel]
          :onyx/batch-size 1}
         conf))

(defn bare-catalog [fn-sym topic]
  [(input-task topic {})
   (output-task topic {})])

(defn bare-lifecycles [fn-sym]
  [{:lifecycle/task :bones/input
    :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}])

(defn sym-to-topic
  "generate a kafka-acceptable topic name
  (sym-to-topic :a.b/c) => \"a.b..c\" "
  [^clojure.lang.Keyword job-sym]
  (-> (str job-sym)
      (clojure.string/replace "/" "..") ;; / is illegal in kafka topic name
      (subs 1))) ;; remove leading colon

(defn bare-job
  "bare minimum"
  ([fn-sym]
   (bare-job fn-sym (sym-to-topic fn-sym)))
  ([fn-sym topic]
   {:workflow (bare-workflow)
    :catalog (bare-catalog fn-sym topic)
    :lifecycles (bare-lifecycles fn-sym)
    :task-scheduler :onyx.task-scheduler/balanced}))

(defn add-fn-task [catalog fn-sym]
  (conj catalog
        {:onyx/name fn-sym
         :onyx/type :function
         :onyx/batch-size 1
         :onyx/fn fn-sym}))

(defn single-function-workflow [fn-sym]
  [[:bones/input fn-sym]
   [fn-sym :bones/output]] )

(defn single-function-job
  "bare minimum plus one function"
  ([fn-sym]
   (single-function-job fn-sym (sym-to-topic fn-sym)))
  ([fn-sym topic]
   {:workflow (single-function-workflow fn-sym)
    :catalog (add-fn-task (bare-catalog fn-sym topic) fn-sym)
    :lifecycles (bare-lifecycles fn-sym)
    :task-scheduler :onyx.task-scheduler/balanced}))
