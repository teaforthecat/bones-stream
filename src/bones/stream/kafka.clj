(ns bones.stream.kafka
  (:require [com.stuartsierra.component :as component]
            [onyx.plugin.kafka :as ok]
            [onyx.plugin.protocols :as p]
            [onyx.kafka.utils]
            [franzy.clients.producer.protocols :refer [send-async! send-sync!]]
            [franzy.serialization.serializers :refer [byte-array-serializer]]
            [bones.stream.serializer :as serializer]
            [bones.stream.redis :as redis]
            [com.stuartsierra.component :as component]
            [manifold.stream :as ms]
            [manifold.deferred :as d])
  (:import [franzy.clients.producer.types ProducerRecord]))

(defn fix-key [segment]
  ;; :kafka/wrap-with-metadata? must be set to true to get the key
  (if (:key segment)
    ;; key is not de-serialized by onyx-kafka; must be an oversight
    (let [new-segment 1 #_(update segment :key unserfun)]
      new-segment)))

(defn redis-write [redi channel message]
  (let [k (:key message)
        v (:message message)]
    (redis/write redi channel k v)
    (redis/publish redi channel message)))

(defn bare-workflow []
  [[:bones/input :bones/output]])

(defn input-task [topic conf]
  (merge {:onyx/name :bones/input
          :onyx/type :input
          :onyx/fn ::fix-key ;; preprocessor
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
          :onyx/fn ::redis-write
          :onyx/medium :function
          :onyx/plugin :onyx.peer.function/function
          ::channel topic ;; the second param sent to ::redis-write
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

;; taken from onyx.plugin.kafka because it is private
(defn message->producer-record
  [serializer-fn topic m]
  (let [message (:message m)
        k (some-> m :key serializer-fn)
        p (some-> m :partition int)
        message-topic (get m :topic topic)]
    (cond (not (contains? m :message))
          (throw (ex-info "Payload is missing required. Need message key :message"
                          {:recoverable? false
                           :payload m}))

          (nil? message-topic)
          (throw (ex-info
                  (str "Unable to write message payload to Kafka! "
                       "Both :kafka/topic, and :topic in message payload "
                       "are missing!")
                  {:recoverable? false
                   :payload m}))
          :else
          (ProducerRecord. message-topic p k (serializer-fn message)))))


;; not sure if this will work
;; where/when to call this? user only?
;; 1. the ::serfun needs to resolve
;; 2. they need to match
;; 3. provide :kafka/serializer-fn to override
(defn serialization-format
  ([]
   (serialization-format :json-plain))
  ([fmt]
   (def serfun (serializer/encoder (or fmt :json-plain)))
   (def unserfun (serializer/decoder (or fmt :json-plain)))))

;; (serialization-format)

(defn producer
  "create a kafka producer.
  CONF is a map that gets merged with development defaults
  provide:
      :kafka/zookeeper host:port
      :kafka/topic is optional can be in the message
      :kafka/serializer-fn should match the :kafka/deserializer-fn"
  [conf]
  (let [kafka-args (select-keys conf [:kafka/topic
                                      :kafka/zookeeper
                                      :kafka/serializer-fn
                                      :kafka/request-size
                                      :kafka/partition
                                      :kafka/no-seal?
                                      :kafka/producer-opts])]
    (ok/write-messages {:onyx.core/task-map
                        (merge
                         {:kafka/zookeeper "127.0.0.1:2181"
                          :kafka/serializer-fn ::serfun}
                         kafka-args)})))

(comment
  ;; configure
  (serialization-format)

  ;; produce
  (send-sync!
   (:producer (producer {:kafka/topic topic}))
   (message->producer-record serfun topic {:message "yo!"
                                           :key "123"}))

 ;; consume
  (future
    (println
     (onyx.kafka.utils/take-now "127.0.0.1:2181" topic unserfun)
     ))

  ;; fetch
  @(.fetch-all (bones.stream.redis/map->Redis {}) topic)

  )

(defprotocol InputOutput
  (input [_ msg])
  (output [_ stream]))

(defrecord Job [conf onyx-job redis]
  component/Lifecycle
  (start [cmp]
    ;; look for the :bones/input task to reuse kafka connection info
    (let [input-task (first (filter #(= :bones/input (:onyx/name %)) (:catalog (:onyx-job cmp))))
          peer-config (get-in cmp [:conf :stream :peer-config])
          env-config (get-in cmp [:conf :stream :env-config])
          ;; job-id defaults to topic, used to kill-job
          job-id (get-in cmp
                         [:onyx-job :metadata :job-id]
                         (:kafka/topic input-task))]

      ;; this seems pretty neat
      (onyx.api/submit-job peer-config
                           ;; job-id makes this idempotent
                           (assoc-in (:onyx-job cmp)
                                      [:metadata :job-id]
                                      job-id))


      ;; use all the values set for the kafka reader for this kafka writer
      ;; (mainly topic)
      (assoc cmp :producer (producer input-task)
                 :job-id job-id)))
  (stop [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])]
      (.close (:producer cmp)) ;; is this right?
      (onyx.api/kill-job peer-config (:job-id cmp))
      (assoc cmp :producer nil)))
  InputOutput
  ;; returns result from kafka :offset,etc.
  (input [cmp msg]
    ;; reuse info from the reader task we found in (start)
    (let [{:keys [:topic :producer :serializer-fn]} (:producer cmp)]
      (send-sync! producer
                  (message->producer-record serializer-fn
                                            topic
                                            msg))))
  (output [cmp stream] ;; provide ms/stream
    ;; redis provided by component's start-system
    (redis/subscribe (:redis cmp) (get-in cmp [:producer :topic]) stream)
    cmp))
