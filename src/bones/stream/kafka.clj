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
  (:import [franzy.clients.producer.types ProducerRecord])
  )

(defn redis-write [redi channel message]
  (spit "onyx.log" "hello from redis-write")
  (let [k (:key message)
        v (:value message)]
    (redis/write redi channel k v)
    (redis/publish redi channel message)))

(defn bare-workflow [fn-sym]
  [[:bones/input :processor]
   [:processor :bones/output]])

(defn bare-catalog [fn-sym]
  [{:onyx/name :bones/input
    :onyx/type :input
    :onyx/medium :kafka
    :onyx/plugin :onyx.plugin.kafka/read-messages
    :onyx/max-peers 1 ;; for read exactly once
    :onyx/batch-size 50
    :kafka/zookeeper "localhost:2181"
    :kafka/topic "test"
    :kafka/deserializer-fn ::unserfun
    :kafka/offset-reset :earliest
    }

   {:onyx/name :processor
    :onyx/type :function
    :onyx/max-peers 1
    :onyx/batch-size 1 ;; turns 1 segment to 2
    :onyx/fn fn-sym}

   {:onyx/name :bones/output
    :onyx/type :output
    :onyx/fn ::redis-write
    :onyx/medium :function
    :onyx/plugin :onyx.peer.function/function
    :onyx/params ["test"] ;; seccond parameter to redis-write (the channel); after :onyx.peer/fn-params
    ;; 3 segments at a time, one for publish, one for set, one for sadd(key to set)
    :onyx/batch-size 3
    }
   ;; waiting on onyx-redis
   #_{:onyx/name :bones/output
    :onyx/plugin :onyx.plugin.redis/writer
    :onyx/type :output
    :onyx/medium :redis
    :redis/uri "redis://127.0.0.1:6379"
    :redis/allowed-commands [:publish :srem :del :sadd :set]
    ;; 3 segments at a time, one for publish, one for set, one for sadd(key to set)
    :onyx/batch-size 3}
   ])

(defn bare-lifecycles [fn-sym]
  [{:lifecycle/task :bones/input
    :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}
   ;; no lifecycle needed for redis
   ])

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


(def topic "test-topic")

;; not sure if this will work
;; where/when to call this? user only?
;; the ::serfun needs to resolve
;; they need to match
;; provide :kafka/serializer-fn to override
(defn serialization-format
  ([]
   (serialization-format :json-plain))
  ([fmt]
   (def serfun (serializer/encoder fmt))
   (def unserfun (serializer/decoder fmt))))

;; (serialization-format)

(defn producer
  "create a kafka producer.
  CONF is a map that gets merged with development defaults
  prodvide
      :kafka/zookeeper url
      :kafka/topic is optional can be in the message
      :kafka/serializer-fn should match the :kafka/deserializer-fn"
  [conf]
  (let [;; kafka-args (remove #(not= "kafka" (namespace (first %)))
                           ;; conf)
        ;; args (reduce merge (map (partial apply hash-map) kafka-args))
        ;; maybe it isn't the best idea to reuse this configuration
        kafka-args (select-keys conf [:kafka/topic
                                      :kafka/zookeeper
                                      :kafka/serializer-fn
                                      :kafka/request-size
                                      :kafka/partition
                                      :kafka/no-seal?
                                      :kafka/producer-opts])]
    (ok/write-messages {:onyx.core/task-map
                        (merge
                         {:kafka/zookeeper "127.0.0.1:2181"
                          ;; :kafka/topic topic
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
     )))

;; TODO: builder
;; (defn consumer-task [task]
;;   (merge
;;    {:onyx/name :read-commands
;;     :onyx/type :input
;;     :onyx/medium :kafka
;;     :onyx/plugin :onyx.plugin.kafka/read-messages
;;     :onyx/max-peers 1
;;     :onyx/batch-size 50
;;     ;; :kafka/zookeeper kafka-zookeeper
;;     ;; :kafka/topic
;;     :kafka/deserializer-fn ::unserfun
;;     :kafka/offset-reset :earliest}
;;          task))

(defprotocol InputOutput
  (input [_ msg])
  ;; maybe some particular stream or something
  (output [_ args]))

(defrecord Job [onyx-job redis]
  component/Lifecycle
  (start [cmp]
    (let [input-task (first (filter #(= :bones/input (:onyx/name %)) (:catalog (:onyx-job cmp))))
          output-task (first (filter #(= :bones/output (:onyx/name %)) (:catalog (:onyx-job cmp))))
          ;; {:keys [:redis/uri]} output-task
          ]

      ;; use all the values set for the kafka reader for this kafka writer
      ;; (mainly topic)
      (assoc cmp :producer (producer input-task))))
  (stop [cmp]
    (.close (:producer cmp)) ;; is this right?
    (assoc cmp :producer nil))
  InputOutput
  ;; returns result from kafka :offset,etc.
  (input [cmp msg]
    ;; reuse info from the reader task we found in (start)
    (let [{:keys [:topic :producer :serializer-fn]} (:producer cmp)]
      (send-sync! producer
                  (message->producer-record serializer-fn
                                            topic
                                            msg))))
  (output [cmp channel]
    ;; redis message format for onyx-redis:
    ;; {:op :operation :args [arg1, arg2, arg3]}
    ;; {:op :publish :args ["channel-name", {msg: data}]}
    ;; (redis/subscribe (:redis cmp) )
    ;; some stream from :bones/output
    (let [ ;;output-task (filter #(= :bones/output (:onyx/name %)) (:catalog (:onyx-job cmp)))
          ;; {:keys [:redis/uri]} output-task
          ]

      )
    cmp))



;; this will have to be done elsewhere to ensure it is only done once
;; (let [frmt (get-in cmp [:conf :stream :serialization-format] )]
;;   (if frmt
;;     ;;
;;     ;; set global default format. can be overridden with custom serializer, and
;;     ;; configured per task
;;     (serialization-format frmt)))


;; (ns bones.stream.kafka
;;   (:require [clj-kafka.new.producer :as nkp]
;;             [clj-kafka.consumer.zk :as zkc]
;;             [bones.stream.serializer :as serializer]
;;             [com.stuartsierra.component :as component]
;;             [manifold.stream :as ms]
;;             [manifold.deferred :as d]))

;; (def default-format :msgpack )

;; (defprotocol Produce
;;   (produce [this topic key data])
;;   (produce-stream [this stream & {:as opts}]))

;; (defprotocol Consume
;;   (consume [this topic stream]))

;; (defrecord Producer [conf conn]
;;   component/Lifecycle
;;   (stop [cmp]
;;     (if (:producer cmp)
;;       (do
;;         (.close (:producer cmp))
;;         ;; dissoc changes the type
;;         (assoc cmp :conn nil :producer nil))
;;       cmp))
;;   (start [cmp]
;;     (if (:producer cmp)
;;       cmp
;;       (let [config (get-in cmp [:conf :stream])
;;             producer-config (select-keys (merge {"bootstrap.servers" "127.0.0.1:9092"}
;;                                                 config)
;;                                          ["bootstrap.servers"])
;;             {:keys [serialization-format]
;;              :or {serialization-format default-format}} config
;;             producer (nkp/producer producer-config
;;                                    ;; passthru key serializer
;;                                    (nkp/byte-array-serializer)
;;                                    ;; passthru value serializer
;;                                    (nkp/byte-array-serializer))
;;             ;; check for conn so we don't need a connection to run tests
;;             conn (if (:conn cmp)
;;                    (:conn cmp)
;;                    (partial nkp/send producer))]
;;         (-> cmp
;;             (assoc :config config) ;; for debugging
;;             (assoc :serialization-format serialization-format) ;; for debugging
;;             (assoc :serializer (serializer/encoder serialization-format))
;;             ;; store producer to call .close on
;;             (assoc :producer producer)
;;             ;; build a conn function that is easy to stub in tests
;;             (assoc :conn conn)))))
;;   Produce
;;   (produce [cmp topic key data]
;;     (let [key-bytes (.getBytes key)
;;           data-bytes ((:serializer cmp) data)
;;           record (nkp/record topic
;;                              key-bytes
;;                              data-bytes)]
;;       ((:conn cmp) record)))
;;   (produce-stream [cmp stream & {:as opts}]
;;     (ms/consume
;;      #(let [{:keys [topic key value]} (merge opts %)]
;;         (produce cmp topic key value))
;;      stream)))

;; (defrecord Consumer [conf conn]
;;   component/Lifecycle
;;   (stop [cmp]
;;     (if (:consumer cmp) ;;idempotent stops
;;       (do
;;         (zkc/shutdown (:consumer cmp))
;;         ;; dissoc changes the type
;;         (assoc cmp :conn nil :consumer nil))
;;       cmp))
;;   (start [cmp]
;;     (if (:consumer cmp)
;;       cmp ;;idempotent starts
;;       (let [config (get-in cmp [:conf :stream])
;;             consumer-config (select-keys (merge {"zookeeper.connect" "127.0.0.1:2181"
;;                                                  "group.id"  "bones.stream"
;;                                                  "auto.offset.reset" "smallest"}
;;                                                 config)
;;                                          ["zookeeper.connect"
;;                                           "group.id"
;;                                           "auto.offset.reset"])
;;             {:keys [serialization-format]
;;              :or {serialization-format default-format}} config
;;             ;; check for conn so we don't need a connection to run tests
;;             consumer (zkc/consumer consumer-config)
;;             conn (if (:conn cmp)
;;                    (:conn cmp)
;;                    (partial zkc/messages consumer))]
;;         (-> cmp
;;             (assoc :deserializer (serializer/decoder serialization-format))
;;             ;; store consumer to call .shutdown on
;;             (assoc :consumer consumer)
;;             ;; build a conn function that is easy to stub in tests
;;             (assoc :conn conn)))))
;;   Consume
;;   (consume [cmp topic handler]
;;     (future
;;       (doseq [msg ((:conn cmp) topic)]
;;         (-> msg
;;              (update :key (:deserializer cmp))
;;              (update :value (:deserializer cmp))
;;              handler)))))
