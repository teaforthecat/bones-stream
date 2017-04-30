(ns bones.stream.kafka
  (:require [bones.stream.redis :as redis]
            [franzy.clients.producer.protocols :refer [send-sync!]]
            onyx.kafka.utils
            [onyx.plugin.kafka :as ok]
            [taoensso.timbre :refer [debug]])
  (:import franzy.clients.producer.types.ProducerRecord))

(defn fix-key [segment]
  (debug "fix-key: " segment)
  ;; :kafka/wrap-with-metadata? must be set to true to get the key
  (if (:key segment)
    ;; key is not de-serialized by onyx-kafka; must be an oversight
    (let [new-segment (update segment :key (find-var 'bones.stream.jobs/unserfun))]
      new-segment)))

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
                          :kafka/serializer-fn :bones.stream.jobs/serfun}
                         kafka-args)})))

(defn produce [prdcr serializer-fn topic msg]
  (send-sync! prdcr
              (message->producer-record serializer-fn
                                        topic
                                        msg)))

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
