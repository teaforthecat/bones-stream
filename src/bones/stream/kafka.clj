(ns bones.stream.kafka
  (:require [bones.stream.redis :as redis]
            [franzy.clients.producer.protocols :refer [send-sync!]]
              [franzy.clients.producer.client :as producer]
              [franzy.serialization.deserializers :as deserializers]
              [franzy.serialization.serializers :as serializers]
              [franzy.admin.topics :as k-topics]
              [franzy.admin.zookeeper.client :as k-admin]
              ;; testing:
              [franzy.clients.consumer.client :as consumer]
              [franzy.clients.consumer.protocols :refer [subscribe-to-partitions!
                                                        partition-subscriptions
                                                        poll!
                                                        records-by-topic]]
              [manifold.stream :as s]

              onyx.kafka.utils
              [onyx.plugin.kafka :as ok]
              [taoensso.timbre :refer [debug]])
    (:import [franzy.clients.producer.types.ProducerRecord]
            [org.apache.kafka.common.errors.TopicExistsException]))

  (defn fix-key [segment]
    (debug "fix-key: " segment)
    ;; :kafka/wrap-with-metadata? must be set to true to get the key
    (if (:key segment)
      ;; key is not de-serialized by onyx-kafka; must be an oversight
      (let [new-segment (update segment :key (find-var 'bones.stream.jobs/unserfun))]
        new-segment)
      segment))

  (defn create-topic [ {:keys [:kafka/zookeeper
                               :kafka/topic
                               :zk/session-timeout
                               :zk/is-secure?
                               :kafka/partitions
                               :kafka/replication-factor]
                        :or {:kafka/partitions 1
                             :kafka/replication-factor 1
                             :zk/is-secure? false}
                        :as zopts}]
    (try
      (k-topics/create-topic!
       (k-admin/make-zk-utils (cond-> {}
                                session-timeout
                                (assoc :session-timeout session-timeout)
                                zookeeper
                                (assoc :servers zookeeper))
                              (boolean is-secure?))
       topic
       partitions
       replication-factor)
      (catch kafka.common.TopicExistsException e
        ;; same as success, makes this idempotent
        nil)))

  (defn writer
    "create a kafka producer reusing conf from onyx job

    CONF is a map that gets merged with development defaults
    provide:
        :kafka/zookeeper host:port
        :kafka/topic is optional as it can also be in the data sent to `send-sync!'
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

  (defn produce [prdcr topic serializer-fn msg]
    (send-sync! prdcr (merge {:topic topic}
                             msg ;; may override topic or add partition
                             {:key (some-> msg :key serializer-fn)
                              :value (some-> msg :value serializer-fn)})))
