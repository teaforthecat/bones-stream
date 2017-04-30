(ns bones.stream.pipelines
  (:require [com.stuartsierra.component :as component]
            [bones.stream.protocols :as p]
            [bones.stream.kafka :as k]
            [onyx.api]
            [bones.stream.redis :as redis]))

;; TODO: make composable
(defrecord KafkaRedis [conf onyx-job redis]
  component/Lifecycle
  (start [cmp]
    ;; look for the :bones/input task to reuse kafka connection info
    (let [input-task  (first (filter #(= :bones/input (:onyx/name %)) (:catalog (:onyx-job cmp))))

          ;; the FIRST parameter sent to ::redis/redis-write
          peer-config (assoc (get-in cmp [:conf :stream :peer-config])
                             :onyx.peer/fn-params {:bones/output [(:redis cmp)]})
          env-config (get-in cmp [:conf :stream :env-config])
          n-peers 3 ;; or greater

          ;; start-env is for dev only I guess
          ;; dev-env (if (:zookeeper/server? env-config)) <- something like this???
          dev-env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          peers (onyx.api/start-peers n-peers peer-group)

          ;; submit-job is idempotent if job-id is set(UUID)
          {:keys [job-id]} (onyx.api/submit-job peer-config
                                                (:onyx-job cmp))]


      ;; use all the values set for the kafka reader for this kafka writer
      ;; (mainly topic)
      (assoc cmp :producer (k/producer input-task)
                 :job-id job-id
                 :dev-env dev-env
                 :peer-group peer-group
                 :peers peers)))
  (stop [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          {:keys [producer
                  job-id
                  dev-env
                  peer-group
                  peers]} cmp]
      ;; stop accepting messages
      (if (:producer producer) (.close (:producer producer)))
      ;; stop doing stuff on peers
      (if job-id (onyx.api/kill-job peer-config job-id))
      ;; stop the peers
      (if peers (onyx.api/shutdown-peers peers))
      ;; stop the peer manager(?)
      (if peer-group (onyx.api/shutdown-peer-group peer-group))
      ;; stop the world
      (if dev-env (onyx.api/shutdown-env dev-env))
      (assoc cmp :producer nil
                 :job-id nil
                 :dev-env nil
                 :peer-group nil
                 :peers nil)))
  p/InputOutput
  ;; returns result from kafka :offset,etc.
  (input [cmp msg]
    ;; reuse info from the reader task we found in (start)
    (let [{:keys [:topic :producer :serializer-fn]} (:producer cmp)]
      (k/produce producer
                 serializer-fn
                 topic
                 msg)))
  (output [cmp stream] ;; provide ms/stream
    ;; redis provided by component's start-system
    (p/subscribe (:redis cmp) (get-in cmp [:producer :topic]) stream)
    cmp))


(defmethod clojure.core/print-method KafkaRedis
  [system ^java.io.Writer writer]
  (.write writer "#<bones.stream.pipelines/KafkaRedis>"))
