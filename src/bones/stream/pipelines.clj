(ns bones.stream.pipelines
  (:require [com.stuartsierra.component :as component]
            [bones.stream.protocols :as p]
            [bones.stream.kafka :as k]
            [onyx.api]
            [onyx.static.util :refer [kw->fn]]
            [bones.stream.redis :as redis]))

(defrecord Peers [conf]
  component/Lifecycle
  (start [cmp]
    (let [
          ;; the FIRST parameter sent to ::redis/redis-write
          peer-config (assoc (get-in cmp [:conf :stream :peer-config])
                             :onyx.peer/fn-params {:bones/output [(:redis cmp)]})
          env-config (get-in cmp [:conf :stream :env-config])
          start-env (get-in cmp [:conf :stream :start-env])
          n-peers 3 ;; or greater

          ;; dev-env (if start-env (onyx.api/start-env env-config))
          dev-env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          peers (onyx.api/start-peers n-peers peer-group)

          ]
      (assoc cmp :peer-group peer-group
                 :peers peers
                 :dev-env dev-env
                 )
      )
    )
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
      ;; stop the peer manager
      (if peer-group (onyx.api/shutdown-peer-group peer-group))
      ;; stop the world
      (if dev-env (onyx.api/shutdown-env dev-env))
      (assoc cmp :producer nil
             :job-id nil
             :dev-env nil
             :peer-group nil
             :peers nil))
    ))

;; TODO: make composable
(defrecord KafkaRedis [conf onyx-job redis]
  component/Lifecycle
  (start [cmp]
    ;; look for the :bones/input task to reuse kafka connection info
    (let [input-task  (first (filter #(= :bones/input (:onyx/name %)) (:catalog (:onyx-job cmp))))

          peer-config (get-in cmp [:conf :stream :peer-config])
          ;; submit-job is idempotent if job-id is set(UUID)
          {:keys [job-id]} (onyx.api/submit-job peer-config
                                                (:onyx-job cmp))
          ]

      (k/create-topic input-task)
      (Thread/sleep 100) ;; wait for topic to be created?????

      ;; use all the values set for the kafka reader for this kafka writer
      ;; (mainly topic)
      (assoc cmp :writer (k/writer input-task)
                 :job-id job-id)))
  (stop [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          {:keys [writer
                  job-id
                  ]} cmp]
      ;; stop accepting messages
      (if (:producer writer) (.close (:producer writer)))
      ;; stop doing stuff on peers
      (if job-id (onyx.api/kill-job peer-config job-id))
      (assoc cmp :writer nil
                 :job-id nil)))
  p/InputOutput
  ;; returns result from kafka :offset,etc.
  (input [cmp msg]
    ;; reuse info from the reader task we found in (start)
    (let [{:keys [:task-map :producer]} (:writer cmp)
          {:keys [:kafka/topic :kafka/serializer-fn]} task-map]
      (k/produce producer topic (kw->fn serializer-fn) msg)))
  (output [cmp stream] ;; provide ms/stream
    (let [{:keys [:task-map :producer]} (:writer cmp)
          {:keys [:kafka/topic :kafka/serializer-fn]} task-map]
      ;; redis provided by component's start-system
      (p/subscribe (:redis cmp) topic stream)
      cmp)))


(defmethod clojure.core/print-method KafkaRedis
  [system ^java.io.Writer writer]
  (.write writer "#<bones.stream.pipelines/KafkaRedis>"))
