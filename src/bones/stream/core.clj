(ns bones.stream.core
  (:require [bones.stream
             [jobs :as jobs]
             [pipelines :as pipelines]
             [redis :as redis]]
            [com.stuartsierra.component :as component]))

;; this is common to all components
(defn- start-systems [system & components]
  (swap! system component/update-system components component/start))

;; this is common to all components
(defn- stop-systems [system & components]
  (swap! system component/update-system-reverse components component/stop))

(defn build-system [sys onyx-job config]
  ;; sets the vars that are used by onyx plugins
  ;; config here is a map. If it is a component it will not be started at this point.
  (jobs/serialization-format (get-in config [:stream :serialization-format] :msgpack))
  (swap! sys #(-> %
                  (assoc :conf config)
                  (assoc :redis (component/using (redis/map->Redis {}) [:conf]))
                  (assoc :job (component/using (pipelines/map->KafkaRedis {:onyx-job onyx-job}) [:conf :redis])))))

(defn assoc-job [sys job]
  (swap! sys assoc-in [:job :onyx-job] job))

(defn update-job [sys update-fn]
  (swap! sys update-in [:job :onyx-job] update-fn))

(defn get-job [sys]
  (get-in sys [:job :onyx-job]))

(defn start [sys]
  (start-systems sys :job :redis :conf))

(defn stop [sys]
  (stop-systems sys :job :redis))


(comment

  (def system (atom {}))
  (build-system system {})

  (start system)

  )
