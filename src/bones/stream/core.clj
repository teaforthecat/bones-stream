(ns bones.stream.core
  (:require [bones.stream
             [jobs :as jobs]
             [pipelines :as pipelines]
             [peer-group :as peer-group]
             [redis :as redis]]
            [com.stuartsierra.component :as component]))


;; this is common to all components
(defn- start-systems [system & components]
  (swap! system component/update-system components component/start))

;; this is common to all components
(defn- stop-systems [system & components]
  (swap! system component/update-system-reverse components component/stop))

(defn build-system [sys config]
  ;; sets the vars that are used by onyx plugins
  ;; config here is a map. If it is a component it will not be started at this point.
  (jobs/serialization-format (get-in config [:stream :serialization-format] :msgpack))
  (swap! sys #(-> %
                  (assoc :conf config)
                  (assoc :peer-group (component/using (peer-group/map->Peers {}) [:conf])))))

(defn start [sys]
  (start-systems sys :peer-group :conf))

(defn stop [sys]
  (stop-systems sys :peer-group))

(defn submit-job [sys job]
  (peer-group/submit-job (:peer-group @sys) job))

(defn kill-jobs [sys]
  (peer-group/kill-jobs (:peer-group @sys)))
(comment

  (def system (atom {}))
  (build-system system {})

  (start system)

  )
