(ns bones.stream.core
  (:require [manifold.stream :as ms]
            [bones.stream.kafka :as kafka]
            [bones.stream.redis :as redis]
            [com.stuartsierra.component :as component]
            [onyx.api :as onyx]
            [onyx.messaging.aeron.embedded-media-driver :as md]
            [manifold.bus :as b]))

;; this is common to all components
(defn- start-systems [system & components]
  (swap! system component/update-system components component/start))

;; this is common to all components
(defn- stop-systems [system & components]
  (swap! system component/update-system-reverse components component/stop))

;; dev only
(defrecord PeerEnv []
  component/Lifecycle
  (start [cmp]
    (let [env-config (get-in cmp [:conf :stream :env-config])]
      (assoc cmp :peer-env (onyx/start-env env-config))))
  (stop [cmp]
    (update cmp :peer-env component/stop)))

(defrecord Aeron []
  component/Lifecycle
  (start [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          md-peer-config (assoc peer-config :onyx.messaging.aeron/embedded-driver? true)]
      (assoc cmp :aeron (component/start (md/new-embedded-media-driver md-peer-config)))))
  (stop [cmp]
    (update cmp :aeron component/stop)))

(defrecord PeerGroup []
  component/Lifecycle
  (start [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])]
      (assoc cmp :peer-group (onyx/start-peer-group peer-config))))
  (stop [cmp]
    (update cmp :peer-group component/stop)))

(defrecord Peers [peer-group conf]
  component/Lifecycle
  (start [cmp]
    (let [{:keys [env-config peer-config]} (get-in cmp [:conf :stream])
          ;; peer-group should be started
          peer-group (get-in cmp [:peer-group :peer-group])
          ;; TODO: validate: n-peers must be greater than 3
          n-peers (get-in cmp [:conf :stream :n-peers] 3)]
      (assoc cmp :peers (onyx/start-peers n-peers peer-group))))
  (stop [cmp]
    (doseq [v-peer (:peers cmp)]
      (onyx/shutdown-peer v-peer))
    (assoc cmp :peers nil)))

(defn build-system [sys config]
  ;; sets the vars that are used by onyx plugins
  (kafka/serialization-format (get-in config [:conf :stream :serialization-format]))
  (swap! sys #(-> %
                  (assoc :conf config)
                  (assoc :aeron (component/using (map->Aeron {}) [:conf]))
                  (assoc :peer-group (component/using (map->PeerGroup {}) [:conf]))
                  (assoc :peers (component/using (map->Peers {}) [:conf :peer-group]))
                  (assoc :redis (component/using (redis/map->Redis {}) [:conf]))
                  (assoc :job (component/using (kafka/map->Job {}) [:conf :redis])))))


(defn start-aeron [sys]
  (start-systems sys :aeron :conf))

(defn stop-aeron [sys]
  (stop-systems sys :aeron))

(defn start-peer-group [sys]
  (start-systems sys :peer-group :conf))

(defn stop-peer-group [sys]
  (stop-systems sys :peer-group))

(defn start-peers [sys]
  (start-systems sys :peers :conf))

(defn stop-peers [sys]
  (stop-systems sys :peers))

(defn start [sys]
  (start-systems sys :job :redis :conf))

(defn stop [sys]
  (stop-systems sys :job :redis))


(comment

  (def system (atom {}))
  (build-system system {})

  (start-peers system)

  (start system)

  )
