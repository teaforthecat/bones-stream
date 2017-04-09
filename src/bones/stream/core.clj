(ns bones.stream.core
  (:require [manifold.stream :as ms]
            [bones.stream.kafka :as kafka]
            [bones.stream.redis :as redis]
            [com.stuartsierra.component :as component]
            [manifold.bus :as b]))

;; this is common to all components
(defn- start-systems [system & components]
  (swap! system component/update-system components component/start))

;; this is common to all components
(defn- stop-systems [system & components]
  (swap! system component/update-system-reverse components component/stop))

(defn build-system [sys config]
  (swap! sys #(-> %
                  (assoc :conf config)
                  (assoc :producer (component/using (kafka/map->Producer {}) [:conf]))
                  (assoc :consumer (component/using (kafka/map->Consumer {}) [:conf]))
                  (assoc :redis (component/using (redis/map->Redis {}) [:conf])))))

(defn start [sys]
  (start-systems sys :producer :consumer :redis :conf))

(defn stop [sys]
  (stop-systems sys :producer :consumer :redis))
