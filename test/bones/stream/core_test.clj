(ns bones.stream.core-test
  (:require [bones.conf :as conf]
            [bones.stream
             [core :as stream]
             [jobs :as jobs]
             [protocols :as p]]
            [manifold.stream :as ms]))

(defn my-inc [segment]
  (update-in segment ["message" "args"] conj "up"))

(comment
  ;; create global state
(def system (atom {}))

;; glue components together
(stream/build-system system
                     ;; single-function-job:
                     ;; kafka -> my-inc -> redis
                     (jobs/single-function-job ::my-inc)
                     (conf/map->Conf {:conf-files ["resources/dev-config.edn"]}))

;; start onyx job, connect to redis, kafka
(stream/start system)

;; stop everything
(stream/stop system)

;; a stream to connect output to a function
(def outputter (ms/stream))

;; connect stream to a function
(ms/consume println outputter)

;; subscribe to redis pub/sub channel
;; connect stream to output
(p/output (:job @system) outputter)

;; input message to kafka
(p/input (:job @system) {:key "123456"
                         :message {:command "move"
                                   :args ["left"]}})


;; see the output printed to the repl
;; {:topic bones.stream.core-test..my-inc, :partition 0, :key 123456, :message {:command move, :args [left up]}, :offset 0}

)
