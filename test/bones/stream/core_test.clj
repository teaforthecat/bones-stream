(ns bones.stream.core-test
  (:require [bones.conf :as conf]
            [bones.stream
             [core :as stream]
             [jobs :as jobs]]
            [manifold.stream :as ms]
            [bones.stream.protocols :as p]))

(comment
  ;; create global state
(def system (atom {}))

;; glue components together
(stream/build-system system
                     (jobs/bare-job ::my-inc)
                     (conf/map->Conf {:conf-files ["resources/dev-config.edn"]}))

;; start job, connect to redis, kafka
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

;; input message
(p/input (:job @system) {:key "123456"
                         :message {:command "move"
                                   :args ["left"]}})


;; see the output printed to the repl
;; {:topic bones.stream.core-test..my-inc, :partition 0, :key 123456, :message {command move, args [left]}, :offset 0}

)
