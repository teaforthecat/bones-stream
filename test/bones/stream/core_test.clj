(ns bones.stream.core-test
  (:gen-class)
  (:require [bones.conf :as conf]
            [bones.stream
             [core :as stream]
             [jobs :as jobs]
             [protocols :as p]]
            [manifold.stream :as ms]))

(defn my-inc [segment]
  ;; has keys :topic, :partition, :offset, :key, :message
  (update-in segment [:message :args] conj "up"))

(def n-messages 3)

;; create global state
(def system (atom {}))


(defn -main []
  ;; glue components together
  (stream/build-system system
                       ;; single-function-job:
                       ;; kafka -> my-inc -> redis
                       (jobs/single-function-job ::my-inc)
                       (conf/map->Conf {:conf-files ["resources/dev-config.edn"]}))

  ;; start onyx job, connect to redis, kafka
  (stream/start system)

  ;; a stream to connect output to a function
  (def outputter (ms/stream))

  ;; connect stream to a function
  ;; prints to stdout
  (ms/consume println outputter)

  ;; subscribe to redis pub/sub channel
  ;; connect stream to output
  (p/output (:job @system) outputter)

  (ms/put! outputter "subscription printer is working :)")

  (time
   (loop [n 0]
     (if (< n n-messages)
       (do
         ;; input message to kafka
         (p/input (:job @system) {:key (str "123" n)
                                  :value {:command "move"
                                          :args ["left"]}})
         (recur (inc n))))))

  ;; keep the current thread that prints going
  (when  @(future
            ;; 10 seconds is more than generous for onyx to start processing
            (Thread/sleep 10000)
            ;; stop everything
            (stream/stop system))
    (System/exit 0))



  ;; see the output printed to the repl (via the "outputter")
  ;; {:topic bones.stream.core-test..my-inc, :partition 0, :key 123456, :message {:command move, :args [left up]}, :offset 0}

  )
