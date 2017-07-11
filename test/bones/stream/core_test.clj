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
                       (jobs/series-> {}
                                         (jobs/input :kafka {:kafka/topic "bones.stream.core-test..my-inc" })
                                         (jobs/function ::my-inc)
                                         (jobs/output :redis {:redis/channel "bones.stream.core-test..my-inc"}))
                       (conf/map->Conf {:conf-files ["resources/dev-config.edn"]}))

  ;; start onyx job, connect to redis, kafka
  (stream/start system)
  ;; wait for onyx to start because it is configured to seek latest offset
  ;; 5 seconds is probably too much, but it is safe
  (Thread/sleep 5000)


  ;; a stream to connect output to a function
  (def outputter (ms/stream))

  ;; connect stream to a function
  ;; prints to stdout
  (ms/consume println outputter)

  ;; subscribe to redis pub/sub channel and send it to a stream
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

  (when  @(future
            ;; 5 seconds is probably too much, but it is safe
            (Thread/sleep 5000)
            (println (p/fetch-all (get-in @system [:job :redis])
                                  (get-in @system [:job :writer :task-map :kafka/topic])))
            ;; stop everything
            (stream/stop system))
    (System/exit 0))



  ;; see the output printed to the repl (via the "outputter")
  ;; {:topic bones.stream.core-test..my-inc, :partition 0, :key 123456, :message {:command move, :args [left up]}, :offset 0}

  )
