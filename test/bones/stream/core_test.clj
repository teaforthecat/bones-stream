(ns bones.stream.core-test
  (:gen-class)
  (:require [clojure.test :refer [deftest testing is are use-fixtures run-tests]]
            [bones.conf :as conf]
            [bones.stream
             [core :as stream]
             [peer-group :as peer-group]
             [jobs :as jobs]
             [protocols :as p]]
            [manifold.stream :as ms]
            [com.stuartsierra.component :as component]))

(defn my-inc [segment]
  ;; has keys :topic, :partition, :offset, :key, :message
  (update-in segment [:message :args] conj "up"))

(def n-messages 3)

;; create global state
(def system (atom {}))

(deftest main-usage

  (let [job (jobs/series-> {}
                           (jobs/input :kafka {:kafka/topic "bones.stream.core-test..my-inc" })
                           (jobs/function ::my-inc)
                           (jobs/output :redis {:redis/channel "bones.stream.core-test..my-inc"}))
        pipeline (stream/pipeline job)]

    (stream/build-system system
                         (conf/map->Conf {:conf-files ["resources/dev-config.edn"]}))

    ;; start onyx peers, connect to redis, kafka
    (stream/start system)
    ;; wait for onyx to start because it is configured to seek latest offset
    ;; 5 seconds is probably too much, but it is safe
    ;; peers need to be running in order for submit-job to succeed
    (Thread/sleep 5000)


    ;; submit-job to start pulling segments from kafka
    (stream/submit-job system job)


    ;; a stream to connect output to a function
    (def outputter (ms/stream))
    (def delayed-result (atom []))

    ;; connect stream to a function
    ;; prints to stdout
    ;; (ms/consume println outputter)
    (ms/consume (fn [m] (swap! delayed-result conj m)) outputter)

    ;; subscribe to redis pub/sub channel and send it to a stream
    (p/output pipeline outputter)


    (time
     (loop [n 0]
       (if (< n n-messages)
         (do
           ;; input message to kafka
           (p/input pipeline {:key (str "123" n)
                              :value {:command "move"
                                      :args ["left"]}})
           (recur (inc n))))))

    ;; way to much time to process
    (Thread/sleep 5000)

    (let [result @delayed-result]
      ;; all messages have been received
      (is (= n-messages (count result)))
      ;; they passed through the ::my-inc function
      (is (= ["left" "up"] (get-in (first result) [:message :args] ))))
    ;; close redis subscriber and kafka publisher
    (component/stop pipeline)
    ;; stop processing
    (stream/kill-jobs system)
    ;; stop everything
    (stream/stop system)


  ;; see the output printed to the repl (via the "outputter") with (ms/consume println outputter)
  ;; {:topic bones.stream.core-test..my-inc, :partition 0, :key 123456, :message {:command move, :args [left up]}, :offset 0}

  )
