(ns bones.stream.core-test
  (:gen-class)
  (:require [clojure.test :refer [deftest testing is are use-fixtures run-tests]]
            [bones.conf :as conf]
            [bones.stream
             [core :as stream]
             [serializer] ;; for onyx to resolve fns
             [peer-group :as peer-group]
             [jobs :as jobs]
             [protocols :as p]]
            [manifold.stream :as ms]
            [com.stuartsierra.component :as component]))

;; silly example function
(defn my-inc [segment]
  ;; has keys :topic, :partition, :offset, :key, :message
  (update-in segment [:message :args] conj "up"))

(def n-messages 3)

;; create global state
(def system (atom {}))

(defn build-system [ftest & addt]
  (stream/build-system system
                       (conf/map->Conf {:conf-files (into ["resources/dev-config.edn"]
                                                          addt)}))
  (ftest))

(use-fixtures :each build-system)

(deftest main-usage

  (let [;build onyx job
        job (jobs/series-> {} ;; empty configuration for simple job with defaults
                           (jobs/input :kafka {:kafka/topic "bones.stream.core-test..my-inc" })
                           (jobs/function ::my-inc)
                           (jobs/output :redis {:redis/channel "bones.stream.core-test..my-inc"}))
        ;; connect to kafka and redis
        pipeline (stream/pipeline job)
        ;; a stream to connect output to a function
        outputter (ms/stream)
        ;; an atom to collect results
        delayed-result (atom [])]

    ;; start onyx peers
    (stream/start system)
    ;; wait for onyx to start because the default is to seek latest offset
    ;; 5 seconds is probably too much (or too little?)
    (Thread/sleep 5000)


    ;; submit-job to start pulling segments from kafka
    (stream/submit-job system job)

    ;; connect stream to a function
    ;; prints to stdout for fun
    (ms/consume println outputter)
    ;; collect results
    (ms/consume (fn [m] (swap! delayed-result conj m)) outputter)

    ;; subscribe to redis pub/sub channel and send it to a stream
    (p/output pipeline outputter)


    ;; idk timing might be why this tests fails in travis-ci (on the first run only)
    (Thread/sleep 1000)

    (time
     (loop [n 0]
       (if (< n n-messages)
         (do
           ;; input message to kafka (to be picked up by the job)
           (p/input pipeline {:key (str "123" n)
                              :value {:command "move"
                                      :args ["left"]}})
           (recur (inc n))))))

    ;; idk wait for async things to happen
    (Thread/sleep 1000)

    (let [result @delayed-result]
      ;; all messages have been received
      (is (= n-messages (count result)))
      ;; key was deserialized
      (is (= "1230" (get-in (first result) [:key])))
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

  ))

(defn -main []
  ;; empty form to be called in lieu of a test for the build-system fixture
  (build-system #() "resources/prod-config.edn")
  (main-usage)
  (System/exit 0))
