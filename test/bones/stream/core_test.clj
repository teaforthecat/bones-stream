(ns bones.stream.core-test
  (:require #_[onyx-local-rt.api :as api] ;; not yet
            [bones.stream.core :as stream]
            [bones.stream.kafka :as k] ;; needed for finding functions
            [bones.conf :as conf]
            [clojure.test :refer [deftest is testing]]
            [manifold.stream :as ms]))


(comment
  ;; create global state
(def system (atom {}))

;; glue components together
(stream/build-system system
                     (k/bare-job ::my-inc)
                     (conf/map->Conf {:conf-files ["resources/dev-config.edn"]}))

;; attach job
;; (swap! system assoc-in [:job :onyx-job] (k/bare-job ::my-inc))

;; start job, connect to redis, kafka
(stream/start system)

(stream/stop system)

;; test
(get-in @system [:conf :stream :peer-config])

;; test
(get-in @system [:job :producer :producer])

(def outputter (ms/stream))

;; listen for message
(ms/consume println outputter)

(k/output (:job @system) outputter)

;; input message
(k/input (:job @system) {:key "123456"
                         :message {:command "move"
                                   :args ["left"]}})


)

(defn my-inc [segment]
  (update-in segment [:n] inc))

(def test-state (atom nil))


(defn update-atom! [event window trigger {:keys [lower-bound upper-bound event-type] :as state-event} extent-state]
  (reset! test-state extent-state))

(def job
  {:workflow [[:in :inc] [:inc :out]]
   :catalog [{:onyx/name :in
              :onyx/type :input
              :onyx/batch-size 20}
             {:onyx/name :inc
              :onyx/type :function
              :onyx/fn ::my-inc
              :onyx/batch-size 20}
             {:onyx/name :out
              :onyx/type :output
              :onyx/batch-size 20}]
   :windows [{:window/id :collect-segments
              :window/task :inc
              :window/type :global
              :window/aggregation :onyx.windowing.aggregation/conj}]
   :triggers [{:trigger/window-id :collect-segments
               :trigger/refinement :onyx.refinements/accumulating
               :trigger/fire-all-extents? true
               :trigger/on :onyx.triggers/segment
               :trigger/id :my-trigger
               :trigger/threshold [1 :elements]
               :trigger/sync ::update-atom!}]
   :lifecycles []})

(deftest run-job-test
  (reset! test-state [])
   (is (= {:next-action :lifecycle/start-task?,
          :tasks {:inc {:inbox []},
                  :out {:inbox [],
                        :outputs [{:n 42} {:n 85}]},
                  :in {:inbox []}}}
         (-> (api/init job)
             (api/new-segment :in {:n 41})
             (api/new-segment :in {:n 84})
             (api/drain)
             (api/stop)
             (api/env-summary))))
   (is (= @test-state [{:n 42} {:n 85}])))
