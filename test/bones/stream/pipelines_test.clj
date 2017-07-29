(ns bones.stream.pipelines-test
  (:require [bones.conf :as conf]
            [bones.stream.pipelines :as pipelines]
            [clojure.test :refer [deftest testing is are use-fixtures run-tests]]
            [manifold.stream :as ms]
            [com.stuartsierra.component :as component]
            [bones.stream.protocols :as p]
            [bones.stream.serializer] ;; for onyx to resolve fns
            [bones.stream.jobs :as jobs]))


(deftest pipeline-builders
  (testing "builds a valid pipeline from a job"
    (let [job (jobs/series-> {}
                             (jobs/input :kafka)
                             (jobs/output :redis))
          pipe (component/start (pipelines/pipeline job))]
      (are [path value] (= value (get-in pipe path))
        [:input :task-map :onyx/medium] :kafka
        [:output :task-map :onyx/params] [:redis/spec :redis/channel])))

  ;; depends on running kafka, provided by docker-compose
  (testing "starts the input and outputs"
    (let  [topic "test123"
           job (jobs/series-> {}
                              (jobs/input :kafka)
                              (jobs/output :redis {:redis/channel topic}))
           pipe (component/start (pipelines/pipeline job))
           stream (ms/stream)]


      ;; the peers aren't running so nothing will arrive at the output.
      ;; (ms/consume println (p/output pipe stream)) ;; doesn't blow up is good
      (p/output pipe stream) ;; doesn't blow up is good

      ;; fake the other side of the pipeline here because the peers aren't running
      (p/publish (:redi (:output pipe)) topic {:abc 123})
      ;; a valid response
      (= {:abc 123} @(ms/take! stream))
      (let [response (p/input pipe {:key 123 :value "hi" :topic topic})]
        ;; a valid response
        (is (= '(:topic :partition :offset) (keys response)))))
    ))
