(ns bones.stream.jobs
  (:require ;; remove these if possible
            [bones.stream.redis :as redis]
            [bones.stream.kafka :as k]
            ;; maybe put this somewhere else:
            [onyx.plugin.core-async]
            ))


;; Task functions
;; these function merge sensible defaults with configuration data
;; all values can be overridden by configured builders below

(comment

  (def  ser-format :msgpack)
  (def keymkr #(->> %2 name (str %1) (keyword "bones.stream.serializer")))
  )

(defn serfun [frmt]
  ;; input :msgpack -> [:bones.stream.serializer/en-msgpack :bones.stream.serializer/de-msgpack ]
  (map #(->> %2 name (str %1) (keyword "bones.stream.serializer"))
       ["en-" "de-"]
       [frmt frmt]))

(defn kafka-input-task [conf]
  ;; serialization format is a shortcut offered to reduce configuration
  ;; to override set :kafka/deserializer-fn and :kafka/serializer-fn
  ;; the default is msgpack
  (let [ser-format (get conf :serialization-format :msgpack) ;; one of :msgpack,:json
        [sfn-kw dfn-kw] (serfun ser-format)]
    (with-meta
      (merge {:onyx/name :bones/input
              :onyx/type :input
              ;; preprocessor fn
              :onyx/fn (->> ser-format name (str "fix-key-") (keyword "bones.stream.kafka"))
              :onyx/medium :kafka
              :onyx/plugin :onyx.plugin.kafka/read-messages
              :onyx/max-peers 1 ;; for read exactly once
              :onyx/batch-size 1
              :kafka/zookeeper "localhost:2181"
              ;; :kafka/topic "default-topic"
              :kafka/deserializer-fn :bones.stream.serializer/de-msgpack
              :kafka/offset-reset :latest
              :kafka/wrap-with-metadata? true
              }
             (dissoc conf :kafka/serializer-fn :serialization-format))
      ;; meta is used by the pipeline to build an input function
      {:bones/service :kafka
       ;; work around. can't set this on the input task, onyx will complain :(
       :kafka/serializer-fn (or (:kafka/serializer-fn conf) sfn-kw)})))

(defn redis-output-task [conf]
  (with-meta
    (merge {:onyx/name :bones/output
            :onyx/type :output
            :onyx/fn :bones.stream.redis/redis-write
            :onyx/medium :function
            :onyx/plugin :onyx.peer.function/function
            ;; the SECOND param sent to bones.stream.redis/redis-write
            ;; :redis/channel "default-topic"
            :redis/spec {:host "127.0.0.1" :port 6379}
            :onyx/params [:redis/spec :redis/channel]
            :onyx/batch-size 1}
           conf)
    {:bones/service :redis}))

(defn dummy-output-task [conf]
  (merge conf {:onyx/name :bones/output
               :onyx/type :output
               :onyx/batch-size 1}
         conf))

(defn fn-task [conf]
  (merge {:onyx/name :default
          :onyx/type :function
          :onyx/batch-size 1
          :onyx/fn :default}
         conf))

(def kafka-lifecycle
  {:lifecycle/task :bones/input
   :lifecycle/calls :onyx.plugin.kafka/read-messages-calls})

;; Helper functions

(defn sym-to-topic
  "generate a kafka-acceptable topic name
  (sym-to-topic :a.b/c) => \"a.b..c\" "
  [^clojure.lang.Keyword job-sym]
  (-> (str job-sym)
      (clojure.string/replace "/" "..") ;; / is illegal in kafka topic name
      (subs 1))) ;; remove leading colon

(defn append-task [workflow task]
  (let [remaining-workflow (vec (butlast workflow))
        [last1 last2] (last workflow)
        second-to-last (remove nil? [last1 (or last2 task)])
        new-last       (remove nil? [last2 (if last2 task)])]
    ;; pity we need to turn all back into vectors
    (mapv vec (distinct (remove empty? (conj remaining-workflow
                                             second-to-last
                                             new-last))))))

;; Task Builders
;; these functions accept a configuration map and
;; return a function that will use the map to add a task to a job
;; the returned function is considered a "configured builder"
;; most will support two ways to add configuration, the first arg, and the third arg
;; having both allows for expressive freedom, which is nice depending on whether
;; there is a lot of configuration data or just a little

(defmulti input (fn [conf x & _] x))

(defmethod input :kafka
  ([conf _]
   (input conf :kafka {}))
  ([conf _ opts]
   (fn [job]
     (let [task-conf (merge {:kafka/topic "bones-input"}
                            (:bones/input conf)
                            opts)]
       (-> job
           (update :workflow conj [:bones/input])
           (update :catalog conj (kafka-input-task task-conf))
           (update :lifecycles conj kafka-lifecycle)
           ;; this could happen anywhere, not sure if it should be able to be overridden
           ;; it can be updated after the job data structure is built
           (assoc :task-scheduler :onyx.task-scheduler/balanced))))))

(defmulti output (fn [conf x & _] x))

(defmethod output :redis
  ([conf _]
   (output conf :redis {}))
  ([conf _ opts]
   (fn [job]
     (let [task-conf (merge {:redis/channel "bones-output"}
                            (:bones/output conf)
                            opts)]
       (-> job
           (update :workflow append-task :bones/output)
           (update :catalog conj (redis-output-task task-conf)))))))

(defmethod output :dummy
  ([conf _]
   (output conf :dummy {}))
  ([conf _ opts]
   (fn [job]
     (let [task-conf (merge opts (:bones/output conf))]
       (-> job
           (update :workflow append-task :bones/output)
           (update :catalog conj (dummy-output-task task-conf)))))))

(defn function
  "arg ns-fn should be a namespaced keyword of a function that takes a segment,
   or anything; in which case :onyx/fn will need to be the \"ns-fn\""
  ([conf ns-fn]
   (function conf ns-fn {}))
  ([conf ns-fn opts]
   (fn [job]
     (let [task-conf (-> opts
                         (update :onyx/name #(or % ns-fn))
                         (update :onyx/fn   #(or % ns-fn)))]
       (-> job
           (update :workflow append-task (:onyx/name task-conf))
           (update :catalog conj (fn-task task-conf))
           ;; no lifecycles here
           )))))

;; Job Builders (only one so far)
;; This macro is a helper to build a job. it is not required.
;; It provides a double threading technique where the first pass
;; puts together "configured builders" and the second pass
;; threads a job through them to complete a job build


(defmacro series->
  " Builds a simple job where the segment will be sent through the tasks in a simple series.

  - thread conf through the builders
  - then thread the job through the returned fn
  - works like the -> macro splice in the `conf' like the -> macro, but also
  call the returned functions with a job. This may be a horrible idea, but it
  looks cool right now :) "
  [conf & tasks]
  (assert (<= 2 (count tasks)) "at minimum an input and an output is required")
  (let [cfn (fn [x] `(~(first x) ~conf ~@(rest x)))
        forms (map cfn tasks)]
    `((comp ~@(reverse forms)) {:workflow []
                                :catalog []
                                :lifecycles []})))
