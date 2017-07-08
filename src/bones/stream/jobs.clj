(ns bones.stream.jobs
  (:require [bones.stream.serializer :as serializer]
            [bones.stream.redis :as redis]
            [bones.stream.kafka :as k]
            [onyx.plugin.core-async]))



;; not sure if this will work
;; where/when to call this? user only?
;; 1. the ::serfun needs to resolve
;; 2. they need to match
;; 3. provide :kafka/serializer-fn to override
(defn serialization-format [fmt]
  {:pre (some #{fmt} #{:json :json-verbose :msgpack :json-plain})}
  (def serfun (serializer/encoder fmt))
  (def unserfun (serializer/decoder fmt)))

(defn kafka-input-task [conf]
  (merge {:onyx/name :bones/input
          :onyx/type :input
          :onyx/fn ::k/fix-key ;; preprocessor
          :onyx/medium :kafka
          :onyx/plugin :onyx.plugin.kafka/read-messages
          :onyx/max-peers 1 ;; for read exactly once
          :onyx/batch-size 1
          :kafka/zookeeper "localhost:2181"
          ;; :kafka/topic "default-topic"
          :kafka/deserializer-fn ::unserfun
          :kafka/offset-reset :latest
          :kafka/wrap-with-metadata? true
          }
         conf))

(defn redis-output-task [conf]
  (merge {:onyx/name :bones/output
          :onyx/type :output
          :onyx/fn ::redis/redis-write
          :onyx/medium :function
          :onyx/plugin :onyx.peer.function/function
          ;; the SECOND param sent to ::redis-write
          ;; :redis/channel "default-topic"
          :onyx/params [:redis/channel]
          :onyx/batch-size 1}
         conf))

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

(defn sym-to-topic
  "generate a kafka-acceptable topic name
  (sym-to-topic :a.b/c) => \"a.b..c\" "
  [^clojure.lang.Keyword job-sym]
  (-> (str job-sym)
      (clojure.string/replace "/" "..") ;; / is illegal in kafka topic name
      (subs 1))) ;; remove leading colon

(defn intersperse-task [catalog task]
  (let [remaining-catalog (vec (butlast catalog))
        [last1 last2] (last catalog)
        second-to-last (remove nil? [last1 task])
        new-last       (remove nil? [(if last2 task) last2])]
    ;; pity we need to turn all back into vectors
    (mapv vec (distinct (remove empty? (conj remaining-catalog
                                             second-to-last
                                             new-last))))))

(defmulti input (fn [conf x & _] x))

(defmethod input :kafka
  ([conf _]
   (input conf :kafka {}))
  ([conf _ opts]
   (fn [job]
     (let [task-conf (merge opts (:bones/input conf))]
       (-> job
           (update :workflow conj [:bones/input])
           ;;FIXME nil to keep signature of input-task for now, topic should be :kafka/topic in the conf map
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
     (let [task-conf (merge opts (:bones/output conf))]
       (-> job
           (update :workflow intersperse-task :bones/output)
           (update :catalog conj (redis-output-task task-conf)))))))

(defmethod output :dummy
  ([conf _]
   (output conf :dummy {}))
  ([conf _ opts]
   (fn [job]
     (let [task-conf (merge opts (:bones/output conf))]
       (-> job
           (update :workflow intersperse-task :bones/output)
           (update :catalog conj (dummy-output-task task-conf)))))))

(defn function
  "arg ns-fn should be a namespaced symbol of a function that takes a segment,
   or anything in which case :onyx/fn will need to be the \"ns-fn\""
  ([ns-fn]
   (function ns-fn {}))
  ([ns-fn opts]
   (fn [job]
     (let [task-conf (-> opts
                         (update :onyx/name #(or % ns-fn))
                         (update :onyx/fn   #(or % ns-fn)))
           ]
       (-> job
           (update :workflow intersperse-task (:onyx/name task-conf))
           (update :catalog conj (fn-task task-conf))
           ;; no lifecycles here
           )))))

(defmacro in-series
  " Rearranges the forms so you can read the order correctly.
    This is a shim or repercussion of how `intersperse-task' works. This way we can have
  anything that mutates a job between the input and output and since the job will require
  an input and an output, we can get way with this I think.
  (in-series conf
           (b/input :kafka)
           (b/function ::my-inc)
           (b/function ::my-other-inc)
           (b/output :redis))"
  [conf input & tasks]
  (assert (< 0 (count tasks)) "at minimum an input and an output is required")
  (let [output (last tasks)
        middle (butlast tasks)]
    `(let [input-fn# (-> ~conf ~input)
           output-fn# (-> ~conf ~output)
           middle-fn# (comp ~@(reverse middle))]
       (-> {:workflow []}
           input-fn#
           output-fn#
           middle-fn#))))
