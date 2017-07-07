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

(defn bare-workflow []
  [[:bones/input :bones/output]])

(defn input-task [topic conf]
  (merge {:onyx/name :bones/input
          :onyx/type :input
          :onyx/fn ::k/fix-key ;; preprocessor
          :onyx/medium :kafka
          :onyx/plugin :onyx.plugin.kafka/read-messages
          :onyx/max-peers 1 ;; for read exactly once
          :onyx/batch-size 1
          :kafka/zookeeper "localhost:2181"
          :kafka/topic topic
          :kafka/deserializer-fn ::unserfun
          :kafka/offset-reset :latest
          :kafka/wrap-with-metadata? true
          }
         conf))

(defn output-task [topic conf]
  (merge {:onyx/name :bones/output
          :onyx/type :output
          :onyx/fn ::redis/redis-write
          :onyx/medium :function
          :onyx/plugin :onyx.peer.function/function
          ;; the SECOND param sent to ::redis-write
          ::channel topic
          :onyx/params [::channel]
          :onyx/batch-size 1}
         conf))

(defn bare-catalog [fn-sym topic]
  [(input-task topic {})
   (output-task topic {})])

(def kafka-lifecycle
  {:lifecycle/task :bones/input
   :lifecycle/calls :onyx.plugin.kafka/read-messages-calls})

;; FIXME: dup
(defn bare-lifecycles [fn-sym]
  [{:lifecycle/task :bones/input
    :lifecycle/calls :onyx.plugin.kafka/read-messages-calls}])


(defn channel-input [channel]
  {:onyx/name :bones/input
   :onyx/plugin :onyx.plugin.core-async/input
   :onyx/type :input
   :onyx/medium :core.async
   :onyx/batch-size 1
   :onyx/max-peers 1
   :onyx/doc "Reads segments from a core.async channel"
   :core.async/chan channel})

(defn channel-output [channel]
  {:onyx/name :bones/output
   :onyx/plugin :onyx.plugin.core-async/output
   :onyx/type :output
   :onyx/medium :core.async
   :onyx/batch-size 1
   :onyx/max-peers 1
   :onyx/doc "Reads segments from a core.async channel"
   :core.async/chan channel})

(defn channel-lifecycle [task-name]
  {:lifecycle/task task-name
   :lifecycle/calls :onyx.plugin.core-async/reader-calls})

#_(defn channel-lifecycle []
  [{:lifecycle/task :bones/input
    :lifecycle/calls ::channel-setup}
   {:lifecycle/task :bones/input
    :lifecycle/calls :onyx.plugin.core-async/reader-calls}])

(defn sym-to-topic
  "generate a kafka-acceptable topic name
  (sym-to-topic :a.b/c) => \"a.b..c\" "
  [^clojure.lang.Keyword job-sym]
  (-> (str job-sym)
      (clojure.string/replace "/" "..") ;; / is illegal in kafka topic name
      (subs 1))) ;; remove leading colon

(defn bare-job
  "bare minimum"
  ([fn-sym]
   (bare-job fn-sym (sym-to-topic fn-sym)))
  ([fn-sym topic]
   {:workflow (bare-workflow)
    :catalog (bare-catalog fn-sym topic)
    :lifecycles (bare-lifecycles fn-sym)
    :task-scheduler :onyx.task-scheduler/balanced}))

(defn add-fn-task [catalog fn-sym]
  (conj catalog
        {:onyx/name fn-sym
         :onyx/type :function
         :onyx/batch-size 1
         :onyx/fn fn-sym}))

(defn single-function-workflow [fn-sym]
  [[:bones/input fn-sym]
   [fn-sym :bones/output]] )

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
  [conf _]
  (fn [job]
    (-> job
        (update :workflow conj [:bones/input])
        ;;FIXME nil to keep signature of input-task for now, topic should be :kafka/topic in the conf map
        (update :catalog conj (input-task nil conf))
        (update :lifecycles conj kafka-lifecycle)
        ;; this could happen anywhere, not sure if it should be able to be overridden
        (assoc :task-scheduler :onyx.task-scheduler/balanced))))

(defmethod input :channel
  [conf _ {:keys [channel]}]
  ;; core.async channel required in options here
  (fn [job]
    (-> job
        (update :workflow conj [:bones/input])
        (update :catalog conj (channel-input channel))
        (update :lifecycles conj (channel-lifecycle :bones/input))
        )))

(defmulti output (fn [conf x & _] x))

(defmethod output :redis
  [conf _]
  (fn [job]
    (-> job
        (update :workflow intersperse-task :bones/output)
        ;;FIXME nil to keep signature of input-task for now, topic should be :kafka/topic in the conf map
        (update :catalog conj (output-task nil conf))
        ;; no lifecycles on this one
        )))

(defmethod output :channel
  [conf _ {:keys [channel]}]
  (fn [job]
    (-> job
        (update :workflow intersperse-task :bones/output)
        (update :catalog conj (channel-output channel))
        (update :lifecycles conj (channel-lifecycle :bones/output)))))

(defn function [ns-fn]
  ;; namespaced function
  (fn [job]
    (-> job
        (update :workflow intersperse-task ns-fn)
        (update :catalog add-fn-task ns-fn)
        ;; no lifecycles here
        )))

(defmacro in-series
  "(series conf
           (b/input :kafka)
           (b/function ::my-inc)
           (b/function ::my-other-inc)
           (b/output :redis))"
  [conf input & tasks]
  (let [output (last tasks)
        middle (butlast tasks)]
    `(let [input-fn# (-> ~conf ~input)
           output-fn# (-> ~conf ~output)
           middle-fn# (comp ~@(reverse middle))]
       (-> {:workflow []}
           input-fn#
           output-fn#
           middle-fn#))))

(macroexpand-1  '(in-series {} :a :b :d :c))

(comment
  (macroexpand-1
   ' (in-series {}
              (input :kafka)
              (function ::my-inc)
              (function ::my-other-inc)
              (output :redis)))

  (  (input {} :kafka) {})

  ((comp (function ::my-inc) (function ::my-other-inc)) {})

  (->- :a :b :c)
  (series [] [:a :b :c])

  (intersperse-task [[:a :b] [:c :d]] :e)
  (intersperse-task [[:a :b]] :e)
  (intersperse-task [[:a]] :e)
  (intersperse-task [] :e)

  (reduce intersperse-task [[:a :b]] [:c :d :e])
  ;;=> [[:a :c] [:c :d] [:d :e] [:e :b]]

  (macroexpand-1 '(in-series conf
                        (b/input :kafka)
                        (b/function ::my-inc)
                        (b/function ::my-other-inc)
                        (b/output :redis)))


  )


(defn single-function-job
  "bare minimum plus one function"
  ([fn-sym]
   (single-function-job fn-sym (sym-to-topic fn-sym)))
  ([fn-sym topic]
   {:workflow (single-function-workflow fn-sym)
    :catalog (add-fn-task (bare-catalog fn-sym topic) fn-sym)
    :lifecycles (bare-lifecycles fn-sym)
    :task-scheduler :onyx.task-scheduler/balanced}))

