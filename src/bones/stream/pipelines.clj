(ns bones.stream.pipelines
  (:require [com.stuartsierra.component :as component]
            [bones.stream.protocols :as p]
            [bones.stream.kafka :as k]
            [onyx.api]
            [onyx.static.util :refer [kw->fn]]
            [bones.stream.redis :as redis]))

(defrecord KafkaInput [task-map]
  component/Lifecycle
  (start [cmp]
    (let [writer (k/writer task-map)]
      (assoc cmp
             :producer (:producer writer)
             :kafka/topic (:kafka/topic task-map)
             ;; work around. can't set this on the input task, onyx will complain :(
             :kafka/serializer (kw->fn (:kafka/serializer-fn (meta task-map))))))
  (stop [cmp]
    (.close (:producer cmp))
    (assoc cmp
           :producer nil))
  p/Input
  (input [cmp msg]
    (k/produce (:producer cmp)
               (:kafka/topic cmp)
               (:kafka/serializer cmp)
               msg)))

(defrecord RedisOutput [task-map redi]
  component/Lifecycle
  (start [cmp] cmp)
  (stop [cmp]
    (.stop redi)
    cmp)
  p/Output
  (output [cmp stream]
    ;; redi is a instance of redis/Redis
    (p/subscribe redi
                 (:redis/channel task-map)
                 stream)
    stream))

(defrecord Pipeline [input output]
  component/Lifecycle
  (start [cmp]
    ;; workaround component's start-system because we don't know what the
    ;; keys(service) will be in the system map (because input/output defmethods
    ;; can be extended)
    (assoc cmp
           :input (component/start input)
           :output (component/start output)))
  (stop [cmp]
    (assoc cmp
           :input (component/stop input)
           :output (component/stop output)))
  p/Input
  (input [cmp msg]
    (p/input (:input cmp) msg))
  p/Output
  (output [cmp stream]
    (p/output (:output cmp) stream)
    stream))

(defmethod clojure.core/print-method Pipeline
  [system ^java.io.Writer writer]
  (.write writer "#<bones.stream.pipelines/Pipeline>"))


(defmulti input (fn [service task-map] service))

(defmethod input :kafka
  [_ task-map]
  (map->KafkaInput {:task-map task-map}))

(defmulti output (fn [service task-map] service))

(defmethod output :redis
  [_ task-map]
  (map->RedisOutput {:task-map task-map
                     :redi (redis/map->Redis {:spec (:redis/spec task-map)})}))


(defn pipeline [job]
  (let [findr (fn [x ys] (first (filter #(= x (:onyx/name %)) ys)))
        input-task-map  (findr :bones/input  (:catalog job))
        output-task-map (findr :bones/output (:catalog job))
        input-service  (get (meta input-task-map)  :bones/service)
        output-service (get (meta output-task-map) :bones/service)]
    ;; this component will start the input and outputs along with it
    (map->Pipeline {:input (input  input-service  input-task-map)
                    :output (output output-service output-task-map)})))
