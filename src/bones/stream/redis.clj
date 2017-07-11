(ns bones.stream.redis
  (:require [com.stuartsierra.component :as component]
            [manifold
             [deferred :as d]
             [stream :as ms]]
            [taoensso
             [carmine :as car]
             [timbre :refer [debug]]]
            [bones.stream.protocols :as p]))

(defn message-handler
  "returns a function that destructures a redis message and sends the good stuff
  to the stream"
  [stream]
  (fn [[type channel message]]
    (if (= type "message")
      (ms/put! stream message))))

(def listeners (atom []))

(defrecord Redis [spec]
  component/Lifecycle
  ;; we don't need to maintain a connection here because carmine maintains a pool
  ;; of connections, but we do maintain the subscriber streams
  (stop [cmp]
    (map car/close-listener @listeners)
    (reset! listeners [])
    cmp)

  p/Publish
  (publish [cmp channel message]
    (car/wcar {:spec spec} ;; use default pool option
              (car/publish channel message)))

  p/Subscribe
  (subscribe [cmp channel stream]
    ; one user/browser connection (through local pool)
    (let [listener
          (car/with-new-pubsub-listener {:spec spec}
            {channel (message-handler stream)}
            (car/subscribe channel))]
      (swap! listeners conj listener)
      listener))

  p/MaterializedView
  (write [cmp topic rkey value]
    (let [result (d/deferred)]
      (d/success! result
        (car/wcar {:spec spec}
                  (if (nil? value)
                    (do
                      (car/srem topic rkey)
                      (car/del rkey))
                    (do
                      (car/set rkey value)
                      (car/sadd topic rkey)))))
      result))
  (fetch [cmp rkey]
    (let [result (d/deferred)]
      ;; todo error handling
      (d/success! result
                  {:key rkey
                   :value (car/wcar {:spec spec}
                                    (car/get rkey))})
      result))
  (fetch-keys [cmp topic]
    (let [result (d/deferred)]
      (d/success! result
                  (car/wcar {:spec spec}
                            (car/smembers topic)))
      result))
  (fetch-all [cmp topic]
    (let [rkeys @(p/fetch-keys cmp topic)]
      ;; not the most performant thing to do here;
      ;; this probably deserves a lua script installed to redis for performance
      (ms/reduce conj
                 []
                 (ms/transform
                  (map deref)
                  (ms/transform
                   (map (partial p/fetch cmp))
                   rkeys))))))

(defmethod clojure.core/print-method Redis
  [system ^java.io.Writer writer]
  (.write writer "#<bones.stream.redis/Redis>"))

(defn redis-write [redi channel message]
  (debug "redis-write: " channel " " message )
  (let [k (:key message)
        v (:message message)]
    (p/write redi channel k v)
    (p/publish redi channel message)))
