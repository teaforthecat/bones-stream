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

(defrecord Redis [conf spec channel-prefix]
  component/Lifecycle
  ;; idempontent start/stop not needed
  (start [cmp]
    (let [config (get-in cmp [:conf :stream :redis])
          {:keys [spec channel-prefix]
           ;; Default spec: {:host \"127.0.0.1\" :port 6379}
           :or {spec {}
                ; this must be matched by whatever is writing on the backend
                channel-prefix "bones-"}} config]
      (-> cmp
          (assoc :spec spec)
          (assoc :channel-prefix channel-prefix))))
  (stop [cmp]
    ;; is there a better way to do this all at once?
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
    (let [{:keys [spec]} cmp
          result (d/deferred)]
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
    (let [{:keys [spec]} cmp
          result (d/deferred)]
      ;; todo error handling
      (d/success! result
                  {:key rkey
                   :value (car/wcar {:spec spec}
                                    (car/get rkey))})
      result))
  (fetch-keys [cmp topic]
    (let [{:keys [spec]} cmp
          result (d/deferred)]
      (d/success! result
                  (car/wcar {:spec spec}
                            (car/smembers topic)))
      result))
  (fetch-all [cmp topic]
    (let [{:keys [spec]} cmp
          rkeys @(p/fetch-keys cmp topic)]
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
