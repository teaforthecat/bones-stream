(ns bones.stream.peer-group
  (:require [com.stuartsierra.component :as component]
            [bones.stream.protocols :as p]
            [bones.stream.kafka :as k]
            [onyx.api]
            [onyx.static.util :refer [kw->fn]]
            [bones.stream.redis :as redis]))

(def default-env-config {:onyx/tenancy-id "dev"
                         ;; Assuming it is best to run a bookkeeper server on each node because that
                         ;; is what the onyx-jepson tests do.
                         ;; This will be in-process.
                         ;; Deploying a bookkeeper cluster is an advanced topic for the user to consider.
                         :onyx.bookkeeper/server? true
                         :onyx.bookkeeper/local-quorum? true
                         ;; :onyx.bookkeeper/delete-server-data? #profile{:test true :default false}
                         :onyx.bookkeeper/delete-server-data? true
                         :onyx.bookkeeper/local-quorum-ports [3196 3197 3198]
                         :onyx.bookkeeper/port 3196
                         :zookeeper/address "127.0.0.1:2181"
                         ;; :zookeeper/server? #profile{:test true :default false}
                         :zookeeper/server? false
                         :zookeeper.server/port 2181})

(def default-peer-config {:onyx/tenancy-id "dev"
                          :zookeeper/address "127.0.0.1:2181"
                          :onyx.peer/job-scheduler :onyx.job-scheduler/balanced
                          :onyx.peer/zookeeper-timeout 60000
                          :onyx.messaging/impl :aeron
                          :onyx.messaging/bind-addr "localhost"
                          :onyx.messaging/peer-port 40200
                          ;;starts aeron messanger, which runs separately normally
                          ;; :onyx.messaging.aeron/embedded-driver? #profile{:test true :default false}}
                          :onyx.messaging.aeron/embedded-driver? true})

;; Two pass start/stop
;; start:
;;   - start peers
;;   - submit-job
;; stop:
;;   - kill-jobs
;;   - stop peers

(defprotocol JobManager
  (submit-job [cmp job])
  (list-jobs [cmp])
  (kill-job [cmp job-id])
  (kill-jobs [cmp]))

(defrecord Peers [conf]
  component/Lifecycle
  (start [cmp]
    (let [
          peer-config (merge default-peer-config (get-in cmp [:conf :stream :peer-config]))
          env-config (merge default-env-config (get-in cmp [:conf :stream :env-config]))
          n-peers 3 ;; or greater

          ;; windowing-env runs(or connects to) a bookeeper server to coordinate
          ;; with other peer groups(processes)
          windowing-env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          peers (onyx.api/start-peers n-peers peer-group)
          ]
      (-> cmp
          (assoc-in [:conf :stream :peer-config] peer-config) ;; to much reach?
          (assoc-in [:conf :stream :env-config] env-config)   ;; to much reach?
          (assoc :peer-group peer-group
                 :peers peers
                 :windowing-env windowing-env))))
  (stop [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          {:keys [job-ids
                  windowing-env
                  peer-group
                  peers]} cmp]
      ;; stop the peers
      (if peers (onyx.api/shutdown-peers peers))
      ;; stop the peer manager
      (if peer-group (onyx.api/shutdown-peer-group peer-group))
      ;; stop bookeeper
      (if windowing-env (onyx.api/shutdown-env windowing-env))
      (assoc cmp
             :job-ids nil
             :windowing-env nil
             :peer-group nil
             :peers nil))
    )
  JobManager
  (submit-job [cmp job]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          result (onyx.api/submit-job peer-config job)]
      (if (:success? result)
        (update cmp :job-ids conj (:job-id result))
        (update cmp :failed-submit-job result))))
  ;; may have to parse output of onyx.api/subscribe-to-log to get current running jobs
  (kill-jobs [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          job-ids (:job-ids cmp)
          ]
      (if (not (empty? job-ids))
        ;; kill-job always returns true
        (map (partial onyx.api/kill-job peer-config) job-ids))
      (assoc cmp :job-ids nil))))

