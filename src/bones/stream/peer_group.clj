(ns bones.stream.peer-group
  (:require [com.stuartsierra.component :as component]
            [bones.stream.protocols :as p]
            [bones.stream.kafka :as k]
            [onyx.api]
            [onyx.static.util :refer [kw->fn]]
            [bones.stream.redis :as redis]))


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
          peer-config (get-in cmp [:conf :stream :peer-config])
          env-config (get-in cmp [:conf :stream :env-config])
          n-peers 3 ;; or greater

          ;; windowing-env runs(or connects to) a bookeeper server to coordinate
          ;; with other peer groups(processes)
          windowing-env (onyx.api/start-env env-config)
          peer-group (onyx.api/start-peer-group peer-config)
          peers (onyx.api/start-peers n-peers peer-group)
          ]
      (assoc cmp :peer-group peer-group
                 :peers peers
                 :windowing-env windowing-env)))
  (stop [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          {:keys [producer
                  job-ids
                  windowing-env
                  peer-group
                  peers]} cmp]
      ;; stop accepting messages
      (if (:producer producer) (.close (:producer producer)))
      ;; stop the peers
      (if peers (onyx.api/shutdown-peers peers))
      ;; stop the peer manager
      (if peer-group (onyx.api/shutdown-peer-group peer-group))
      ;; stop bookeeper
      (if windowing-env (onyx.api/shutdown-env windowing-env))
      (assoc cmp
             :producer nil
             :job-ids nil
             :windowing-env nil
             :peer-group nil
             :peers nil))
    )
  JobManager
  (submit-job [cmp job]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          job-id (onyx.api/submit-job peer-config job)]
      (update cmp :job-ids conj job-id)))
  ;; may have to parse output of onyx.api/subscribe-to-log to get current running jobs
  (kill-jobs [cmp]
    (let [peer-config (get-in cmp [:conf :stream :peer-config])
          job-ids (:job-ids cmp)]
      (if (not (empty? job-ids))
        (map (partial onyx.api/kill-job peer-config) job-ids))
      (assoc cmp :job-ids nil))))

