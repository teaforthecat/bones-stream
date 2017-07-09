(ns bones.stream.protocols)

(defprotocol InputOutput
  (input [_ msg])
  (output [_ stream]))

(defprotocol Input
  (input [_ msg]))

(defprotocol Output
  (output [_ stream]))

(defprotocol Publish
  (publish [this user-id message]))

(defprotocol Subscribe
  (subscribe [this user-id stream]))

(defprotocol MaterializedView
  (write [this topic rkey value])
  (fetch [this rkey])
  (fetch-keys [this topic])
  (fetch-all [this topic]))
