(ns bones.stream.serializer
  "read and write transit data to and from byte-arrays
  non-transit json is supported with the :json-plain parameter
  (encoder :json)         ;=> transit
  (decoder :json)         ;=> transit
  (encoder :json-verbose) ;=> transit
  (decoder :json-verbose) ;=> transit
  (encoder :msgpack)      ;=> transit
  (decoder :msgpack)      ;=> transit
  (encoder :json-plain)   ;=> clojure.data.json
  (decoder :json-plain)   ;=> clojure.data.json
  "
  (:require [cognitect.transit :as transit]
            [clojure.data.json :as json])
  (:import [java.io ByteArrayInputStream ByteArrayOutputStream]))

(def formats (-> (make-hierarchy)
                 (derive :json :transit)
                 (derive :json-verbose :transit)
                 (derive :msgpack :transit)))

(defmulti encoder keyword :hierarchy #'formats)

(defmethod encoder :transit
  [data-format]
  (fn [data]
    (.toByteArray
     (let [buf (ByteArrayOutputStream. 4096)
           writer (transit/writer buf data-format)]
       ;; write to writer, but return buffer
       (transit/write writer data)
       buf))))

(defmethod encoder :json-plain
  [data-format]
  (fn [data]
    (.getBytes (json/write-str data))))

(defmulti decoder keyword :hierarchy #'formats)

(defmethod decoder :transit
  [data-format]
  (fn [bytes]
    (if bytes ;; nil is allowed for kafka log compaction
      (-> bytes
          (ByteArrayInputStream.)
          (transit/reader data-format)
          (transit/read)))))

(defmethod decoder :json-plain
  [data-format]
  (fn [data]
    (json/read-str (apply str (map char data)))))

;; these are here just because transit has them
(def en-json-transit (encoder :json))
(def de-json-transit (decoder :json))
(def en-json-verbose (encoder :json-verbose))
(def de-json-verbose (decoder :json-verbose))
;; I really only see these as interesting options
;; - msgpack for compression
;; - json-plain for sharing data
(def en-msgpack (encoder :msgpack))
(def de-msgpack (decoder :msgpack))
(def en-json-plain (encoder :json-plain))
(def de-json-plain (decoder :json-plain))

(comment
  (methods encoder)
  (remove-method encoder [:json :json-verbose :msgpack])
  (remove-method decoder [:json :json-verbose :msgpack])
  (isa? formats :json :transit)

  ((decoder :json)
   ((encoder :json) "hello"))

)
