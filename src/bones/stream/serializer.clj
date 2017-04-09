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
    (-> bytes
        (ByteArrayInputStream.)
        (transit/reader data-format)
        (transit/read))))

(defmethod decoder :json-plain
  [data-format]
  (fn [data]
    (json/read-str (apply str (map char data)))))

(comment
  (methods encoder)
  (remove-method encoder [:json :json-verbose :msgpack])
  (remove-method decoder [:json :json-verbose :msgpack])
  (isa? formats :json :transit)

  ((decoder :json)
   ((encoder :json) "hello"))

)
