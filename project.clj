(defproject bones/stream "0.0.3"
  :description "A spec-driven Onyx compiler"
  :url "https://github.com/teaforthecat/bones-stream"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [#_[org.clojure/clojure "1.9.0-alpha14"]
                 [org.clojure/clojure "1.8.0"] ;; onyx
                 [com.stuartsierra/component "0.3.2"]
                 [byte-streams "0.2.3"]

                 ;; bones deps
                 [bones/conf "0.2.3"]

                 ;; dev profile only?
                 [org.onyxplatform/onyx-local-rt "0.10.0.0-beta8"]

                 [org.onyxplatform/lib-onyx "0.10.0.0"]
                 [org.onyxplatform/onyx "0.10.0-beta10"]
                 [org.onyxplatform/onyx-kafka "0.10.0.0-beta10"]
                 [com.taoensso/carmine "2.16.0"]
                 ;; [clj-kafka "0.3.4"];; onyx-kafka moved to franzy
                 ;; [cheshire "5.5.0"] ;; missing from onyx-kafka deps
                 ;; [aero "0.2.0"]     ;; missing from onyx-kafka deps
                 [manifold "0.1.6-alpha4"]
                 [com.cognitect/transit-clj "0.8.297"]
                 ]


  :profiles {
             ;; for testing a production configuration
             :uberjar {:aot [bones.stream.core-test
                             lib-onyx.media-driver]
                       :source-paths ["src" "test"]}})
