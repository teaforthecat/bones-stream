(defproject bones/stream "0.0.1"
  :description "A spec-driven Onyx compiler"
  :url "https://github.com/teaforthecat/bones-stream"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0-alpha14"]
                 [com.stuartsierra/component "0.3.2"]

                 [org.onyxplatform/onyx "0.10.0-beta8"]

                 [com.taoensso/carmine "2.16.0"]
                 [org.onyxplatform/onyx-kafka "0.8.8.0"]
                 [manifold "0.1.6-alpha4"]
                 [com.cognitect/transit-clj "0.8.297"]
                 ]

  ;; maybe include this too to help others write tests
  :profiles {:dev {:dependencies [[org.onyxplatform/onyx-local-rt "0.10.0.0-beta8"                                   ]]}}

  )
