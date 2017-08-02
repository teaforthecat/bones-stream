# bones.stream

work in progress 

part of the [bones framework](https://github.com/teaforthecat/bones)

bones.stream is a Pipeline implementation built
on [Onyx](http://onyxplatform.org). It offers Input and Output components to
ease the configuration and setup effort. It has the goal of a slim
API to make getting started as easy as possible. For a quick implementation
example see [core-test](test/bones/stream/core_test.clj).

[![Build Status](https://travis-ci.org/teaforthecat/bones-stream.svg?branch=master)](https://travis-ci.org/teaforthecat/bones-stream)

## Deploy

It is recommended to run Aeron in it's own process in production.
Here is how to do that.

Add namespaces to the lein profile that builds the jar. The namespaces must have
`-main` methods and `(:gen-class)` in the definition.

    :profiles {
      :uberjar {:aot [xyz.stream
                      lib-onyx.media-driver
                      xyz.web
                      xyz.other]}}

Build the jar

    lein uberjar
    
   
Start processes with each of the desired entry points like this: 

(note underscores)
 
    java -cp target/xyz-standalone.jar xyz.stream
    java -cp target/xyz-standalone.jar lib_onyx.media_driver
    java -cp target/xyz-standalone.jar xyz.web
    java -cp target/xyz-standalone.jar xyz.other


## Kafka Input

Reuse configuration from Onyx Kafka Plugin to create a producer that will use
the topic that the :bones/input task is configured to cosume.

## Redis Output

Reuse configuration from the :bones/output task to create a subscription to the
same channel that the task is configured to publish to.


## Tests

    docker-compose up -d
    lein test
    
There is also a "test" in "production configuration". Try it with these commands.
    
    lein uberjar
    # start first
    docker-compose up -d 
    # start second
    java -cp target/stream-0.0.3-standalone.jar lib_onyx.media_driver 
    # start third
    java -cp target/stream-0.0.3-standalone.jar bones.stream.core_test 

You should see `:args [left up]` in the output because
`bones.stream.core-test/my-inc` was executed between the `:bones/input` kafka
task and the `:bones/output` redis task

## License

Copyright © 2017 Chris Thompson

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
