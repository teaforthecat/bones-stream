# bones.stream

work in progress 

part of the [bones framework](https://github.com/teaforthecat/bones)

bones.stream is a Pipeline implementation built
on [Onyx](http://onyxplatform.org). It offers Input and Output components to
ease the configuration and setup effort. It has the goal of a slim
API to make getting started as easy as possible. For a quick implementation
example see [core-test](test/bones/stream/core_test.clj).


## Kafka Input

Reuse configuration from Onyx Kafka Plugin to create a producer that will use
the topic that the :bones/input task is configured to cosume.

## Redis Output

Reuse configuration from the :bones/output task to create a subscription to the
same channel that the task is configured to publish to.



## License

Copyright © 2017 Chris Thompson

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
