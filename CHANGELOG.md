# Reactor 1.1.0 - CHANGELOG

This is a non-exhaustive list of changes between Reactor 1.0 and 1.1:

## `reactor-core`
----

* Numerous bug fixes and improvements

### Stream / Promise

* Improved Stream and Promise value-handling
* Additional composition methods like connect(), merge(), timeout(), window() and more
* Many methods moved into Composable so shared between Stream and Promise

### Utilities

* Robust HashWheelTimer implemenation based on a `RingBuffer`
* Allocator API for efficient object pooling
* New Consumer Registry implementation based on gs-collections 5.0 [1]

### Testing

* Numerous improvements to benchmarking
* Added dedicated `reactor-benchmark` project based on JMH [2]
* Removed most benchmarking code from core project
* Expanded and improved test coverage

## `reactor-logback`
----

* Extremely efficient high-speed logging using Java Chronicle
* Re-written Reactor-based async appender implementations

## `reactor-groovy`
----

* Better organization of Groovy support
* AST-based extensions moved to their own subproject for better Gradle compatibility
* Ready for Groovy 2.3 and Java 1.8

## `reactor-net`
----

* Renamed `reactor-tcp` to `reactor-net`
* Refactored base abstractions to handle both TCP and UDP
* Added UDP support using Netty
* Added ZeroMQ support using `jeromq`
* Rewritten `reconnect` support 
* Improved and exapanded testing
* Numerous bug fixes and improvements

----
[1] - https://github.com/goldmansachs/gs-collections

[2] - http://openjdk.java.net/projects/code-tools/jmh/