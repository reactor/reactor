# Reactor Pipes - in-process channels and streams

Reactor Pipes is foundation for building complex, performant
distributed and in-memory stream processing topologies.

## Why Pipes?

Current state-of-art Stream Processing solutions bring enormous
development and maintenance overhead and can mostly serve as a
replacement for Extract-Load-Transform / Batch Processing jobs (think
Hadoop).

Reactor Pipes is a lightweight Stream Processing system, which was
produced with performance, composability, development and support
simplicity in mind. You can scale it on one or many machines without any
trouble, building complex processing topologies that are as easy to test
as normal operations on `Collections`. After all, Streams are nothing
but eternal sequences.

The problem with Machine Learning on Streams is that most of the
algorithms assume some kind of state: for example, Classification
assumes that trained model is available. Having this state somewhere in
the Database or Cache brings additional deserialization overhead, and
having it in memory might be hard if the system doesn't give you
partitioning guarantees (that requests dedicated to same logical entity
will end up on the same node).

Reactor Pipes approach is simple: develop on one box, for one box, break
processing in logical steps for distribution and scale up when needed.
Because of nature and the layout of wiring between pipes and data
flows, you will be able to effortlessly scale it up.

## Quickstart

Each `Pipe` represents a transformation from some `INITIAL` type
to `CURRENT` type. Pipe gets subscribed to the given key, and
expects values of the `INITIAL` type to be published for that key.

With `Pipe`, you can build more complex topologies. Let's build
one which would receive a stream of `Integers`, increment them,
then multiply by two.

```java
import reactor.pipe.Pipe;

Firehose firehose = new Firehose();

Pipe.<Integer>build()
	.map(i -> i + 1)
	.map(i -> i * 2)
	.consume((v) -> System.out.println(v)))
    .subscribe(Key.wrap("key1"), firehose);

// send a couple of payloads
firehose.notify(Key.wrap("key1"), 1);
// => 4

firehose.notify(Key.wrap("key1"), 2);
// => 6
```

Pipe could be subscribed to t

## Built-in operations

  * `map` takes every incoming item and transforms it to the item of the other type,
  possibly having some side effects inbetween
  * `filter`
  * `reduce`
  * `slide`
  * `partition`
  * `consume`

## State

Sometimes it's required to store some intermeiate computation
state during the processing. In order to do that, pipes
provide

## Custom Operations

## Order

We do not guarantee any order on interleaved operations, since they're
executed in a manner resembling tail calls. Although if some of your
operations (for example, windowing) rely on the order of things, you
have to use sequence ids or timestamps to restore order or use
non-concurrent processors.

# License

Copyright © 2014 Alex Petrov

Reactor Pipes is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
