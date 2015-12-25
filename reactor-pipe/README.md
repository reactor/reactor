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

Pipe.<Integer>build()
    .map(i -> i + 1)
    .map(i -> i * 2)
    .consume((v) -> System.out.println(v))
    .subscribe(Key.wrap("key1"), bus);

// send a couple of payloads
bus.notify(Key.wrap("key1"), 1);
// => 4

bus.notify(Key.wrap("key1"), 2);
// => 6
```

## Built-in operations

Pipe operations are somewhat similar to callbacks. For each item
within the stream, the operation will be called. __Optionally__,
operation can construct a new value from the old one and
pass it to the next operation within the pipe chain.

`map`/`scan` operations are taking items one by one and always
call the next operation within the chain. `filter` operation would only
call next operation if predicate is matched, and operations
such as `slide`, `partition` would aggregate and call
next operation depending on the partitioning logic.

  * `map` takes every incoming item and transforms it to the item of the other type,
  possibly having some side effects inbetween.
  * `filter` calls the next operation within the chain only for the items for which
  the predicate returns true.
  * `scan` "reduces" (aggregates) the stream to some representative value, downstreams
  the aggregate on the each incoming item.
  * `slide` "sliding" window that keeps seen items within the stream based on some logic.
  * `partition` "tumbling" window that fills up and flushes based on the provided
  logic. Could be used to split stream of `Integer` values into `List<Integer>` of
  size 5 (partition stream by 5 items).
  * `consume` registers a consumer callback for each item within the stream.

## State

Sometimes it's required to store some intermeiate computation
state for the processing. This might be particularly useful
for the cases when replaying the whole stream from the very
beginning is expensive. Instead, you save the intermediate
state in the `Atom`.

In order to do that, pipes provide `reactor.pipe.state.StateProvider`.
State provider has to implement the `makeAtom` method, that
constructs the `reactor.pipe.concurrent.Atom` with the state
either loaded from database or some initial state provided
by default.

`Atom` could be constructed with a callback that could be
used for flushing state back to the database.

## Order

We do not guarantee any order on interleaved operations, since they're
executed in a manner resembling tail calls. Although if some of your
operations (for example, windowing) rely on the order of things, you
have to use sequence ids or timestamps to restore order or use
non-concurrent processors.

# License

Copyright © 2014 Alex Petrov

Reactor Pipes is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
