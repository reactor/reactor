# Reactor Pipes - in-process channels and streams

Reactor Pipes is foundation for building complex, performant distributed and in-memory
stream processing topologies.

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

Reactor Pipes can be also used to make asynchronous/concurrent programs
simpler, easier to maintain and construct. For example, you could use
__Channels__ with __Pipes__ in order to build a lightweight WebSocket
server implementation. Another example is asynchronous handlers in
your HTTP server, which would work with any driver, no
matter whether it offers asynchronous API or no. Other examples include
IoT, Machine Learning, Business Analytics and more.

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

## Terminology

`Stream` is a term coming from Reactive Programming. The stream looks a
little like a collection from the consumer perspective, with the only
difference that if a collection is a ready set of events, the stream is
an infinite collection. If you do `map` operation on the stream, the
`map` function will see each and every element coming to the stream.

`Publisher` (`generator` or `producer` in some terminologies) is a
function or entity that publishes items to the stream. `Consumer` (or
`listener`, in some terminologies) is a function that is subscribed to
the stream, and will be asynchronously getting items that the
`publisher` publishes to the stream.

In some cases, pipes can serve simultaneously as a `consumer` and as a
`producer`. For example, `map` is consumed to the events coming in the
stream, and publishes modified events further downstream.

`Topology` is a stream with a chain of publishers and producers attached
to it. For example, you can have a stream that `maps` items,
incrementing each one of them, then a `filter` that picks up only even
incremented numbers. Of course, in real life applications topologies are
much more complex.

## Firehose

`Firehose` is an in-memory key/value transport. It serves as a flexible
abstraction for all the other parts of reactive streams implemented
in Pipes.

With `Firehose`, you can `subscribe` to some `key`, `notify` all
the `key` subscribers about the new `value`:

```java
import reactor.pipe.Firehose;

Firehose firehose = new Firehose();

firehose.on(Key.wrap("key1"),
            (value) -> System.out.println(value));

// Push some values to "key1"
firehose.notify(Key.wrap("key1"),
                1);
// => 1

firehose.notify(Key.wrap("key1"),
                2);
// => 2
```

You can have multiple `consumers` for each key. Also, you can build
cascading topologies, for example, you can receive values from
one `key`, transform them and redirect the result to the other key:

```java
// Make a subscription to "key1", where we increment
// the incoming Integer by 1 and redirect the result to "key2"
firehose.on(Key.wrap("key1"), (Integer i) -> {
      firehose.notify(Key.wrap("key2"), i + 1);
    });

// Subscribe to the resulting "key2"
firehose.on(Key.wrap("key2"), System.out::println);

// Push some values to "key1"
firehose.notify(Key.wrap("key1"), 1);
// => 2

firehose.notify(Key.wrap("key1"), 2);
// => 3
```

However useful this is, code based entirely on the callbacks might
be hard to maintain and debug, as subscriptions might get
scattered across the codebase.

In order to simplify this workflow, `Pipe` was created.

## Pipes

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

## Uni- and Bi- directional Channels

Channels are much like a queue you can publish to and pull your changes
from the queue-like object. This feature is particularly useful in
scenarios when you don't need to have neither subscription nor publish
hey, and you need only to have async or sync uni- or bi- directional
communication.

```java
Pipe<Integer> intPipe = new Pipe<>(firehose);
Channel<Integer> chan = intPipe.channel();

chan.tell(1);
chan.tell(2);

chan.get();
// => 1

chan.get();
// => 2

chan.get();
// => null
```

Channels can be consumed by the pipes, too. It is although important to
remember that in order to avoid unnecessary state accumulation Channels
can either be used as a pipe or as a channel:

```java
Pipe<Integer> pipe = new Pipe<>(firehose);
Channel<Integer> chan = pipe.channel();

chan.stream()
    .map(i -> i + 1)
    .consume(i -> System.out.println(i));

chan.tell(1);
// => 2

chan.get();
// throws RuntimeException, since channel is already drained by the stream
```

Channels can also be split to publishing and consuming channels for type
safety, if you need to ensure that consuming part can't publish messages
and publishing part can't accidentally consume them:

```java
Pipe<Integer> pipe = new Pipe<>(firehose);
Channel<Integer> chan = pipe.channel();

PublishingChannel<Integer> publishingChannel = chan.publishingChannel();
ConsumingChannel<Integer> consumingChannel = chan.consumingChannel();

publishingChannel.tell(1);
publishingChannel.tell(2);

consumingChannel.get();
// => 1

consumingChannel.get();
// => 2
```

## Order

We do not guarantee any order on interleaved operations, since they're
executed in a manner resembling tail calls. Although if some of your
operations (for example, windowing) rely on the order of things, you
have to use sequence ids or timestamps to restore order or use
non-concurrent processors.

# License

Copyright © 2014 Alex Petrov

Reactor Pipes is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
