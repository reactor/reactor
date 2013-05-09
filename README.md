# Reactor

`Reactor` is a foundation for asynchronous applications on the JVM. It provides abstractions for Java, Groovy and other JVM languages to make building event and data-driven applications easier. It’s also really fast. On modest hardware, it's possible to process around 15,000,000 events per second with the fastest non-blocking `Dispatcher`. Other dispatchers are available to provide the developer with a range of choices from thread-pool style, long-running task execution to non-blocking, high-volume task dispatching.

### Build instructions

[![Build Status](https://drone.io/github.com/reactor/reactor/status.png)](https://drone.io/github.com/reactor/reactor/latest)

`Reactor` uses a Gradle-based build system. Building the code yourself should be a straightforward case of:

    git clone git@github.com:reactor/reactor.git
    cd reactor
    ./gradlew test

This should cause the submodules to be compiled and the tests to be run. To install these artifacts to your local Maven repo, use the handly Gradle Maven plugin:

    ./gradlew install

### Introduction

`Reactor`, as the name suggests, is heavily influenced by the well-known [Reactor design pattern](http://en.wikipedia.org/wiki/Reactor_pattern). But it is also influenced by other event-driven design practices, as well as several awesome JVM-based solutions that have been developed over the years. Reactor's goal is to condense these ideas and patterns into a simple and reusable foundation for making event-driven programming much easier.

Reactor is also designed to be friendly to [Java 8 lambdas](http://www.jcp.org/en/jsr/detail?id=335). Many components within Reactor can be drop-in replaced with lambdas or method references to make your Java code more succinct. We've also found that using Java 8 lambdas and method references results in slightly higher throughput. But even if you can't use Java 8 yet, Reactor will work fine in Java 6 and 7 (you'll just have to implement more anonymous inner classes).

### Events, Selectors and Consumers

Three of the most foundational components in Reactor’s `reactor-core` module are the `Selector`, the `Consumer`, and the `Event`. A `Consumer` can be assigned to a `Reactor` by using a `Selector`, which is a simple abstraction to provide flexibility when finding the `Consumer`s to invoke for an `Event`. A range of default selectors are available. From plain `String`s to regular expressions to Spring MVC-style URL templates.

##### Selector Matching

There are different kinds of `Selector`s for doing different kinds of matching. The simplest form is just to match one object with another. For example, a `Selector` created from a `String` "test" will match another `Selector` whose wrapped object is also a `String` "test" (in this case it's just like a `String.equals(String)`.

But a `Selector` can also match another `Selector` based on `Class.isAssignableFrom(Class<?>)`, regular expressions, URL templates, or the like. There are helper methods on the `Fn` abstract class to make creating these `Selector`s very easy in user code.

Here's is an example of wiring a `Consumer` to a `Selector` on a `Reactor`:

	  import static reactor.Fn.$;

    // Call a helper method that creates a Reactor that is registered in the global registry.
    Reactor r = R.create();

    // Wire an event to print out the data sent with the Event
    r.on($("hello"), new Consumer<Event<String>>() {
      public void accept(Event<String> ev) {
        System.out.println("Greeting: " + ev.getData());
      }
    });

    // Send an event to this Reactor and trigger all consumers that match the given Selector
    r.notify($("hello"), Fn.event("Hello World!"));

In Java 8, the event wiring would become extremely succinct:

    public class MyService {
      public void handleEvent(Event<String> ev) {
        // Do some work
      }
    }

    @Inject
    MyService service;

    r.on($("test"), (ev) -> service::handleEvent);

##### Flexible Consumer Assignment

If you don't need to access the argument of the `Event` and just need to execute code when the event is triggered, then use the composition method on `Fn` to turn any `Runnable` or `Callable<?>` into a consumer:

    Reactor r = R.create();

    r.on($("test"), Fn.compose(new Runnable() {
      public void run() {
        System.out.println("Test is finished.");
      }
    }));

    // Trigger all consumers that match the given Selector
    r.notify($("test"));

##### Headers

Events have optional associated metadata in the `headers` property. Events are meant to be stateless helpers that provide an consumer with an argument value and related metadata. If you need to communicate information to consumer components, like the IDs of other Reactors that have an interest in the outcome of the work done on this `Event`, then set that information in a header value.

	// Just use the default selector instead of a specific one
	r.on(new Consumer<Event<String>>() {
      public void accept(Event<String> ev) {
        String otherData = ev.getHeaders().get("x-custom-header");
        // do something with this other data
      }
	});

	Event<String> ev = Fn.event("Hello World!");
	ev.getHeaders().set("x-custom-header", "ID_TO_ANOTHER_REACTOR");
	r.notify(ev);

### Registrations

When assigning an `Consumer` to a `Reactor`, a `Registration` is provided to the caller to manage that assignment. `Registration`s can be cancelled, which removes them from the `Reactor` or, if you don't want to remove an consumer entirely but just want to pause its execution for a time, you can accept `pause()` and later `resume()` which will cause the `Dispatcher` to skip over that `Consumer` when finding `Consumer`s that match a given `Selector`.

	Registration reg = r.on(#("test"), new Consumer<Event<?>>() { … });

	// pause this consumer so it's not executed for a time
	reg.pause();

	// later decide to resume it
	reg.resume();

### Complete Extensibility

Reactor really just provides a foundation upon which you can build a very powerful event-driven framework to power your own apps. If the default `Selector` implementations don't take into account your own domain-specific information, you can simply implement your own `Selector` that does whatever checks are required. It would be quite easy to add security to an application by creating a user-aware `Selector` that would fail to match an consumer if the user wasn't authorized.

Beyond implementing a `Selector`, there is also a `SelectionStrategy` interface that can be provided to a `Reactor` which will be used to match `Selector`s rather than by using the `matches()` methods on the `Selector` itself. One could implement consistent hashing of consumers by implementing a `ConsistentHashingSelectionStrategy`.

### Load-balancing

Reactor includes two kinds of built-in load-balancing for assigned consumers: ROUND_ROBIN and RANDOM. This means that, of the given `Consumer`s assigned to the same `Selector`, the `LoadBalancingStrategy` is checked to determine whether to execute all consumers, one of them randomly selected, or in a round robin fashion. The default is `NONE`, which means execute all assigned consumers.

---

`Rector` is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).