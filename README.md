# Reactor

`Reactor` is a foundation for asynchronous applications on the JVM. It provides abstractions for Java, Groovy and other JVM languages to make building event and data-driven applications easier. It’s also really fast. On modest hardware, it's possible to process around 15,000,000 events per second with the fastest non-blocking `Dispatcher`. Other dispatchers are available to provide the developer with a range of choices from thread-pool style, long-running task execution to non-blocking, high-volume task dispatching.

[![Build Status](https://drone.io/github.com/reactor/reactor/status.png)](https://drone.io/github.com/reactor/reactor/latest)

### Build instructions

`Reactor` uses a Gradle-based build system. Building the code yourself should be a straightforward case of:

    git clone git@github.com:reactor/reactor.git
    cd reactor
    ./gradlew test

This should cause the submodules to be compiled and the tests to be run. To install these artifacts to your local Maven repo, use the handly Gradle Maven plugin:

    ./gradlew install

### Maven Artifacts

Snapshot Maven artifacts are provided in the SpringSource snapshot repositories. To add this repo to your Gradle build, specify the URL like the following:

    ext {
      reactorVersion = '1.0.0.BUILD-SNAPSHOT'
    }

    repositories {
      mavenLocal()
      maven { url 'http://repo.springsource.org/libs-release' }
      maven { url 'http://repo.springsource.org/libs-milestone' }
      maven { url 'http://repo.springsource.org/libs-snapshot' }
      mavenCentral()
    }

    dependencies {
      // Reactor Core
      compile 'reactor:reactor-core:$reactorVersion'
      // Reactor Groovy
      compile 'reactor:reactor-groovy:$reactorVersion'
      // Reactor Spring
      compile 'reactor:reactor-spring:$reactorVersion'
    }

When Reactor is released as a milestone or release, the artifacts will be generally available in Maven Central. Until then, you'll want to use the above snapshot repository for access to the artifacts.

### Community / Support

* [reactor-framework Google Group](https://groups.google.com/forum/?#!forum/reactor-framework)
* [GitHub Issues](https://github.com/reactor/reactor/issues)
* [IRC Channel](irc://irc.freenode.net/#ProjectReactor)

### Introduction

`Reactor`, as the name suggests, is heavily influenced by the well-known [Reactor design pattern](http://en.wikipedia.org/wiki/Reactor_pattern). But it is also influenced by other event-driven design practices, as well as several awesome JVM-based solutions that have been developed over the years. Reactor's goal is to condense these ideas and patterns into a simple and reusable foundation for making event-driven programming much easier.

Reactor is also designed to be friendly to [Java 8 lambdas](http://www.jcp.org/en/jsr/detail?id=335). Many components within Reactor can be drop-in replaced with lambdas or method references to make your Java code more succinct. We've also found that using Java 8 lambdas and method references results in slightly higher throughput. But even if you can't use Java 8 yet, Reactor will work fine in Java 6 and 7 (you'll just have to implement more anonymous inner classes).

### Event Loops, Dispatchers, and RingBuffers

Reactor provides three out-of-the-box `Dispatcher` implementations. The `ThreadPoolExecutorDispatcher` is designed to be used in situations where your tasks are longer-running, do blocking IO, or otherwise take longer to complete. It is backed by a standard JDK `ExecutorService`. The `BlockingQueueDispatcher` is analogous to an event loop. In the default configuration, one of these Dispatchers is created per CPU. They are handed out in a round-robin fashion, so when a new `Reactor` is created and the `dispatcher(Environment.EVENT_LOOP)` method is called on the `Reactor.Spec`, one of the event loop Dispatchers is handed back.

The highest-throughput Dispatcher is the `RingBufferDispatcher` which is based on the [LMAX Disruptor RingBuffer](https://github.com/lmax-exchange/disruptor). It is a single-thread, RingBuffer-backed Dispatcher. It usually has the highest performance because there is no context switching required. Tasks are executed in order since their execution is managed by the RingBuffer. The CPU is also saturated when a large volume of tasks is submitted to the Dispatcher so although it might seem necessary to provide multiple threads to ensure the highest throughput, the RingBufferDispatcher provides significantly higher overall non-blocking task throughput than any other Dispatcher implementation with a single thread.

### The Environment

Each `Reactor` you create needs a `Dispatcher` to execute tasks. By default, with no configuration, you'll get a synchronous Dispatcher. This works fine for testing but is probably not what you want for a real application.

Since it's not desirable to create too many threads in an asynchronous application and *something* has to keep track of those few Dispatchers that are divvyed out to the components that need them, you need to instaniate an Environment which will create those Dispatchers based on either the default configuration (provided in a properties file in the Reactor JAR file) or by providing your own configuration.

There's no magic to it. You simply "new" up an instance of `Environment` and, when creating Reactors, Streams, and Promises, pass a reference to this Environment into the Specs (essentially a "builder" helper class). The Environment instance is where the `reactor.` system properties live and it's also the place where the small number of Dispatchers that are intended to be used by any component in your application that needs one reside.

You can, of course, create Dispatchers directly in your code. There may be times in scenarios like embedding in other threading scenarios where that's desirable. But in general, you should refrain from directly instantiating your own Dispatchers and instead use those configured to be created by the Environment.

### Events, Selectors and Consumers

Three of the most foundational components in Reactor’s `reactor-core` module are the `Selector`, the `Consumer`, and the `Event`. A `Consumer` can be assigned to a `Reactor` by using a `Selector`, which is a simple abstraction to provide flexibility when finding the `Consumer`s to invoke for an `Event`. A range of default selectors are available. From plain `String`s to regular expressions to Spring MVC-style URL templates.

##### Selector Matching

There are different kinds of `Selector`s for doing different kinds of matching. The simplest form is just to match one object with another. For example, a `Selector` created from a `String` "parse" will match another `Selector` whose wrapped object is also a `String` "parse" (in this case it's just like a `String.equals(String)`.

But a `Selector` can also match another `Selector` based on `Class.isAssignableFrom(Class<?>)`, regular expressions, URL templates, or the like. There are helper methods on the `Selectors` abstract class to make creating these `Selector`s very easy in user code.

Here's is an example of wiring a `Consumer` to a `Selector` on a `Reactor`:

    // This helper method is like jQuery’s.
    // It just creates a Selector instance so you don’t have to "new" one up.
    import static reactor.event.selector.Selectors.$;

    …

    Environment env = new Environment();

    // This factory call creates a Reactor.
    Reactor reactor = Reactors.reactor()
      .env(env) // our current Environment
      .dispatcher(Environment.EVENT_LOOP) // use one of the BlockingQueueDispatchers
      .get(); // get the object when finished configuring

    // Register a consumer to handle events sent with a key that matches "parse"
    reactor.on($("parse"), new Consumer<Event<String>>() {
      @Override
      public void accept(Event<String> ev) {
        System.out.println("Received event with data: " + ev.getData());
      }
    });
    // Send an event to this Reactor and trigger all actions that match the given key
    reactor.notify("parse", Event.wrap("data"));

In Java 8, the event wiring would become extremely succinct:

    // Use a POJO as an event handler
    class Service {
      public <T> void handleEvent(Event<T> ev) {
        // handle the event data
      }
    }

    @Inject
    Service service;

    // Use a method reference to create a Consumer<Event<T>>
    reactor.on($("parse"), service::handleEvent);

    // Notify consumers of the 'parse' topic that data is ready
    // by passing a Supplier<Event<T>> in the form of a lambda
    reactor.notify("parse", () -> {
      slurpNextEvent()
    });


##### Headers

Events have optional associated metadata in the `headers` property. Events are meant to be stateless helpers that provide a consumer with an argument value and related metadata. If you need to communicate information to consumer components, like the IDs of other Reactors that have an interest in the outcome of the work done on this `Event`, then set that information in a header value.

    // Just use the default selector instead of a specific one
    r.on(new Consumer<Event<String>>() {
        public void accept(Event<String> ev) {
          String otherData = ev.getHeaders().get("x-custom-header");
          // do something with this other data
        }
    });

    Event<String> ev = Event.wrap("Hello World!");
    ev.getHeaders().set("x-custom-header", "ID_TO_ANOTHER_REACTOR");
    r.notify(ev);

### Registrations

When assigning an `Consumer` to a `Reactor`, a `Registration` is provided to the caller to manage that assignment. `Registration`s can be cancelled, which removes them from the `Reactor` or, if you don't want to remove an consumer entirely but just want to pause its execution for a time, you can accept `pause()` and later `resume()` which will cause the `Dispatcher` to skip over that `Consumer` when finding `Consumer`s that match a given `Selector`.

    Registration reg = r.on($("test"), new Consumer<Event<?>>() { … });

    // pause this consumer so it's not executed for a time
    reg.pause();

    // later decide to resume it
    reg.resume();

### Complete Extensibility

Reactor really just provides a foundation upon which you can build a very powerful event-driven framework to power your own apps. If the default `Selector` implementations don't take into account your own domain-specific information, you can simply implement your own `Selector` that does whatever checks are required. It would be quite easy to add security to an application by creating a user-aware `Selector` that would fail to match an consumer if the user wasn't authorized.

Beyond implementing a `Selector`, there is also a `SelectionStrategy` interface that can be provided to a `Reactor` which will be used to match `Selector`s rather than by using the `matches()` methods on the `Selector` itself. One could implement consistent hashing of consumers by implementing a `ConsistentHashingSelectionStrategy`.

### Routing

Reactor includes five different kinds of routing for assigned consumers: broadcast, random, round-robin, first, and last. This means that, of the given `Consumer`s assigned to the same `Selector`, the routing will determine whether to execute all the consumers, one of them randomly selected, one of them selected in a round robin fashion, the first matching consumer, or the last matching consumer. The default is to use broadcast routing.

---

Reactor is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
