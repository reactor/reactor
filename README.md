# Reactor Framework

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

`Reactor` is a foundational library building for reactive fast data applications on the JVM. It provides abstractions for Java, Groovy, [Clojure](https://github.com/clojurewerkz/meltdown) and other JVM languages to make building event and data-driven applications easier. It’s also really fast. On a recent laptop with a dual-core processor, it's possible to process over 15,000,000 events per second with the `RingBufferProcessor` and over 25,000,000 events per second in a single thread. Other processors are available to provide the developer with a range of choices from thread-pool style, long-running task execution to non-blocking, high-volume task dispatching.

[![Build Status](http://drone.io/github.com/reactor/reactor/status.png)](https://drone.io/github.com/reactor/reactor/latest)

### Enrolling

[Join the initiative](https://support.springsource.com/spring_committer_signup), fork, discuss and PR anytime. Roadmap is collaborative and we do enjoy new ideas, simplifications, doc, feedback, and, did we mention feedback already ;) ? As any other open source project, you are the hero, Reactor is only useful because of you and we can't wait to see your pull request mate !

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
      reactorVersion = '2.5.0.BUILD-SNAPSHOT'
    }

    repositories {
      //maven { url 'http://repo.spring.io/libs-release' }
      //maven { url 'http://repo.spring.io/libs-milestone' }
      maven { url 'http://repo.spring.io/libs-snapshot' }
      mavenCentral()
    }

    dependencies {
      // Reactor Core
      compile "io.projectreactor:reactor-core:$reactorVersion"

      // Reactor Stream
      // compile "io.projectreactor:reactor-stream:$reactorVersion"

      // Reactor Spring
      // compile "io.projectreactor:reactor-spring:$reactorVersion"

       // Reactor Net
       // compile "io.projectreactor:reactor-net:$reactorVersion"

       // Reactor Codecs (Jackson, Kryo...)
       // compile "io.projectreactor:reactor-codec:$reactorVersion"

       // Netty for Reactor Net (auto detects if in classpath)
       // compile 'io.netty:netty-all:4.0.27.Final'
    }


### Documentation

* [Guides](http://projectreactor.io/docs/)
* [API Reference](http://reactor.github.io/docs/api/)
* [Reactive Streams](http://www.reactive-streams.org/)

### Community / Support

* [reactor-framework Google Group](https://groups.google.com/forum/?#!forum/reactor-framework)
* [GitHub Issues](https://github.com/reactor/reactor/issues)

### License

Reactor is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
