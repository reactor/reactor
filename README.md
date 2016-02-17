# Reactor Project

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Starting from 2.5.0, Reactor is now organized into multiple projects:

# Reactor Core
[![Reactor Core](https://maven-badges.herokuapp.com/maven-central/io.projectreactor/reactor-core/badge.svg?style=plastic)](http://mvnrepository.com/artifact/io.projectreactor/reactor-core)

Reactive foundations for apps and frameworks and lite reactive extensions API with [Mono](http://projectreactor.io/core/docs/api/reactor/core/publisher/Mono.html) (1 element) and [Flux](http://projectreactor.io/core/docs/api/reactor/core/publisher/Flux.html) (n elements) types

- https://github.com/reactor/reactor-core
- Documentation source : http://projectreactor.io/core/docs/reference
- API space : http://projectreactor.io/core/docs/api

# Reactor Stream
_2.5.0.BUILD-SNAPSHOT_

Fully featured reactive extension API with Promise and Stream types

- https://github.com/reactor/reactor-stream
- Documentation source : http://projectreactor.io/stream/docs/reference
- API space : http://projectreactor.io/stream/docs/api

# Reactor IO
_2.5.0.BUILD-SNAPSHPOT_

- https://github.com/reactor/reactor-io:
- Documentation source : http://projectreactor.io/io/docs/reference
- API space : http://projectreactor.io/io/docs/api
- Sub-modules:
    - reactor-io : Memory and InterProcessCommunication abstractions
    - reactor-aeron : Efficient unicast/multicast messaging
    - reactor-codec: JSON, compression, Kryo, Protobuf codecs
    - reactor-net: TCP and HTTP client and server

# Reactor Addons
_2.5.0.BUILD-SNAPSHPOT_

- https://github.com/reactor/reactor-addons
- Documentation source : http://projectreactor.io/ext/docs/reference
- API space : http://projectreactor.io/ext/docs/api
- Sub-modules:
    - reactor-alloc
    - reactor-bus
    - reactor-logback
    - reactor-pylon
    - reactor-pipes

# Reactor Incubator
_2.5.0.BUILD-SNAPSHPOT_

- https://github.com/reactor/reactor-incubator
- Sub-modules:
    - reactor-amqp
    - reactor-chronicle
    - reactor-groovy
    - reactor-net-0mq

# Reactive Streams Commons
In a continuous mission to design the most efficient concurrency operators for Reactive Streams, a common effort -codename [Reactive Streams Commons](https://github.com/reactor/reactive-streams-commons)- has begun. Reactor is fully aligned with _RSC_ design and is directly inlining _RSC_ within its stable API contract scoped under reactor-core (Rx Lite) and reactor-stream (full rx). Reactive Streams Commons is a research effort shared with everyone and is demanding of efficient stream processing challengers, therefore it is naturally decoupled of any framework noise. 

### Enrolling

[Join the initiative](https://support.springsource.com/spring_committer_signup), fork, discuss and PR anytime. Roadmap is collaborative and we do enjoy new ideas, simplifications, doc, feedback, and, did we mention feedback already ;) ? As any other open source project, you are the hero, Reactor is only useful because of you and we can't wait to see your pull request mate !

### Maven Artifacts

Fresh snapshot and release artifacts are provided in the _repo.spring.io_ repositories. 
Stable Release are synchronozied with Maven Central. To add this repo to your Gradle build, specify the URL like the following:

```groovy

    repositories {
      //maven { url 'http://repo.spring.io/libs-release' }
      //maven { url 'http://repo.spring.io/libs-milestone' }
      maven { url 'http://repo.spring.io/libs-snapshot' }
      mavenCentral()
    }

    dependencies {
      // Reactor Core
      compile "io.projectreactor:reactor-core:2.5.0.BUILD-SNAPSHOT"

      // Reactor Stream
      // compile "io.projectreactor:reactor-stream:2.5.0.BUILD-SNAPSHOT"

      // Reactor Aeron
      // compile "io.projectreactor:reactor-aeron:2.5.0.BUILD-SNAPSHOT"

       // Reactor Pipes
      // compile "io.projectreactor:reactor-pipes:2.5.0.BUILD-SNAPSHOT"

       // Reactor Netty4
       // compile "io.projectreactor:reactor-net:2.5.0.BUILD-SNAPSHOT"

       // Netty for Reactor Net (auto detects if in classpath)
       // compile 'io.netty:netty-all:4.0.34.Final'

       // Reactor Codecs (Jackson, Kryo...)
       // compile "io.projectreactor:reactor-codec:2.5.0.BUILD-SNAPSHOT"

    }
```

### Documentation

* [Guides](http://projectreactor.io/docs/)
* [Reactive Streams](http://www.reactive-streams.org/)

### Community / Support

* [reactor-framework Google Group](https://groups.google.com/forum/?#!forum/reactor-framework)
* [GitHub Issues](https://github.com/reactor/reactor/issues)

### License

Reactor is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
