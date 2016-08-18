# Reactor Project

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Starting from 3.0, Reactor is now organized into multiple projects:

![Reactor Project](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/org3.png)

# Reactor Core
[![Reactor Core](https://maven-badges.herokuapp.com/maven-central/io.projectreactor/reactor-core/badge.svg?style=plastic)](http://mvnrepository.com/artifact/io.projectreactor/reactor-core)

Reactive foundations for apps and frameworks and reactive extensions inspired API with [Mono](http://projectreactor.io/core/docs/api/reactor/core/publisher/Mono.html) (1 element) and [Flux](http://projectreactor.io/core/docs/api/reactor/core/publisher/Flux.html) (n elements) types

- https://github.com/reactor/reactor-core
- Documentation source : http://projectreactor.io/core/docs/reference
- API space : http://projectreactor.io/core/docs/api

# Reactor IPC
_0.5.0.BUILD-SNAPSHPOT_

Memory and InterProcessCommunication abstractions.

- https://github.com/reactor/reactor-ipc:
- Documentation source : http://projectreactor.io/ipc/docs/reference
- API space : http://projectreactor.io/ipc/docs/api

# Reactor Netty :
_0.5.0.BUILD-SNAPSHPOT_

TCP and HTTP client and server

- https://github.com/reactor/reactor-ipc:
- Documentation source : http://projectreactor.io/netty/docs/reference
- API space : http://projectreactor.io/netty/docs/api

# Reactor Addons
_3.0.0.BUILD-SNAPSHPOT_

- https://github.com/reactor/reactor-addons
- Documentation source : http://projectreactor.io/ext/docs/reference
- API space : http://projectreactor.io/ext/docs/api
- Sub-modules:
    - reactor-bus
    - reactor-codec: JSON, compression, Kryo, Protobuf codecs
    - reactor-logback

# Reactor Incubator
_3.0.0.BUILD-SNAPSHPOT_

- https://github.com/reactor/reactor-incubator
- Sub-modules:
    - reactor-amqp
    - reactor-groovy

# Reactive Streams Commons
In a continuous mission to design the most efficient concurrency operators for Reactive Streams, a common effort -codename [Reactive Streams Commons](https://github.com/reactor/reactive-streams-commons)- has begun. Reactor is fully aligned with _RSC_ design and is directly inlining _RSC_ within its stable API contract scoped under reactor-core. Reactive Streams Commons is a research effort shared with everyone and is demanding of efficient stream processing challengers, therefore it is naturally decoupled of any framework noise.

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
      compile "io.projectreactor:reactor-core:3.0.0.BUILD-SNAPSHOT"

      // Reactor Aeron
      // compile "io.projectreactor.ipc:reactor-aeron:0.5.0.BUILD-SNAPSHOT"

       // Reactor Netty4
       // compile "io.projectreactor.ipc:reactor-netty:0.5.0.BUILD-SNAPSHOT"

       // Reactor Codecs (Jackson, Kryo...)
       // compile "io.projectreactor.ipc:reactor-codec:0.5.0.BUILD-SNAPSHOT"

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
