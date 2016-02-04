# Reactor Project

[![Join the chat at https://gitter.im/reactor/reactor](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Starting from 2.5.0, Reactor is composed of the following modules:

# reactor-core 2.5.0.M1
Reactive foundations for apps and frameworks and lite reactive extensions API with [Mono](http://next.projectreactor.io/core/docs/api/reactor/core/publisher/Mono.html) (1 element) and [Flux](http://next.projectreactor.io/core/docs/api/reactor/core/publisher/Flux.html) (n elements) types

- https://github.com/reactor/reactor-core
- Documentation source : http://next.projectreactor.io/core/docs/reference
- API space : http://next.projectreactor.io/core/docs/api

# reactor-stream 2.5.0.M1
Fully featured reactive extension API with Promise and Stream types

- https://github.com/reactor/reactor-stream
- Documentation source : http://next.projectreactor.io/stream/docs/reference
- API space : http://next.projectreactor.io/stream/docs/api

# reactor-io 2.5.0.BUILD-SNAPSHPOT

- https://github.com/reactor/reactor-io:
- Documentation source : http://next.projectreactor.io/io/docs/reference
- API space : http://next.projectreactor.io/io/docs/api
- Sub-modules:
    - reactor-io : Memory and InterProcessCommunication abstractions
    - reactor-aeron : Efficient unicast/multicast messaging
    - reactor-codec: JSON, compression, Kryo, Protobuf codecs
    - reactor-net: TCP and HTTP client and server

# reactor-extensions 2.5.0.BUILD-SNAPSHPOT

- https://github.com/reactor/reactor-extensions
- Documentation source : http://next.projectreactor.io/ext/docs/reference
- API space : http://next.projectreactor.io/ext/docs/api
- Sub-modules:
    - reactor-alloc
    - reactor-bus
    - reactor-logback
    - reactor-pylon
    - reactor-pipes

# reactor-incubator 2.5.0.BUILD-SNAPSHPOT

- https://github.com/reactor/reactor-incubator
- Sub-modules:
    - reactor-amqp
    - reactor-chronicle
    - reactor-groovy
    - reactor-net-0mq

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

* [Guides](http://next.projectreactor.io/docs/)
* [Reactive Streams](http://www.reactive-streams.org/)

### Community / Support

* [reactor-framework Google Group](https://groups.google.com/forum/?#!forum/reactor-framework)
* [GitHub Issues](https://github.com/reactor/reactor/issues)

### License

Reactor is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
