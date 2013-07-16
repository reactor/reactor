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
      compile 'org.projectreactor:reactor-core:$reactorVersion'
      // Reactor Groovy
      compile 'org.projectreactor:reactor-groovy:$reactorVersion'
      // Reactor Spring
      compile 'org.projectreactor:reactor-spring:$reactorVersion'
    }

When Reactor is released as a milestone or release, the artifacts will be generally available in Maven Central. Until then, you'll want to use the above snapshot repository for access to the artifacts.

### Community / Support

* [reactor-framework Google Group](https://groups.google.com/forum/?#!forum/reactor-framework)
* [GitHub Issues](https://github.com/reactor/reactor/issues)
* [Reference Documentation](wiki/Home)

---

Reactor is [Apache 2.0 licensed](http://www.apache.org/licenses/LICENSE-2.0.html).
