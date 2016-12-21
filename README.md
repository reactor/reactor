# Reactor Project

[![Join the chat at https://gitter.im/reactor/reactor](	https://img.shields.io/gitter/room/reactor/reactor.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

<a title="Currently not released">![](https://img.shields.io/badge/current%20release%20train-Aluminium-orange.svg)</a><br/>
![](https://img.shields.io/badge/current%20stable%20BOM%20%20-n%2Fa-lightgrey.svg)

Starting from 3.0, Reactor is now organized into multiple projects:

![Reactor Project](https://raw.githubusercontent.com/reactor/projectreactor.io/master/src/main/static/assets/img/org3.png)

A set of compatible versions for all these projects is curated under a BOM ("Bill of Material").

## Current Release Train

The first release train to follow the new and shiny BOM model will be `Aluminium-RELEASE`.

### Using the BOM with Maven
In Maven, you need to import the bom first:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-releasetrain</artifactId>
            <version>Aluminium</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>
```
Notice we use the `<dependencyManagement>` section and the `import` scope.

Next, add your dependencies to the relevant reactor projects as usual, except without a `<version>`:

```xml
<dependencies>
    <dependency>
        <groupId>io.projectreactor</groupId>
        <artifactId>reactor-core</artifactId>
    </dependency>
    <dependency>
        <groupId>io.projectreactor.addons</groupId>
        <artifactId>reactor-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

### Using the BOM with Gradle
Gradle has no core support for Maven BOMs, but you can use Spring's [`gradle-dependency-management` plugin](https://github.com/spring-gradle-plugins/dependency-management-plugin).

First, apply the plugin from Gradle Plugin Portal (check and change the version if a new one has been released):

```groovy
plugins {
    id "io.spring.dependency-management" version "1.0.0.RC1"
}
```

Then use it to import the BOM:

```groovy
dependencyManagement {
     dependencies {
          dependency 'io.projectreactor:reactor-releasetrain:Aluminium'
     }
}
```

Then add a dependency to your project without a version number:

```groovy
dependencies {
     compile 'io.projectreactor:reactor-core'
}
```


### BOM Versionning Scheme
The BOM can be imported in Maven, which will provide a set of default artifact versions to use whenever the corresponding dependency is added to a pom without an explicitly provided version.

As the different artifacts versions are not necessarily aligned, the BOM represents a release train with a codename-based versioning scheme: the usual MAJOR.MINOR numbers are replaced by a chemical name from the [Periodic Table of Elements](https://en.wikipedia.org/wiki/List_of_chemical_elements), in growing alphabetical order.

The first stable release is simply suffixed with `-RELEASE`, but the equivalent of patch releases are also possible as "Service Releases", appending the suffix `-SR` followed by the number of the service release (eg. `-SR1`, `-SR2`)

# Enrolling

[Join the initiative](https://support.springsource.com/spring_committer_signup), fork, discuss and PR anytime. Roadmap is collaborative and we do enjoy new ideas, simplifications, doc, feedback, and, did we mention feedback already ;) ? As any other open source project, you are the hero, Reactor is only useful because of you and we can't wait to see your pull request mate !

[![GitHub forks](https://img.shields.io/github/forks/reactor/reactor-core.svg?style=social&label=Fork)](https://github.com/reactor/reactor-core/issues#fork-destination-box)
[![license](https://img.shields.io/github/license/reactor/reactor-core.svg?label=Reactor%20is)](https://github.com/reactor/reactor-core/blob/master/LICENSE)

### Documentation

* [Guides](http://projectreactor.io/docs/)
* [Reactive Streams](http://www.reactive-streams.org/)

### Community / Support
* [![Join the chat at https://gitter.im/reactor/reactor](	https://img.shields.io/gitter/room/reactor/reactor.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)
* [![GitHub issues](https://img.shields.io/github/issues/reactor/reactor-core.svg)](https://github.com/reactor/reactor-core/issues)
* [![Twitter URL](https://img.shields.io/twitter/url/http/projectreactor.svg?style=social&label=@projectreactor)](https://twitter.com/projectreactor)

# Detail of Projects
## Reactor Core
[![Reactor Core](https://img.shields.io/badge/github-reactor/reactor--core-green.svg)](https://github.com/reactor/reactor-core)

Reactive foundations for apps and frameworks and reactive extensions inspired API with [Mono](http://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html) (1 element) and [Flux](http://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html) (n elements) types

 - API documentation: [/docs/core/release/api](http://projectreactor.io/docs/core/release/api)

## Reactor IPC
[![Reactor Core](https://img.shields.io/badge/github-reactor/reactor--ipc-green.svg)](https://github.com/reactor/reactor-ipc)

Memory and InterProcessCommunication abstractions.

 - API documentation: [/docs/ipc/release/api](http://projectreactor.io/docs/ipc/release/api)

## Reactor Netty
[![Reactor Netty](https://img.shields.io/badge/github-reactor/reactor--netty-green.svg)](https://github.com/reactor/reactor-netty)

TCP and HTTP client and server.

 - API documentation: [/docs/netty/release/api](http://projectreactor.io/docs/netty/release/api)

## Reactor Addons
[![Reactor Addons](https://img.shields.io/badge/github-reactor/reactor--addons-green.svg)](https://github.com/reactor/reactor-addons)

Extra projects adding features to reactor:

  - **`reactor-adapter`**: adapt to/from various libraries, mainly RxJava 2.
    - API documentation: [/docs/adapter/release/api](http://projectreactor.io/docs/adapter/release/api)
  - **`reactor-test`**: test `Flux`, `Mono` and other `Publisher`...
    - API documentation: [/docs/test/release/api](http://projectreactor.io/docs/test/release/api)
  - **`reactor-logback`**: `logback` adapter for Flux/Mono `log()` feature.


### Snapshot Artifacts

While Stable Releases are synchronized with Maven Central, fresh snapshot and milestone artifacts are provided in the _repo.spring.io_ repositories.

To add this repo to your Maven build, add it to the `<repositories>` section like the following:

```xml
<repositories>
	<repository>
	    <id>spring-snapshot</id>
	    <name>Spring Snapshot Repository</name>
	    <url>https://repo.spring.io/snapshot</url>
	    <snapshots>
	        <enabled>true</enabled>
	    </snapshots>
	</repository>
<repositories>
```

To add it to your Gradle build, use the `repositories` configuration like this:
```groovy
repositories {
	maven { url 'http://repo.spring.io/libs-snapshot' }
	mavenCentral()
}
```

You should then be able to import a `BUILD-SNAPSHOT` version of the BOM, like `Aluminium.BUILD-SNAPSHOT`.

# Reactive Streams Commons
In a continuous mission to design the most efficient concurrency operators for Reactive Streams, a common effort -codename [Reactive Streams Commons](https://github.com/reactor/reactive-streams-commons)- has begun. Reactor is fully aligned with _RSC_ design and is directly inlining _RSC_ within its stable API contract scoped under reactor-core. Reactive Streams Commons is a research effort shared with everyone and is demanding of efficient stream processing challengers, therefore it is naturally decoupled of any framework noise.
