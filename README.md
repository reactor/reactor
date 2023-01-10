# Reactor Project

[![Join the chat at https://gitter.im/reactor/reactor](	https://img.shields.io/gitter/room/reactor/reactor.svg)](https://gitter.im/reactor/reactor?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

 [![Download](https://img.shields.io/maven-central/v/io.projectreactor/reactor-bom.svg) ](https://img.shields.io/maven-central/v/io.projectreactor/reactor-bom.svg)

Starting from 3.0, Reactor is now organized into multiple projects:
 - [`reactor-core`](https://github.com/reactor/reactor-core/)
 - [`reactor-netty`](https://github.com/reactor/reactor-netty/)
 - addons like [`reactor-extra`](https://github.com/reactor/reactor-addons/) or [`reactor-pool`](https://github.com/reactor/reactor-pool/)
 - other more community-driven integrations like [`reactor-kafka`](https://github.com/reactor/reactor-kafka/) and [`reactor-rabbitmq`](https://github.com/reactor/reactor-rabbitmq)

A set of compatible versions for all these projects is curated under a BOM ("Bill of Materials") hosted under this very repository.

## Using the BOM with Maven
In Maven, you need to import the bom first:

```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-bom</artifactId>
            <version>2022.0.2</version>
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
        <groupId>io.projectreactor</groupId>
        <artifactId>reactor-test</artifactId>
        <scope>test</scope>
    </dependency>
</dependencies>
```

## Using the BOM with Gradle
### Gradle 5.0+
Use the `platform` keyword to import the Maven BOM within the `dependencies` block, then add dependencies to
your project without a version number.

```groovy
dependencies {
     // import BOM
     implementation platform('io.projectreactor:reactor-bom:2022.0.2')

     // add dependencies without a version number
     implementation 'io.projectreactor:reactor-core'
}
```

### Gradle 4.x and earlier
Gradle versions prior to 5.0 have no core support for Maven BOMs, but you can use Spring's [`gradle-dependency-management` plugin](https://github.com/spring-gradle-plugins/dependency-management-plugin).

First, apply the plugin from Gradle Plugin Portal (check and change the version if a new one has been released):

```groovy
plugins {
    id "io.spring.dependency-management" version "1.0.11.RELEASE"
}
```
Then use it to import the BOM:

```groovy
dependencyManagement {
     imports {
          mavenBom "io.projectreactor:reactor-bom:2022.0.2"
     }
}
```

Then add a dependency to your project without a version number:

```groovy
dependencies {
     compile 'io.projectreactor:reactor-core'
}
```


## BOM Versioning Scheme
The BOM can be imported in Maven, which will provide a set of default artifact versions to use whenever the corresponding dependency is added to a pom without an explicitly provided version.

As the different artifacts versions are not necessarily aligned, the BOM represents a release train with an heterogeneous range of versions that are curated to work together.
The artifact version follows the `YYYY.MINOR.MICRO-QUALIFIER` scheme since Europium, where:

 * `YYYY` is the year of the first GA release in a given release cycle (like 3.4.0 for 3.4.x)
 * `.MINOR` is a 0-based number incrementing with each new release cycle
 ** in the case of the BOM it allows discerning between release cycles in case two get first released the same year
 * `.PATCH` is a 0-based number incrementing with each service release
 * `-QUALIFIER` is a textual qualifier, which is omitted in the case of GA releases (see below)
 
On top of the artifact version, each release train has an associated codename, a chemical name from the [Periodic Table of Elements](https://en.wikipedia.org/wiki/List_of_chemical_elements) in growing alphabetical order, for reference in discussions.

So far, the release trains code names are:
 - `Aluminium` for the `3.0.x` generation of Reactor-Core ([:bulb:](# 'aluminium is shiny, as is this brand new release'))
 - `Bismuth` for the `3.1.x` generation ([:bulb:](# 'intricate crystaline structure, a bit like this release'))
 - `Californium` for the `3.2.x` generation ([:bulb:](# 'made in California, can be used to help start up nuclear reactors... shoutout to our own @smaldini moving there'))
 - `Dysprosium` for the `3.3.x` generation ([:bulb:](# 'means hard to get and is used in nuclear reactors'))
 - `Europium` (`2020.0`) for the `3.4.x` generation ([:bulb:](# 'a large part of the team is now based in Europe'))

NOTE: Up until Dysprosium, the BOM was versioned using a release train scheme with a codename followed by a qualifier, and the qualifiers were slightly different.
For example: Aluminium-RELEASE (first GA release, would now be something like YYYY.0.0), Bismuth-M1, Californium-SR1 (service release
would now be something like YYYY.0.1), Dysprosium-RC1, Dysprosium-BUILD-SNAPSHOT (after each patch, we'd go back to the same snapshot version. would now be something
like YYYY.0.X-SNAPSHOT so we get 1 snapshot per PATCH).
 

# Contributing, Community / Support

[![license](https://img.shields.io/github/license/reactor/.github.svg?label=Reactor%20is)](https://github.com/reactor/.github/blob/main/LICENSE)

As hinted above, this repository is for hosting the BOM and for transverse issues only. Most of the time, if you're looking to open an issue or a PR, it should be done in a more specific repository corresponding to one of the actual artifacts.

All projects follow the same detailed contributing guidelines which you can find [here](https://github.com/reactor/.github/blob/main/CONTRIBUTING.md).

This document also give some ways you can get answers to your [questions](https://github.com/reactor/.github/blob/main/CONTRIBUTING.md#question-do-you-have-a-question).

### Documentation

* [Guides](https://projectreactor.io/docs)
* [Reactive Streams](https://www.reactive-streams.org/)

# Detail of Projects
## Reactor Core
[![Reactor Core](https://img.shields.io/badge/github-reactor/reactor--core-green.svg)](https://github.com/reactor/reactor-core)

Reactive foundations for apps and frameworks and reactive extensions inspired API with [Mono](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Mono.html) (1 element) and [Flux](https://projectreactor.io/docs/core/release/api/reactor/core/publisher/Flux.html) (n elements) types

 - API documentation: [/docs/core/release/api](https://projectreactor.io/docs/core/release/api)

## Reactor Netty
[![Reactor Netty](https://img.shields.io/badge/github-reactor/reactor--netty-green.svg)](https://github.com/reactor/reactor-netty)

TCP and HTTP client and server.

 - API documentation: [/docs/netty/release/api](https://projectreactor.io/docs/netty/release/api)

## Reactor Addons
[![Reactor Addons](https://img.shields.io/badge/github-reactor/reactor--addons-green.svg)](https://github.com/reactor/reactor-addons)

Extra projects adding features to reactor:

  - **`reactor-adapter`**: adapt to/from various libraries, mainly RxJava 2.
    - API documentation: [/docs/adapter/release/api](https://projectreactor.io/docs/adapter/release/api)
  - **`reactor-extra`**: Retry utils, Math utils, ...
    - API documentation: [/docs/extra/release/api](https://projectreactor.io/docs/extra/release/api)

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
</repositories>
```

To add it to your Gradle build, use the `repositories` configuration like this:
```groovy
repositories {
	maven { url 'https://repo.spring.io/libs-snapshot' }
	mavenCentral()
}
```

You should then be able to import a `-SNAPSHOT` version of the BOM, like `2020.0.{NUMBER}-SNAPSHOT` for the `snapshot` of the `{NUMBER}th service release` of `2020.0` (Europium).

_Sponsored by [VMware](https://tanzu.vmware.com)_
