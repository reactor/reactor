# Reactor Releases Manifests

This orphan branch of the BOM repository contains properties files that
represent manifests for release trains.

Each `.properties` represents a release train, with an ordered set of versions
that are to be included into the release.

The [`releaser` tool](https://github.com/spring-cloud/spring-cloud-release-tools/)
from the Spring Cloud team can use these manifests to drive a release:
 - check each participating repository for the corresponding version
 - skip if it already is at that version
 - bump the project's version and its relevant dependencies that are part of this train
 - release the project
 - go to the next project in the manifest and repeat...

Each file has to be named according to the Release Train codename and version, lowercased and with underscores. For example:

 - `californium_sr4.properties`
 - `dysprosium_release.properties`

The content of the files follows the `map` convention of Spring Boot properties and uses repository names as keys:

```properties
releaser.fixed-versions[reactor-core]=3.3.1.RELEASE
releaser.fixed-versions[reactor-addons]=3.3.1.RELEASE
```

:warning: This branch is protected, and no external PRs will be accepted on this branch.