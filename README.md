<p align="center">
  <img alt="Snodra Messaging mascot" src="docs/assets/brand/svg/snodra-messaging.svg" width="160">
</p>

<h1 align="center">Snodra Messaging (Kotlin)</h1>

<p align="center">
  Commands, queries, events, sagas and event processors for Spring Boot services, routed by Snodra Server and stored in Kafka, RabbitMQ, MongoDB or SQL.
</p>

<p align="center">
  <a href="https://github.com/snodra/snodra-messaging/actions/workflows/ci.yml"><img alt="ci" src="https://github.com/snodra/snodra-messaging/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://central.sonatype.com/artifact/com.damoreira.snodra/snodra-messaging"><img alt="maven central" src="https://img.shields.io/maven-central/v/com.damoreira.snodra/snodra-messaging"></a>
  <img alt="Kotlin" src="https://img.shields.io/badge/Kotlin-2.4.21-7F52FF?logo=kotlin&logoColor=white">
  <img alt="Spring Boot" src="https://img.shields.io/badge/Spring_Boot-4.1.2-6DB33F?logo=springboot&logoColor=white">
  <a href="https://snodra.damoreira.com/docs/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-Apache--2.0-blue"></a>
</p>

---

## What it is

One Spring Boot starter that gives a service everything it needs to take part in a Snodra system, and
nothing it does not: the buses, the handlers, the stores and the tests.

- **Commands and queries** through Snodra Server, with deadlines, retries and streaming queries.
- **Events** published through the outbox of a message store, never by a second write.
- **Event processors** that are idempotent by construction: processed-event markers in the same store.
- **Sagas** with associations, deadlines and a relay that retries commands.
- **Stores are add-ons**: Kafka or RabbitMQ Streams for events, MongoDB or SQL (PostgreSQL, MySQL) for state.
- **Testing kits** for event stores, message stores and handlers, with Testcontainers.

## Install

```kotlin
val snodraVersion = "X.Y.Z" // the latest release: see the Maven Central badge above

dependencies {
    implementation("com.damoreira.snodra:snodra-messaging:$snodraVersion")
    implementation("com.damoreira.snodra:snodra-messaging-kafka:$snodraVersion")   // or -rabbitmq
    implementation("com.damoreira.snodra:snodra-messaging-mongo:$snodraVersion")   // or -jdbc
    testImplementation("com.damoreira.snodra:snodra-messaging-test:$snodraVersion")
}
```

Everything is on Maven Central; no repository configuration beyond `mavenCentral()`. Every module of a
release has the same version.

## A handler

```kotlin
@Component
class PlaceOrder(private val events: EventBus) {
    @CommandHandler
    suspend fun handle(command: PlaceOrderCommand) {
        events.publish(OrderPlaced(command.orderId))
    }
}
```

The starter finds it, registers the type on the server and runs it. Settings are Spring keys under
`snodra.messaging.*` with `SNODRA_*` environment fallbacks.

## Documentation

Quick start, the settings map, every add-on and the testing kits: **<https://snodra.damoreira.com/docs/messaging/kotlin/>**.
Other languages: <https://snodra.damoreira.com/>. Services written with any Snodra library speak the
same protocol (`snodra.hub.v1`) and share the event records and store layouts.

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow.

## Releases

CI cuts every version from the commit types. Never push a tag. When every check passes on a push to
`main`, the commit subjects since the last `v*` tag decide:

- any `feat` is the next minor;
- otherwise any `fix` or `perf` is the next patch;
- anything else (`docs`, `test`, `chore`, `ci`, `refactor`, `build`, `style`) releases nothing.

CI then builds the Maven Central bundle with that version and checks it. It tags the tested commit
`vX.Y.Z` and publishes every module to Maven Central and to the GitHub release. No file holds the version:
the build takes it from the tag, so there is no release commit.

- Below 1.0.0, any `type!:` subject and any line starting `BREAKING CHANGE:` count as a minor, never a
  major. 1.0.0 is declared by hand.
- A revert (`Revert "feat: ..."`, `revert:`) is not a release reason. The reverted change stays in the last
  published version until the next `feat`, `fix` or `perf`.
- Only tags shaped `vN.N.N` count. The highest one reachable from `main` is the last release.
- `scripts/next-version.sh` holds the rule. `for t in scripts/*.test.sh; do sh "$t"; done` runs the tests
  of the release scripts.

A release needs every check green, the throughput floors included. If a check fails spuriously, use
**Re-run failed jobs**: the release follows when it passes.

If a release fails half way (the tag exists, Maven Central or the GitHub release does not have it), use
**Re-run failed jobs** on that run, or run the `release` workflow by hand with the existing tag. A version
Maven Central already shows is not uploaded again. After the upload itself succeeded, wait until the
version shows on Maven Central (it can take half an hour) before you re-run: until then a re-run uploads
it again, and Maven Central refuses it. The GitHub release stays a draft until the upload to Maven Central
succeeded. **Re-run all jobs** on a run that tagged does nothing, because its commit then carries the tag.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
