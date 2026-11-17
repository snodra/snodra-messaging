<p align="center">
  <img alt="Snodra mascot" src="docs/assets/brand/svg/snodra.svg" width="160">
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
  <a href="https://snodra.damoreira.com/"><img alt="docs" src="https://img.shields.io/badge/docs-snodra.damoreira.com-0B5FFF"></a>
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
dependencies {
    implementation("com.damoreira.snodra:snodra-messaging:0.5.0")
    implementation("com.damoreira.snodra:snodra-messaging-kafka:0.5.0")   // or -rabbitmq
    implementation("com.damoreira.snodra:snodra-messaging-mongo:0.5.0")   // or -jdbc
    testImplementation("com.damoreira.snodra:snodra-messaging-test:0.5.0")
}
```

Everything is on Maven Central; no repository configuration beyond `mavenCentral()`.

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

Quick start, the settings map, every add-on and the testing kits: **<https://snodra.damoreira.com/messaging/kotlin/>**.
The Rust counterpart is [`snodra-messaging-rs`](https://github.com/snodra/snodra-messaging-rs); both speak the
same protocol (`snodra.hub.v1`) and share the event records and store layouts.

## Contributing

Branches off `main`, conventional commits, a pull request with a green workflow.

## License

[Apache-2.0](LICENSE) © 2026 Guilherme D'Amoreira.
