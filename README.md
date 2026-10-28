# kraken-messaging

Kotlin messaging library for the kraken services: the Axon-shaped `EventBus`, `CommandBus` and
`QueryBus` facades, the `@EventHandler` / `@CommandHandler` / `@QueryHandler` annotations, Kafka
event transport with a MongoDB outbox, and the gRPC client for the kraken hub.

Design: `../kraken/docs/02-messaging.md`. Repo conventions: `../kraken/docs/04-repositories.md`.

**Status: skeleton, no business logic.** Interfaces, annotations, configuration properties and a
stub auto-configuration exist; every bus method throws `NotImplementedError("skeleton")`.

## Modules

| Module | Published as | Purpose |
|---|---|---|
| `kraken-messaging-api` | `io.damoreira.kraken:kraken-messaging-api` | `@MessageType`, `Event`, `Command<R>`, `Query<R>`. No Spring. Depended on by every `*-api` module. |
| `kraken-messaging` | `io.damoreira.kraken:kraken-messaging` | Spring Boot 4.1 starter with the facades, handler annotations and transports. |
| `kraken-messaging-test` | `io.damoreira.kraken:kraken-messaging-test` | Testcontainers 2.0.5 helpers (Kafka, MongoDB, hub, RustFS). |

## Build

Requires JDK 25 (Temurin) on the PATH or discoverable by the Gradle toolchain resolver.

```bash
./gradlew build
./gradlew publishToMavenLocal          # makes 0.1.0-SNAPSHOT available to the service repos
```

Publishing to the GitLab Package Registry happens in CI on `api/vX.Y.Z` tags
(`KRAKEN_GITLAB_MAVEN_URL`, `CI_JOB_TOKEN`); locally set `KRAKEN_GITLAB_TOKEN`.

Builder call: the wrapper files were taken from the Gradle 9.8.2 release tag and have not been
executed on this machine. The first `./gradlew` run downloads the distribution.
