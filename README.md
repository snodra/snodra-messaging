# kraken-messaging

Kotlin library for service-to-service messaging in the kraken platform. It provides the Axon-shaped `EventBus`,
`CommandBus` and `QueryBus` facades with `@EventHandler` / `@CommandHandler` / `@QueryHandler` annotations. Events travel
over Kafka with a MongoDB transactional outbox and an idempotent consumer; commands and queries travel over a gRPC stream
to the kraken hub. It ships as a Spring Boot 4.1 starter plus a test module.

Design and wire protocol: `../kraken/docs/02-messaging.md`. Test strategy: `../kraken/docs/09-observability-and-testing.md`.
Repo conventions: `../kraken/docs/04-repositories.md`.

## Modules

| Module | Coordinates | Purpose |
|---|---|---|
| `kraken-messaging-api` | `io.damoreira.kraken:kraken-messaging-api:0.1.0-SNAPSHOT` | `@MessageType`, `Event`, `Command<R>`, `Query<R>`. No Spring. Depended on by every `*-api` module. |
| `kraken-messaging` | `io.damoreira.kraken:kraken-messaging:0.1.0-SNAPSHOT` | Spring Boot starter: buses, handler annotations, hub client, outbox, Kafka consumer, auto-configuration. |
| `kraken-messaging-test` | `io.damoreira.kraken:kraken-messaging-test:0.1.0-SNAPSHOT` | `InMemoryBuses`, `InProcessHub`, `KrakenContainers`, `KrakenTestKafka`. |

## Requirements

- JDK 25 (Temurin), found on the PATH or by the Gradle toolchain resolver.
- Docker for the container tests (Kafka, MongoDB, and the hub image for the end-to-end test).

Rancher Desktop: Testcontainers needs two environment variables, otherwise it does not find the Docker socket.

```bash
export DOCKER_HOST=unix://$HOME/.rd/docker.sock
export TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock
```

or the equivalent lines in `~/.testcontainers.properties`:

```properties
docker.host=unix:///Users/<you>/.rd/docker.sock
testcontainers.docker.socket.override=/var/run/docker.sock
```

## Build, test, publish

```bash
./gradlew build                  # compile + all tests (needs Docker, see above)
./gradlew publishToMavenLocal    # installs 0.1.0-SNAPSHOT into ~/.m2 for the service repos
```

Publishing to the GitLab Package Registry happens in CI on tags matching `api/vX.Y.Z`: the `publish-api` job sets
`KRAKEN_GITLAB_MAVEN_URL` and authenticates with `CI_JOB_TOKEN`, then publishes all three modules with the tag's version.
Locally, set `KRAKEN_GITLAB_MAVEN_URL` and `KRAKEN_GITLAB_TOKEN` and run `./gradlew publish`.

The published POM and Gradle module metadata carry resolved versions for every dependency (Gradle `versionMapping`), and the
POM also imports the Spring Boot BOM, so consumers do not need to manage them. Builder call.

## Using the starter

### Dependency

```kotlin
// settings.gradle.kts / build.gradle.kts
repositories {
    mavenLocal()   // until the GitLab registry is wired into the service repos
    mavenCentral()
}

dependencies {
    implementation("io.damoreira.kraken:kraken-messaging:0.1.0-SNAPSHOT")
    // a service's *-api module depends on the api artifact only:
    // api("io.damoreira.kraken:kraken-messaging-api:0.1.0-SNAPSHOT")
    testImplementation("io.damoreira.kraken:kraken-messaging-test:0.1.0-SNAPSHOT")
}
```

Kotlin version note (Builder call): a project that applies `io.spring.dependency-management` must set the Kotlin version
in its build script, otherwise the Boot BOM pins the Kotlin compiler classpath to 2.3.x and `compileKotlin` fails with a
`getPluginClasspaths()` NullPointerException.

```kotlin
extra["kotlin.version"] = "2.4.21"   // the Kotlin version in your version catalog
```

### Configuration

```yaml
kraken:
  messaging:
    service-name: workouts          # Kafka consumer group, event source, hub registration
    type-prefix: workouts           # wire type prefix for classes without @MessageType
    hub:
      address: static://kraken-hub:7000
    kafka:
      bootstrap-servers: broker:19092
spring:
  mongodb:
    uri: mongodb://mongo:27017/workouts   # Spring Boot 4 key; spring.data.mongodb.uri is no longer bound
  security:
    oauth2:
      resourceserver:
        jwt:
          issuer-uri: https://keycloak.example/realms/kraken
```

For services or tools without JWTs set `kraken.messaging.security.require-jwt: false`.

All keys (`kraken.messaging.*`; durations accept `200ms`, `5s`, `7d`; relaxed binding, e.g. `KRAKEN_MESSAGING_HUB_ADDRESS`):

| Key | Default | Meaning |
|---|---|---|
| `service-name` | required | Kafka consumer group, event `source`, hub `Register.service_name`. |
| `type-prefix` | required | Prefix of the wire type name for message classes without `@MessageType`. |
| `hub.enabled` | `true` | Connect to the hub and expose the hub-backed `CommandBus`/`QueryBus` (addition beyond the spec table). |
| `hub.address` | `static://kraken-hub:7000` | gRPC target; an optional `static://` prefix is stripped. |
| `hub.request-timeout` | `5s` | Default per-request timeout, sent as `timeout_ms`. |
| `hub.heartbeat-interval` | `10s` | Heartbeat period on the hub stream. |
| `hub.reconnect-backoff.min` | `500ms` | Initial reconnect delay when the stream drops. |
| `hub.reconnect-backoff.max` | `30s` | Maximum reconnect delay (exponential). |
| `kafka.bootstrap-servers` | `broker:19092` | Kafka bootstrap servers. |
| `kafka.topic` | `kraken.events` | Events topic. |
| `kafka.consumer.enabled` | `true` | Set `false` in tools that only publish. |
| `kafka.consumer.max-attempts` | `5` | Handler invocations per record (first try included) before the record is logged and skipped. |
| `outbox.enabled` | `true` | Run the outbox publisher that relays `outbox` documents to Kafka. |
| `outbox.poll-interval` | `200ms` | Outbox poll period. |
| `outbox.batch-size` | `100` | Documents per poll. |
| `outbox.retention` | `7d` | Sent outbox documents older than this are deleted (TTL index). |
| `idempotency.retention` | `30d` | `processed_events` documents older than this are deleted (TTL index). |
| `security.forward-jwt` | `true` | Put the current request's bearer token in the hub request's `authorization` header. |
| `security.require-jwt` | `true` | Reject forwarded requests without a valid JWT; requires a `JwtDecoder` bean. |

### Handlers

```kotlin
@Component
class WorkoutHandlers(private val repo: WorkoutRepository, private val events: EventBus) {

    @CommandHandler
    @Transactional
    suspend fun handle(cmd: LogWorkout, ctx: MessageContext): LogWorkoutResult {
        val workout = repo.save(Workout.from(cmd, ownerId = ctx.userId))
        events.publish(WorkoutLogged(workout.id, workout.ownerId, workout.loggedAt), key = workout.id)
        return LogWorkoutResult(workout.id)
    }

    @QueryHandler
    suspend fun handle(q: GetWorkout): GetWorkoutResult =
        repo.findById(q.workoutId)?.toResult() ?: throw NotFoundException(q.workoutId)

    @EventHandler
    suspend fun on(e: FileDeleted) {
        repo.detachFile(e.fileId)
    }
}
```

Messages are plain data classes implementing the marker interfaces from the api module:

```kotlin
@MessageType("workouts.LogWorkout")
data class LogWorkout(val name: String) : Command<LogWorkoutResult>
```

### Sending

```kotlin
val result = commandBus.send(LogWorkout("legs"))                 // suspend
val result = commandBus.sendBlocking(LogWorkout("legs"))         // blocking, for MVC controllers
val workout = queryBus.send(GetWorkout(id))
eventBus.publish(WorkoutLogged(id, owner, at), key = id)         // inside the caller's Mongo transaction
```

## Semantics worth knowing

- **Typed facades.** `Command<R>` and `Query<R>` carry the response type; it is read from the interface type argument at
  runtime. Generic responses such as `List<X>` are erased, so wrap them in a response class. Builder call.
- **Local short-circuit.** A command or query whose handler lives in the same service is invoked locally and does not go
  through the hub.
- **Request ids.** `Request.id` is fresh per attempt. A retried command must carry its own business id in the payload.
- **JWT.** The bearer token is forwarded on the hub path only. The receiving service validates it with its own
  `JwtDecoder`; `MessageContext.userId` and `roles` come from the validated token. Role authorisation is left to the
  handlers. Builder call.
- **Events** carry `userId` only, not the token.
- **Outbox ordering.** Events with the same key are published in order per publishing instance, using a `seq` field on the
  outbox document. Builder call.
- **Consumer idempotency.** The consumer records processed event ids in `processed_events` and skips duplicates. A handler
  gets up to `max-attempts` invocations with a 1 s back-off; a record that still fails is logged and skipped. There is no
  dead-letter topic.
- **Transactions.** Event handlers run inside the Mongo transaction on the Kafka listener thread. A handler that relies on
  the transaction must not switch threads.
- **Hub errors.** The hub answers malformed or duplicate in-flight requests with `INVALID_REQUEST`; other hub errors map to
  the exception types in `io.damoreira.kraken.messaging` (`NoHandlerException`, `UnauthenticatedException`, ...).

## Test helpers (`kraken-messaging-test`)

- `InMemoryBuses(typePrefix, userId)`: implements `EventBus`, `CommandBus` and `QueryBus` in memory for unit tests without
  Docker. Messages still pass through the JSON codec. `register(handlerBean)` adds handlers; `published` lists events.
- `InProcessHub`: in-JVM stand-in for the hub (`start()`, `channel()`, `dropInstance(...)`, `lastRequestHeaders()`), for
  hub-client tests without a container.
- `KrakenContainers`: unstarted Testcontainers definitions. `kafka()`, `mongo()` (single-node replica set, needed for
  transactions), `hub()` from image `kraken-hub:dev` or the one named by `KRAKEN_HUB_IMAGE`, and `hubImageAvailable()`.
- `KrakenTestKafka(bootstrap, typePrefix, topic).awaitEvent<T>()`: reads an event back from the topic.

```kotlin
val buses = InMemoryBuses(typePrefix = "workouts")
buses.register(WorkoutHandlers(repo, buses))   // the handler publishes events through the same in-memory bus
val result = buses.sendBlocking(LogWorkout("legs"))
```

The end-to-end test (`TwoServicesEndToEndTest`) starts Kafka, MongoDB, the real hub and two Spring Boot services. It is
skipped when the hub image is absent. Build the image from the kraken-hub repo:

```bash
docker build -t kraken-hub:dev .
```

## Known limitations

- No dead-letter topic: poison records are logged and skipped.
- No leader election for the outbox publisher; with several instances delivery is at-least-once and ordering is per
  publishing instance.
- No scatter-gather (one command or query reaches one handler instance).
- Library metrics are not exported yet; OpenTelemetry comes from the application.
- The `require-jwt` check lives in the core auto-configuration, so Kafka-only tools must set `require-jwt: false` or
  provide a `JwtDecoder`.
- Configuration metadata is hand-maintained in `META-INF/spring-configuration-metadata.json`.
- `TraceHeaders` is only active with Micrometer Tracing on the classpath.
