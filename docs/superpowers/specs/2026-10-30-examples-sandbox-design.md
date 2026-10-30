# Examples sandbox: two Spring Boot services through the hub

**Date:** 2026-10-30
**Status:** draft, awaiting approval
**Repos touched:** `kraken-messaging` (examples modules, Dockerfile, README), `kraken` umbrella (`infra/compose.local.yaml`, `docs/07-local-development.md`)

## Intent

Guilherme wants to see cross-service commands, queries and events actually flow through the real `kraken-hub`
image and Kafka, driven by hand with `curl`, before any real service exists. Two tiny Spring Boot apps built on the
`kraken-messaging` starter are the vehicle:

1. service A sends command `SendGreeting` over the hub;
2. service B handles it, publishes event `GreetingDelivered`;
3. service A reacts to `GreetingDelivered`;
4. service A sends query `CountGreetings` over the hub; service B answers it.

Everything runs in Docker Compose from locally built images. The sandbox is disposable test tooling, not a
product; it stays in the starter repo so it always compiles against the current library and cannot rot.

## Goals

- One command brings up hub, Kafka, MongoDB and the two apps from local builds: no registry, no Keycloak, no
  RustFS, no Grafana.
- Each of the four steps above is observable: `curl` output on A plus one clear `INFO` log line per step in A and
  B, and both services visible on the hub's `GET /topology`.
- The failure cases of the hub are reachable by hand: stop B and the next command yields `NO_HANDLER`
  at A (HTTP 503 with the error code in the body).
- Service A, which owns no command or query, still appears on the topology: the hub lists every registered
  instance (the registry builds the view from its instances map, with empty type lists for A). Verified in
  `kraken-hub/src/registry.rs` while writing this.
- `./gradlew build` in `kraken-messaging` compiles and unit-tests the examples, so a starter change that breaks them
  is caught.

## Non-goals

- No domain persistence: B keeps its greeting counts in memory and loses them on restart. MongoDB is present only
  because the starter's outbox publisher and idempotent consumer require it; the examples store nothing of their own
  there.
- No authentication: all calls are anonymous, no `@RequiresAuthentication`, no Keycloak, no tokens.
- Not published to the GitLab Package Registry or container registry; no CI job.
- No Rust participant (`kraken-messaging-rs` is out of scope).

## Anti-goals

- The examples must not become a third place where library behaviour is specified: they call the public starter
  API only, exactly as a real service would. If something is awkward to do from the examples, that is a finding
  against the starter, to be fixed there.
- The sandbox must not depend on anything outside the workspace (`~/.projects/kraken-workspace/`).

## Decisions taken with Guilherme

| Area | Decision |
|---|---|
| Location | Gradle modules inside the `kraken-messaging` repo: `examples/examples-api`, `examples/service-a`, `examples/service-b`. |
| Trigger | REST endpoints on service A, driven with `curl`. |
| Runtime | Both apps run in Compose, built locally. One Dockerfile for both apps. |
| Persistence | None in the apps; MongoDB stays as hidden infrastructure for the starter's outbox and consumer. |
| Security | Anonymous only. |
| Domain | A tiny "greetings" domain (below). |
| Ports | Any; see builder calls. |
| Compose | Override file for local builds next to the umbrella `compose.yaml`; the main file keeps registry image names. |

## The domain and the wire types (`examples/examples-api`)

Plain Kotlin data classes, every one annotated with `@MessageType` so the wire name does not depend on either
service's `type-prefix`:

| Class | Kind | Wire type | Fields |
|---|---|---|---|
| `SendGreeting` | `Command<GreetingAccepted>` | `examples.SendGreeting` | `name: String` |
| `GreetingAccepted` | reply | n/a | `id: String`, `name: String` |
| `GreetingDelivered` | `Event` | `examples.GreetingDelivered` | `id: String`, `name: String`, `at: Instant` |
| `CountGreetings` | `Query<GreetingCount>` | `examples.CountGreetings` | `name: String` |
| `GreetingCount` | reply | n/a | `name: String`, `count: Long` |

## Service B (`examples/service-b`), the owner

- `@CommandHandler suspend fun handle(cmd: SendGreeting, ctx: MessageContext): GreetingAccepted`: generates
  `id = UUID.randomUUID().toString()`, increments the in-memory count for `cmd.name`, publishes
  `GreetingDelivered(id, name, Instant.now())` with `key = cmd.name`, logs
  `B: handled SendGreeting name=<name> id=<id> correlationId=<ctx.correlationId>`, returns `GreetingAccepted`.
- `@QueryHandler fun handle(q: CountGreetings): GreetingCount` from the same map (0 when unknown), logs
  `B: answered CountGreetings name=<name> count=<n>`.
- No HTTP endpoints of its own, but the app includes `spring-boot-starter-webmvc` and `spring-boot-starter-actuator`
  so the JVM stays alive the normal Boot way and `GET /actuator/health` exists.

## Service A (`examples/service-a`), the caller

- `POST /greetings` with body `{"name": "Ada"}` sends `SendGreeting("Ada")` via `CommandBus.send`, returns
  `200 {"id": "...", "name": "Ada"}`, logs `A: sent SendGreeting name=Ada -> accepted id=<id>`.
- `GET /greetings/{name}/count` sends `CountGreetings(name)` via `QueryBus.send`, returns
  `200 {"name": "Ada", "count": 3}`, logs `A: asked CountGreetings name=Ada -> count=3`.
- `@EventHandler fun on(event: GreetingDelivered, ctx: MessageContext)` logs
  `A: received GreetingDelivered id=<id> name=<name> at=<at>` and appends the event to an in-memory list.
- `GET /greetings/delivered` returns that list as JSON, so the event leg can be checked from a script without
  reading logs.
- Error mapping in one `@RestControllerAdvice`: `NoHandlerException` and `HandlerDisconnectedException` → 503,
  `MessageTimeoutException` → 504, any other `MessagingException` → 502; body
  `{"error": "<exception simple name>", "message": "<message>"}` (class names as in the starter's
  `MessagingExceptions.kt`).
- Controllers are `suspend fun` (Spring MVC supports Kotlin coroutines) and call the suspend bus methods.

## Configuration (both apps, `application.yaml`)

```yaml
kraken.messaging:
  service-name: ${KRAKEN_SERVICE_NAME}        # service-a | service-b
  type-prefix: ${KRAKEN_SERVICE_NAME}
  hub.address: ${KRAKEN_HUB_ADDRESS:static://localhost:7002}
  kafka.bootstrap-servers: ${KRAKEN_KAFKA_BOOTSTRAP:localhost:9092}
spring.mongodb.uri: ${KRAKEN_MONGODB_URI:mongodb://localhost:27017/${KRAKEN_SERVICE_NAME}?replicaSet=rs0}
server.port: ${SERVER_PORT:8080}
logging.level.io.damoreira.kraken: INFO
```

The defaults point at the published ports of the umbrella compose so either app can also be started from the IDE
against the compose infrastructure (hub gRPC on host 7002 because macOS AirPlay owns 7000; see builder calls).

## Build

- `settings.gradle.kts` includes `examples:examples-api`, `examples:service-a`, `examples:service-b`.
- Service modules apply `kotlin.jvm`, `kotlin.spring`, `spring.boot`, `spring.dependency.management`, pin
  `extra["kotlin.version"]` (known quirk), depend on `project(":kraken-messaging")`,
  `project(":examples:examples-api")`, `spring-boot-starter-webmvc`, `spring-boot-starter-actuator`,
  `jackson-module-kotlin`; test deps `spring-boot-starter-test`, `kotlin-test-junit5`,
  `project(":kraken-messaging-test")`.
- The api module applies `kotlin.jvm` and `java-library`, depends on `project(":kraken-messaging-api")`.
- No `maven-publish` on any example module.
- One `examples/Dockerfile` with `ARG SERVICE`, context = the `kraken-messaging` repo root:
  stage 1 `eclipse-temurin:25-jdk` runs `./gradlew :examples:${SERVICE}:bootJar --no-daemon` with a BuildKit cache
  mount on `/root/.gradle`; stage 2 `eclipse-temurin:25-jre` copies the boot jar to `/app/app.jar`, `EXPOSE 8080`,
  `ENTRYPOINT ["java", "-jar", "/app/app.jar"]`. A `.dockerignore` at the repo root excludes `build/`, `.gradle/`,
  `.kotlin/`, `.worktrees/`, `.superpowers/`, `.idea/`, `docs/`.

## Compose (`kraken/infra/compose.local.yaml`)

Override merged with `compose.yaml`:

- `hub`: `image: kraken-hub:dev`, `build: { context: ../../kraken-hub }`, `profiles: [examples]` (merged with the
  base file's `[apps]`, so the hub is active under either profile) and `ports: !override ["7002:7000", "7001:7001"]`
  (Compose appends port lists on merge; without `!override` the base `7000:7000` would stay and collide with
  AirPlay on macOS).
- `service-a`, `service-b`: `profiles: [examples]`, `build: { context: ../../kraken-messaging, dockerfile:
  examples/Dockerfile, args: { SERVICE: service-a | service-b } }`, `image: kraken-examples-service-a:dev` /
  `-b:dev`, env `KRAKEN_SERVICE_NAME`, `KRAKEN_HUB_ADDRESS=static://hub:7000`,
  `KRAKEN_KAFKA_BOOTSTRAP=kafka:19092`, `KRAKEN_MONGODB_URI=mongodb://mongodb:27017/<name>?replicaSet=rs0`,
  `SERVER_PORT=8080`; ports `8091:8080` (A) and `8092:8080` (B); `depends_on`: `hub` healthy, `mongodb` healthy,
  `kafka-init` completed successfully; `networks: [kraken]`.
- Start: `docker compose -f compose.yaml -f compose.local.yaml --profile examples up --build service-a service-b`.
  Naming the two services starts only their dependency closure (hub, kafka, kafka-init, mongodb).

## Verification the builder must deliver

- Unit tests per module using `InMemoryBuses` from `kraken-messaging-test` (no Docker): B's command handler
  increments and publishes; B's query answers 0 then n; A's controller returns the reply JSON and maps
  `NoHandlerException` to 503; A's event handler records the event.
- `examples/smoke.sh` (bash, curl only): waits for `http://localhost:8091/actuator/health`, posts two greetings for
  `Ada`, polls `GET /greetings/delivered` until both ids appear (timeout 30 s), asserts
  `GET /greetings/Ada/count` returns `"count":2`, prints the hub topology from `http://localhost:7001/topology`,
  exits non-zero on any mismatch.
- `examples/README.md`: the compose command, the four curl calls with expected output, the expected log lines, the
  `NO_HANDLER` experiment (stop B, post, expect 503), teardown.
- Umbrella `docs/07-local-development.md` gains a short section pointing at the examples README and explaining the
  override file; `kraken-messaging/README.md` gains an "Examples" section with the same pointer.

## Builder calls (my decisions, flagged)

- Ports 8091 (A) and 8092 (B) on the host, 8080 inside; hub gRPC published on host 7002 in the override as the
  umbrella docs already do for macOS, HTTP topology stays on 7001.
- B gets a web server and actuator even without endpoints, for a conventional long-running Boot process and a
  health URL.
- `GET /greetings/delivered` exists on A so the smoke script can prove the event leg without parsing logs.
- Image names `kraken-examples-service-a:dev` and `kraken-examples-service-b:dev`.
- The Dockerfile builds inside Docker (slow first build, cached afterwards) rather than copying a host-built jar,
  so `docker compose up --build` is the only command needed.
- `GreetingDelivered` is published from inside B's command handler (no outbox transaction around domain state,
  since there is none); it still goes through the outbox collection and the publisher like any event.

## Left to the builder

- Exact exception class names for the error mapping (read `MessagingExceptions.kt`).
- Log format details beyond the prefixes above.
- Whether `kafka-init` must be listed explicitly in `depends_on` or is already pulled in transitively by `kafka`
  (it is not: `kafka-init` depends on `kafka`, nothing depends on `kafka-init`; list it).
- Gradle wrapper handling inside the Dockerfile (copy `gradlew` + `gradle/wrapper`).
