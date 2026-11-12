<p align="center">
  <img alt="kraken messaging mascot" src="https://gitlab.damoreira.ml/kraken-org/kraken/-/raw/main/docs/assets/brand/svg/kraken-messaging.svg" width="160">
</p>

<h1 align="center">kraken-messaging</h1>

<p align="center">
  Axon-shaped messaging for Kotlin services: event, command and query buses over Kafka and the kraken hub, as a Spring Boot starter.
</p>

<p align="center">
  <a href="https://gitlab.damoreira.ml/kraken-org/kraken-messaging/-/pipelines"><img alt="pipeline" src="https://gitlab.damoreira.ml/kraken-org/kraken-messaging/badges/main/pipeline.svg"></a>
  <a href="https://gitlab.damoreira.ml/kraken-org/kraken-messaging/-/releases"><img alt="release" src="https://gitlab.damoreira.ml/kraken-org/kraken-messaging/-/badges/release.svg"></a>
  <a href="https://gitlab.damoreira.ml/kraken-org/kraken-messaging/-/packages"><img alt="maven" src="https://img.shields.io/badge/maven-io.damoreira.kraken-C71A36?logo=apachemaven&logoColor=white"></a>
  <img alt="Kotlin" src="https://img.shields.io/badge/Kotlin-2.4.21-7F52FF?logo=kotlin&logoColor=white">
  <img alt="Spring Boot" src="https://img.shields.io/badge/Spring_Boot-4.1.2-6DB33F?logo=springboot&logoColor=white">
  <img alt="Kafka" src="https://img.shields.io/badge/Kafka-4.4-231F20?logo=apachekafka&logoColor=white">
  <img alt="gRPC" src="https://img.shields.io/badge/gRPC-1.85-244c5a?logo=grpc&logoColor=white">
  <a href="https://gitlab.damoreira.ml/kraken-org/kraken/-/tree/main/docs"><img alt="kraken docs" src="https://img.shields.io/badge/kraken-umbrella%20docs-0B5FFF"></a>
  <a href="LICENSE"><img alt="license" src="https://img.shields.io/badge/license-MIT-green"></a>
</p>

---

Kotlin library for service-to-service messaging in the kraken platform. It provides the Axon-shaped `EventBus`,
`CommandBus` and `QueryBus` facades with `@EventHandler` / `@CommandHandler` / `@QueryHandler` annotations. Events travel
over Kafka with a MongoDB transactional outbox and an idempotent consumer; commands and queries travel over a gRPC stream
to the kraken hub. It ships as a Spring Boot 4.1 starter plus a test module.

Design and wire protocol: `../kraken/docs/02-messaging.md`. Test strategy: `../kraken/docs/09-observability-and-testing.md`.
Repo conventions: `../kraken/docs/04-repositories.md`.

## Table of contents

- [Modules](#modules)
- [Requirements](#requirements)
- [Build, test, publish](#build-test-publish)
- [Using the starter](#using-the-starter)
- [Semantics worth knowing](#semantics-worth-knowing)
- [Multiple hubs](#multiple-hubs)
- [Scatter-gather](#scatter-gather)
- [Streaming queries](#streaming-queries)
- [Subscription queries](#subscription-queries)
- [Event processors](#event-processors)
- [Dead letters](#dead-letters)
- [Processor control](#processor-control)
- [Sagas](#sagas)
- [Metrics](#metrics)
- [Test helpers (`kraken-messaging-test`)](#test-helpers-kraken-messaging-test)
- [Examples](#examples)
- [Known limitations](#known-limitations)
- [CI/CD pipeline](#cicd-pipeline)
- [Contributing](#contributing)
- [License](#license)

## Modules

| Module | Coordinates | Purpose |
|---|---|---|
| `kraken-messaging-api` | `io.damoreira.kraken:kraken-messaging-api:0.1.0-SNAPSHOT` | `@MessageType`, `Event`, `Command<R>`, `Query<R>`, `StreamingQuery<R>`. No Spring. Depended on by every `*-api` module. |
| `kraken-messaging` | `io.damoreira.kraken:kraken-messaging:0.1.0-SNAPSHOT` | Spring Boot starter: buses, handler annotations, hub client, outbox, Kafka consumer, sagas, deadlines and the external-integration toolkit, auto-configuration. |
| `kraken-messaging-test` | `io.damoreira.kraken:kraken-messaging-test:0.1.0-SNAPSHOT` | `InMemoryBuses`, `InProcessHub`, `KrakenContainers`, `KrakenTestKafka`, `SagaTestFixture`. |
| `examples/examples-api`, `examples/service-a`, `examples/service-b` | not published | Two sample services talking through the hub; see [Examples](#examples). |

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

The `issuer-uri` is only needed when the service also serves HTTP or wants forwarded tokens verified (see
[Security](#security)); Kafka-only tools and services behind the hub start without any security configuration.

All keys (`kraken.messaging.*`; durations accept `200ms`, `5s`, `7d`; relaxed binding, e.g. `KRAKEN_MESSAGING_HUB_ADDRESS`):

| Key | Default | Meaning |
|---|---|---|
| `service-name` | required | Kafka consumer group, event `source`, hub `Register.service_name`. |
| `type-prefix` | required | Prefix of the wire type name for message classes without `@MessageType`. |
| `instance-id` | a fresh UUID per start | This instance's id: its hub `Register.instance_id` and the `service.instance.id` of its metrics ([Metrics](#metrics)). Set it to something stable per instance (a pod name) if you like; unique among the service's instances. |
| `hub.enabled` | `true` | Connect to the hub and expose the hub-backed `CommandBus`/`QueryBus` (addition beyond the spec table). |
| `hub.address` | `static://kraken-hub:7000` | The hub or hubs: one gRPC target (`host:port`, optional `static://` prefix), a comma-separated list of them (`hub-1:7000,hub-2:7000`, one connection per hub), or one `dns:///name:port` whose addresses are re-resolved every 30 s. See [Multiple hubs](#multiple-hubs). A malformed value fails the startup. |
| `hub.status-interval` | `5s` | Period of the `InstanceStatus` report (connected hub ids, handlers in flight) sent to every live hub; also sent whenever the set of live hubs changes. Must be positive. |
| `hub.request-timeout` | unset | Per-request timeout sent as `timeout_ms`. Unset: the hub applies its default (5 s) or its per-type override (`KRAKEN_HUB_TIMEOUT_OVERRIDES`). Also bounds the wait for a hub stream (5 s when unset). `0s` counts as unset. |
| `hub.reconnect-backoff.min` | `500ms` | Initial reconnect delay when the stream drops; doubles per failed attempt. Must be positive and not above `max`, or the startup fails naming the property. |
| `hub.reconnect-backoff.max` | `5s` | Maximum reconnect delay. Every wait is jittered by ±20 % and capped at this value (at the cap: 80-100 % of it); the delay resets to `min` once an attempt reached the hub's handshake. Must be positive (a zero would hot-loop handshakes against a refusing hub): checked at binding, a bad value fails the startup naming the property. |
| `hub.max-subscriptions` | `1024` | Subscription queries this instance serves at once, over every hub (at least 1). The next one is refused with `RESOURCE_EXHAUSTED` ("subscription limit (N) reached"), which the subscriber sees as a `HandlerBusyException`. A subscription holds no handler permit, so `max-concurrent-handlers` does not bound it; the default equals the hub's own `KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE`. |
| `hub.stream-credit-timeout` | `10m` | The longest a streaming handler of this instance waits for credit from the collector before it ends the stream with `TIMEOUT` ("no credit within ...") and frees its handler slot and hub permit (a hub that forgot the stream would otherwise leave them held forever). Must be positive; the default is the hub's default maximum stream duration (`KRAKEN_HUB_MAX_STREAM_DURATION`). |
| `hub.token` | none | Static token sent in `Register.credentials.token` of **every** hub stream (hub access control, [Hub security](#hub-security-token-acl-and-tls)). Optional; an empty or blank value is the same as none. Never logged, redacted in `toString` and in the actuator's `configprops` and `env`. Environment: `KRAKEN_MESSAGING_HUB_TOKEN`, or `KRAKEN_HUB_TOKEN` when this key is unset. |
| `hub.token-file` | none | File holding the token (a container secret such as `/run/secrets/kraken-hub-token`; surrounding whitespace ignored). Read at startup (an unreadable file fails it, naming the file) and **again on every reconnect**, so a rotated secret is picked up without a restart. After a first non-empty read, an unreadable or empty file (a non-atomic rotation caught half way) keeps the last token read and logs one `WARN`; at startup an empty file means no token. **Wins over `hub.token` when both are set.** Environment: `KRAKEN_MESSAGING_HUB_TOKEN_FILE`, or `KRAKEN_HUB_TOKEN_FILE` when this key is unset. |
| `hub.tls.enabled` | `false` | Talk TLS to the hub, for every `hub.address` form (a list of targets and `dns:///`). Off: plain text. The hub must serve TLS on that port (`KRAKEN_HUB_TLS_CERT` / `KRAKEN_HUB_TLS_KEY`). |
| `hub.tls.ca-file` | JVM system roots | PEM file with the CA certificate(s) that signed the hub's certificate (a self-signed hub certificate is its own CA). Read on every connection attempt; a file that cannot be used fails the startup. Set without `hub.tls.enabled` it is ignored with a warning. |
| `hub.tls.server-name` | the address's host | Name to verify the hub's certificate against, for when the address is an IP or an alias the certificate does not list. |
| `hub.oauth2.token-uri` | none | Setting it turns Keycloak client-credentials authentication on (P5b, [JWT](#authenticating-with-a-jwt-keycloak-client-credentials)): the token endpoint, for example `https://keycloak/realms/kraken/protocol/openid-connect/token`. `http` is allowed and warns once unless the host is loopback or a single-label name. Cannot be combined with `hub.token` or `hub.token-file` (the startup fails naming both). Environment: `KRAKEN_MESSAGING_HUB_OAUTH2_TOKEN_URI`, or `KRAKEN_HUB_OAUTH2_TOKEN_URI` when this key is unset. |
| `hub.oauth2.client-id` | `kraken.messaging.service-name` | The confidential client's id; it is the token's `azp`, which the hub requires to equal the service name. Environment: `KRAKEN_HUB_OAUTH2_CLIENT_ID`. |
| `hub.oauth2.client-secret` | none | The client secret (sent as HTTP Basic, `client_secret_basic`). Never logged, redacted in `toString` and in the actuator's `configprops` and `env`. Prefer the file. Environment: `KRAKEN_MESSAGING_HUB_OAUTH2_CLIENT_SECRET`, or `KRAKEN_HUB_OAUTH2_CLIENT_SECRET`. |
| `hub.oauth2.client-secret-file` | none | File holding the client secret: read at startup (an unreadable or empty file fails it, naming the file) and **again on every token fetch**, so a rotated secret applies without a restart. Wins over `client-secret`. Environment: `KRAKEN_MESSAGING_HUB_OAUTH2_CLIENT_SECRET_FILE`, or `KRAKEN_HUB_OAUTH2_CLIENT_SECRET_FILE`. A token URI with neither secret fails the startup. |
| `hub.oauth2.scope` | none | `scope` of the token request, sent as written (space-separated). Environment: `KRAKEN_HUB_OAUTH2_SCOPE`. |
| `hub.oauth2.ca-file` | platform roots | PEM CA certificate(s) trusted **on top of** the platform roots for an `https` token URI (not the hub's CA, which is `hub.tls.ca-file`). A file that cannot be used fails the startup. Environment: `KRAKEN_HUB_OAUTH2_CA`. |
| `hub.max-concurrent-handlers` | `256` | Incoming hub requests handled concurrently by this instance, over all hubs (at least 1, else the startup fails); further requests wait for a free slot. Also announced as `initial_permits` to a hub with flow control (split over N hubs: `ceil(max / N)` each), which then queues the excess on its side. |
| `hub.load-factor` | `100` | Relative capacity of this instance (`Register.load_factor`); weights its share of the hub's routing (keyed ring and load score). A negative value fails the startup; 0 is allowed (the hub reads it as 100). |
| `hub.tags.<name>` | none | Labels announced in `Register.tags`, for example `hub.tags.zone=eu-1`; unkeyed requests prefer instances whose tags match the sender's. |
| `hub.priorities.<type>` | none (0) | Default `Request.priority` per message type, for example `hub.priorities.orders.PlaceOrder=5`; higher is served first when the hub queues requests for a busy handler. `SendOptions.priority` wins. |
| `hub.retry.enabled` | `false` | Retry failed sends that are safe to retry (see "Retry" below). `SendOptions.retry` overrides per call. |
| `hub.retry.max-attempts` | `3` | Total attempts including the first (1 = no retry). The `hub.retry.*` values are validated at startup even while `enabled` is false: `max-attempts` < 1, `multiplier` < 1 or a negative backoff fails it. |
| `hub.retry.initial-backoff` | `100ms` | Wait before the second attempt. Every wait is jittered by ±20 % and capped at `max-backoff` (at the cap: 80-100 % of it). |
| `hub.retry.multiplier` | `2` | Growth of the wait per further attempt. |
| `hub.retry.max-backoff` | `2s` | Upper bound of a single wait. |
| `kafka.bootstrap-servers` | `broker:19092` | Kafka bootstrap servers. |
| `kafka.topic` | `kraken.events` | Events topic. |
| `kafka.consumer.enabled` | `true` | Set `false` in tools that only publish. |
| `kafka.group-id` | `service-name` | The service's consumer group: every event processor, `default` included, uses `<this>.<name>`. See [Event processors](#event-processors). |
| `kafka.consumer.max-attempts` | `5` | Handler invocations per record (first try included, 1 s apart) before the record goes to the dead letters; shared by every processor. |
| `kafka.dead-letter.mode` | `collection` | What a processor does with a record it cannot handle: `collection` (park it in `dead_letters`, sequence its key, `DeadLetters` admin API), `topic` (produce it to `<topic>.dlt`) or `log` (log and skip: nothing is parked). Case-insensitive; anything else fails the startup. See [Dead letters](#dead-letters). |
| `kafka.dead-letter.retention` | `30d` | A letter is deleted this long after it was parked (TTL index on `expireAt`), which also unblocks its key. At least `1s`. |
| `outbox.enabled` | `true` | Run the outbox publisher that relays `outbox` documents to Kafka. |
| `outbox.poll-interval` | `200ms` | Outbox poll period. |
| `outbox.batch-size` | `100` | Documents per poll. |
| `outbox.retention` | `7d` | Sent outbox documents older than this are deleted (TTL index). |
| `outbox.max-attempts` | `10` | Send attempts per outbox document before it is marked failed (`failedAt`) and skipped. |
| `idempotency.retention` | `30d` | `processed_events` documents older than this are deleted (TTL index). |
| `security.forward-jwt` | `true` | Put the current request's bearer token in the hub request's `authorization` header. |
| `sagas.enabled` | `true` | Switch for the saga runtime, the dispatch relay and the deadline scheduler. They exist only when the application has a `@Saga` class (found by scanning its packages) anyway. See [Sagas](#sagas). |
| `sagas.max-attempts` | `5` | Attempts per saga event (first try included) for **conflicts** (a version check, a start-key duplicate, a Mongo write conflict or transient transaction error) before the event is parked in `saga_failures`; also the bound for firing a deadline. Backoff 200 ms doubling to 5 s (not configurable). A **handler exception** is not cured by waiting: it gets two attempts in all, at most 50 ms apart, and the event is then parked. |
| `sagas.ended-retention` | `30d` | Ended sagas are deleted this long after they ended (TTL index on `sagas.endedAt`). Must be positive. |
| `sagas.dispatch-retention` | `7d` | Dispatched `saga_dispatch` entries are deleted this long after they were sent (TTL index on `dispatchedAt`). Must be positive. |
| `sagas.failed-retention` | `30d` | What failed for good is deleted this long after it failed: `saga_dispatch` entries given up on (TTL index on `failedAt`) and parked events in `saga_failures` (TTL index on `lastFailedAt`, which a retry that fails again moves on). Must be positive. |
| `sagas.relay.poll-interval` | `500ms` | How often the dispatch relay looks for entries that saga handlers recorded. Must be positive. |
| `sagas.relay.lease` | `30s` | How long a relay instance holds one saga's dispatch order before another instance may take over (a crashed instance's lease expires); renewed before each entry and, every third of the lease, while a send runs. Needs synchronised clocks (NTP) across instances. Must be positive. |
| `sagas.relay.max-attempts` | `10` | Attempts per command for transient failures (first try included, at least 1) before it becomes `SagaCommandFailed(EXHAUSTED)`. |
| `sagas.relay.initial-backoff` | `1s` | Wait before the second attempt of a command; doubles per attempt up to `max-backoff`. |
| `sagas.relay.max-backoff` | `5m` | Upper bound of one wait between attempts of a command. |
| `sagas.relay.send-timeout` | `1m` | How long a command send may take. One with no answer by then (a local adapter that hangs: a local call has no timeout of its own) is abandoned, its thread interrupted, and retried later with the same idempotency key as a transient failure. Adapters should still set their own I/O timeouts well below it. Must be positive. |
| `deadlines.poll-interval` | `1s` | How often the deadline scheduler looks for due deadlines. Must be positive. |
| `deadlines.lease` | `30s` | How long a claimed deadline stays reserved for the instance that claimed it; an expired claim lets it fire again (at least once). Must be positive. |
| `deadlines.retention` | `7d` | Fired and cancelled deadlines are deleted this long after (TTL index on `deadlines.finishedAt`). Must be positive. |
| `external.calls.enabled` | unset | Whether the idempotency interceptor and the `external_calls` indexes exist. Unset: they exist when a class in the application's packages has an `@IdempotentHandler` method. `true` when the adapter lives outside those packages, `false` to switch it off. |
| `external.calls.lease` | `5m` | How long an `IN_PROGRESS` external call may take before its outcome counts as unknown (a crash in the middle of the call). Must be positive. |
| `external.calls.retention` | `30d` | `external_calls` documents older than this are deleted (TTL index on `updatedAt`). Must be positive. |
| `external.kafka.sources.<name>.topic` | required | A partner's topic, consumed into local kraken events (never republished to `kraken.events`). Each source needs an `ExternalRecordTranslator` bean named `<name>`; a source without one fails the startup naming it. |
| `external.kafka.sources.<name>.bootstrap-servers` | `kafka.bootstrap-servers` | The partner's cluster. |
| `external.kafka.sources.<name>.group-id` | `<service-name>.external.<name>` | The source's consumer group. |
| `external.kafka.sources.<name>.properties.*` | none | Extra consumer properties (for example `security.protocol`); bootstrap servers, group id, deserializers and auto-commit are fixed by the library: an entry for one of those keys fails the startup, naming it. `auto.offset.reset` is `earliest` unless set here. Record retries use `kafka.consumer.max-attempts`. |

### Security

(The hub's own access control, the service token and TLS are in [Hub security](#hub-security-token-acl-and-tls) below.)

Cross-service commands and queries are service-to-service calls on a private network, so they are **trusted by
default**: a request with no token is anonymous and is still handled. A handler that needs an authenticated user
opts in:

```kotlin
@CommandHandler
@RequiresAuthentication(expression = "hasRole('kraken-user')")
suspend fun handle(cmd: LogWorkout, ctx: MessageContext): LogWorkoutResult = ...

@QueryHandler
@RequiresAuthentication   // isAuthenticated()
fun handle(q: GetWorkout, ctx: MessageContext): GetWorkoutResult = ...

@CommandHandler
fun handle(cmd: RecomputeStats) { ... }   // no annotation: any caller, including event handlers and jobs
```

The expression is Spring Security SpEL evaluated by the **receiving** library before the handler runs:
`isAuthenticated()`, `hasRole('x')`, `hasAnyRole(...)`, `hasAuthority('ROLE_x')`, `principal` (a `MessagePrincipal`
with `userId`, `roles`, `verified`), plus `#message` (the decoded command or query) and `#ctx` (the `MessageContext`),
e.g. `"hasRole('kraken-user') and #message.ownerId == #ctx.userId"`. Realm roles from the token's `realm_access.roles`
become `ROLE_<role>` authorities. A syntax error fails startup naming the handler; `@RequiresAuthentication` is not
allowed on `@EventHandler` (events carry no identity). Annotated handlers are listed at startup at `INFO`.

Outcomes, on the hub path and the local short-circuit alike:

| Caller | Result at the sender |
|---|---|
| no identity (no token, or an event handler / job) | `UnauthenticatedException` (wire code `UNAUTHENTICATED`) |
| identity present, expression false or fails to evaluate | `AccessDeniedException` (wire code `PERMISSION_DENIED`) |
| identity present, expression true | handler runs |

A handler that throws `AccessDeniedException` (this library's or Spring's) also yields `PERMISSION_DENIED`, so a
nested send denied deeper in a chain reaches the original caller as `AccessDeniedException`.

How the receiver resolves the forwarded identity (`ForwardedIdentityResolver`):

| Forwarded `authorization` header | `JwtDecoder` bean | Result |
|---|---|---|
| absent | any | anonymous; `MessageContext.userId = null`, request proceeds |
| `Bearer <jwt>`, valid | present | identity `{sub, realm roles}`, `verified = true` |
| `Bearer <jwt>`, invalid or expired | present | `UNAUTHENTICATED` (a forged or stale token is a bug or an attack) |
| `Bearer <jwt>`, signed | absent | claims read **without verification**, `verified = false`; warned once per process |
| `Bearer <jwt>`, unsigned (`alg: none`) or encrypted | absent | `UNAUTHENTICATED` (only signed tokens are read unverified) |
| malformed | any | `UNAUTHENTICATED` |

Builder call: an unverified identity counts as authenticated. The peer that forwarded it validated it at its own
ingress and is trusted by the network model; verification is for catching forged or expired tokens where the
service can, not for distrusting the peer. Without a `JwtDecoder` the checks are therefore advisory: any client on
the private network can mint a signed token with any claims. Use `principal.verified` in an expression when a
handler must insist on a verified identity (`"isAuthenticated() and principal.verified"`). On the local
short-circuit the current Spring `Authentication` is normalised the same way: `sub` and `realm_access.roles` are
read from the token (`JwtAuthenticationToken`, `BearerTokenAuthentication`), so `hasRole('x')` means the realm role
regardless of how the application maps authorities for its own REST endpoints.

Notes:

- `@RequiresAuthentication` is read from the handler method itself and is not inherited from an interface or
  superclass method.
- `permitAll()`, `denyAll()` and `isAnonymous()` are accepted syntax but meaningless here: an anonymous caller is
  rejected with `UNAUTHENTICATED` before any expression runs, so leave the annotation off a handler open to anyone.
- A string result such as `'true'` is not a grant; the expression must evaluate to the Boolean `true`.
- An expression that throws (for example `#message.owner.id` on a null `owner`) is a denial, not a handler error.

The resolved identity is installed in the `SecurityContext` while the handler runs, so `CallerContext` works inside
handlers and nested sends re-forward the same token. `kraken.messaging.security.require-jwt` was removed; setting it
fails startup with a hint.

### Hub security: token, ACL and TLS

Since P5a the hub can authenticate services and restrict what they register and send, and serve its gRPC port over TLS
(decision record: umbrella `docs/adr/0018-hub-access-control.md`, operator how-to: `docs/06-security.md`). All of it is opt-in
on the hub (`KRAKEN_HUB_AUTH_MODE=off|permissive|enforce`, default `off`); this section is the client's side. It is
unrelated to [Security](#security) above, which is about the **user's** forwarded JWT.

**The token.** Give each service its own random token (`openssl rand -hex 32`) and the same one in the hub's ACL file
(which holds only its SHA-256):

```yaml
kraken:
  messaging:
    hub:
      token: ${BILLING_HUB_TOKEN}            # or: token-file: /run/secrets/kraken-hub-token
```

The token goes in `Register.credentials.token` of every stream (read once per stream; the hub reads it from the first
`Register` only). It is never logged, `HubSettings.toString()` and `KrakenMessagingProperties.toString()` print `****`, and with
`spring-boot-actuator` on the classpath the starter contributes a `SanitizingFunction` that masks it (and
`KRAKEN_HUB_TOKEN`) in `/actuator/configprops` and `/actuator/env` even with `show-values=ALWAYS` and whatever
sanitization the application configured. An empty token is no token (a `Register` without credentials, which `off` and
`permissive` hubs accept). **`token-file` wins when both are set** (it is the source that can rotate); the
file is read at startup, where a missing file fails the startup, and on every reconnect, so replacing the secret and
letting the connection reconnect (or fail once) is a rotation. **An empty file after a first good read** (a rotation that
replaces the file non-atomically can be caught half way) is treated like an unreadable one: the last token read is kept
and one `WARN` is logged, so a `Register` never goes out without its token just because of a race. At startup an empty
file still means no token. A file that disappears later likewise keeps the last token read and logs one warning.

**What the hub does with it**, and what you see:

| Hub (`enforce`) | Stream or request | In the client |
|---|---|---|
| no token, an empty token, an unknown token (a JWT on a hub with no issuer: "jwt credentials are not configured") | the hub closes the stream with `UNAUTHENTICATED` | registration rejection |
| `service-name` the token does not grant, or a handled type outside its `handle` | the hub closes the stream with `PERMISSION_DENIED` (naming the type) | registration rejection |
| a `Register` naming the instance id of a live session of **another service** (`enforce` and `permissive`; `off` keeps replacing it) | the hub closes the stream with `PERMISSION_DENIED` (`instance id … belongs to another service`); the owner's session is untouched | registration rejection |
| a request whose type is outside `send` (unary, scatter-gather, streaming, subscription) | an immediate `Error{PERMISSION_DENIED}`, nothing routed | `AccessDeniedException`, **never retried** |
| a request after the token left the ACL | an immediate `Error{UNAUTHENTICATED}` | `UnauthenticatedException`, never retried |

A **registration rejection** (`ALREADY_EXISTS`, `INVALID_ARGUMENT`, `UNAUTHENTICATED`, `PERMISSION_DENIED`, the last also for an instance id that belongs to another service): the status is
in `HubConnection.lastRejection`, `awaitRegistered` throws `HubRegistrationRejectedException` naming the code and the hub's
message, the connection reconnects at the backoff cap (80-100 % of `hub.reconnect-backoff.max`, no tight loop), logs at
`ERROR` once per distinct message and then at `DEBUG`, and keeps retrying (a fixed ACL or token file heals it without a
restart; `lastRejection` clears when a stream completes its handshake). The service keeps running; the hub counts every attempt as a
violation. In `permissive` nothing is refused: the hub logs and counts what `enforce` would refuse (except an instance-id
takeover, below, which it refuses too).

**Instance ids belong to a service.** A `Register` that names the instance id of a live session of *another* service is
refused with `PERMISSION_DENIED` (`instance id … belongs to another service`) in `enforce` and `permissive`, and the owner's
session is left alone, so a service with a valid token cannot hijack another one's stream and its queued requests. A
reconnect of the *same* service still replaces its own older stream. Make instance ids unique per instance (the default is
random) and never reuse another service's.

**The back-off is validated.** `hub.reconnect-backoff.max` must be positive and `min` positive and not above `max`; a bad
value fails the startup naming the property (a zero `max` would hot-loop handshakes against a refusing hub).

**TLS.** `hub.tls.enabled=true` with `hub.tls.ca-file` (the CA that signed the hub's certificate; the JVM's trust store when
absent) and, if the address is not a name on the certificate, `hub.tls.server-name`. It applies to every address form.
Plain text against a TLS hub (or TLS against a plain-text one) never hangs: the stream fails at once as
`UNAVAILABLE (io exception)` or `INTERNAL (http2 exception)` and the first `WARN` line that applies (not
necessarily the very first attempt: a hub that was still starting is a plain refusal, which carries no hint) says to check
`kraken.messaging.hub.tls.enabled` and `tls.ca-file`; later attempts log it at `DEBUG`. A client certificate (mTLS, P5c) is
[its own section](#authenticating-with-a-client-certificate-mtls); `DefaultHubChannelFactory.customizeTls` stays a hook for a subclass.

**Turning it on.** `enforce` last: (1) run the hub with `KRAKEN_HUB_AUTH_MODE=permissive` and an ACL file; (2) give every service its
token (and the CA, if the hub serves TLS); (3) watch `kraken_hub_auth_violations_total{reason,service}` and the dashboard until
it stays flat; (4) switch the hub to `enforce`; (5) turn on TLS before tokens cross an untrusted network. A service
whose token is missing or wrong in `enforce` fails visibly (`HubRegistrationRejectedException`, an `ERROR` line) instead of
reconnecting in a tight loop.

### Authenticating with a JWT (Keycloak client credentials)

Since P5b a service can present a short-lived Keycloak access token instead of a static token (decision record: umbrella
`docs/adr/0019-hub-jwt-authentication.md`). The hub validates it against the realm's keys (`KRAKEN_HUB_JWT_ISSUER`, ...), requires
its `azp` to equal the service name and takes the rights from the `[[service]]` table of that name in its ACL file.

**Keycloak, in two lines.** One confidential client per service with *service accounts enabled and nothing else* (no standard
flow, no direct access grants), its **client id = the service name**; add an audience mapper and set the hub's
`KRAKEN_HUB_JWT_AUDIENCE`. Never give a `[[service]]` table to the gateway or frontend client.

```yaml
kraken:
  messaging:
    hub:
      oauth2:
        token-uri: https://keycloak.example/realms/kraken/protocol/openid-connect/token
        client-secret-file: /run/secrets/kraken-client-secret   # client-id defaults to kraken.messaging.service-name
```

The environment forms are `KRAKEN_HUB_OAUTH2_TOKEN_URI`, `_CLIENT_ID`, `_CLIENT_SECRET`, `_CLIENT_SECRET_FILE`, `_SCOPE` and `_CA`
(relaxed binding also gives `KRAKEN_MESSAGING_HUB_OAUTH2_*`); the properties win. A token URI together with a static `hub.token`
or `hub.token-file`, or without any secret, fails the startup. So does any `hub.oauth2.*` setting (or `KRAKEN_HUB_OAUTH2_*` variable)
without a token URI: a stray client id or secret would otherwise leave the service silently unauthenticated.

**How it behaves.**

- The `client_credentials` grant is a `POST` to the token URI (`application/x-www-form-urlencoded`, 10 s, answer capped at 1 MiB,
  redirects never followed), the client authenticated with HTTP Basic (id and secret form-urlencoded first, RFC 6749 2.3.1). The
  answer needs `access_token`; `expires_in` missing means 60 s (one `WARN`); a `token_type` other than `Bearer` is refused.
- **One token per process**, shared by every hub connection (N hubs, one fetch), fetched single-flight: connections that start
  together wait for the same request. Each new stream (connect, reconnect) gets a token before it sends `Register`.
- A cached token is used while more than its refresh margin is left; the provider **renews it in the background** at that
  point: margin = 20 % of the lifetime, at least 5 s, and at most half of it (a 6 s token is renewed after 3 s; the Rust library
  uses the same rule). A lifetime over 10 years (a huge `expires_in`) is cut to 10 years. The renewer looks at the wall clock at
  least every 5 s, so a clock that jumped ahead while the machine slept does not postpone the renewal. The renewal goes to every live
  stream (one that has its ack) as `ClientMessage.credentials`. The hub closes a JWT session at `exp` plus
  `KRAKEN_HUB_JWT_EXPIRY_GRACE` (30 s) without one.
- A renewal that fails keeps the current token and retries with a back-off from 1 s to 30 s (one `WARN` per distinct error).
- A stream the hub closes (or a `Register` it refuses) with `UNAUTHENTICATED` ("token expired", "unknown signing key",
  "credentials expired", a refused refresh) drops the cached token: the reconnect fetches a new one instead of presenting the
  same one until its margin runs out. `PERMISSION_DENIED` (wrong `azp`, no `[[service]]` entry) keeps it: a new token would not help
  and the identity provider is not asked again for every retry.
- **No token, no stream.** When the endpoint cannot give a token (401 `invalid_client`, down, TLS, an unusable answer) the stream is
  not opened: the attempt fails locally, `HubConnection.lastRejection` holds `UNAUTHENTICATED` and `awaitRegistered` throws
  `HubRegistrationRejectedException` ("no access token for ...: invalid_client: ..." with the endpoint's `error` and
  `error_description`, clipped to 200 characters), the connection retries with the normal reconnect back-off, logs `ERROR` once
  per distinct error and then `DEBUG`, and recovers by itself when the endpoint does. A hub that is not ready (`UNAVAILABLE`
  "jwks not loaded yet") is an ordinary reconnect.
- With a static `hub.token-file` the same message rotates the token without a reconnect: when the file's content changes the new
  token is sent as `ClientMessage.credentials` on every live stream (checked once per heartbeat).
- The client secret and the access tokens never appear in logs, exception messages, `toString()` or the actuator's `configprops`
  and `env` (the starter's `SanitizingFunction` masks `hub.oauth2.client-secret` and `KRAKEN_HUB_OAUTH2_CLIENT_SECRET`).

**What you can read.** `HubConnections.authStatus()` returns `kind` (`NONE`, `TOKEN`, `JWT`), the current token's `tokenExpiresAt`, the
`lastTokenError` (null once a fetch worked), `tokenFetches` by result and `credentialsRefreshesSent` (one process-wide counter: it never drops when a hub connection goes away): wire it into your own
health indicator. With Micrometer on the classpath the starter binds `kraken_hub_client_token_fetches_total{result}`
(`ok`, `http_error`, `oauth_error`, `io_error`, `bad_response`) and `kraken_hub_client_credentials_refreshes_sent_total` ([Metrics](#metrics)).

**Divergences from the Rust library** (same design, ecosystem differences): the Kotlin starter reports through
`HubConnections.authStatus()` and Micrometer counters where Rust has an accessor. A token URI host with an underscore in
its name is refused by the JDK's URI parser (the host would be null), so it fails the startup here; the Rust library accepts it.

### Authenticating with a client certificate (mTLS)

Since P5c the hub can take a **client certificate** as the service's credential (decision record: umbrella
`docs/adr/0020-hub-mutual-tls.md`, issuing certificates and the hub's settings: `docs/06-security.md`, "mTLS mode"). The TLS handshake
proves the service holds the key, the certificate names the service, and the rights are the hub's `[[service]]` table of that name, the
same table a JWT uses. No secret crosses the wire and no identity provider is involved.

```yaml
kraken:
  messaging:
    service-name: billing-service
    hub:
      tls:
        ca-file: /etc/kraken/hub-ca.pem                 # the CA that signed the hub's certificate (or the system roots)
        client-cert-file: /etc/kraken/tls/client.pem    # certificate chain, leaf first
        client-key-file: /etc/kraken/tls/client.key     # unencrypted PKCS#8 PEM
```

Environment names (relaxed binding): `KRAKEN_MESSAGING_HUB_TLS_CLIENT_CERT_FILE`, `KRAKEN_MESSAGING_HUB_TLS_CLIENT_KEY_FILE`.

- **Both files or neither.** One alone fails the startup naming the missing one. A client certificate **implies TLS**: `hub.tls.enabled`
  need not be set, and an explicit `hub.tls.enabled=false` next to the certificate settings fails the startup (the Rust library does the
  same with `KRAKEN_HUB_TLS_CLIENT_CERT`/`_KEY`).
- **One credential kind per service.** A client certificate together with a static token (`hub.token`, `hub.token-file`,
  `KRAKEN_HUB_TOKEN`, `KRAKEN_HUB_TOKEN_FILE`) or OAuth2 (`hub.oauth2.*`, `KRAKEN_HUB_OAUTH2_*`) fails the startup naming both: a stale
  token next to a good certificate would be refused by the hub. With a certificate the library sends no credentials at all.
- **The SAN rule.** The leaf must carry exactly one URI SAN `kraken://service/<name>` (the name is 1 to 128 characters of
  `[A-Za-z0-9._-]`; DNS, IP and other URI SANs are ignored; two such SANs, even equal ones, are refused as the hub refuses them), and
  `<name>` must be `kraken.messaging.service-name`. The hub would refuse anything else with `PERMISSION_DENIED` at `Register`; the
  starter says so at startup instead. Also checked at startup: both files exist, are not empty and parse, the key belongs to the
  certificate, and the certificate has not ended. The key must be **PKCS#8 PEM** (`-----BEGIN PRIVATE KEY-----`); RSA (`BEGIN RSA PRIVATE KEY`), SEC1 (`BEGIN EC PRIVATE KEY`)
  and encrypted keys are refused with the conversion hint (`openssl pkcs8 -topk8 -nocrypt`). The JDK reads RSA, EC and Ed25519 keys.
- **How to issue one.** Follow `docs/06-security.md`, "mTLS mode" (`openssl`, or cert-manager with `uris:
  [kraken://service/billing-service]` and `usages: [client auth]`). Issue short-lived certificates (hours to days): there is no
  revocation list, so the lifetime is the revocation story.
- **Failures at the handshake are transport errors.** With `KRAKEN_HUB_TLS_CLIENT_AUTH=required`, no certificate or one the hub does not
  trust fails the TLS handshake, which TLS 1.3 shows when the stream opens: `UNAVAILABLE` ("ssl exception"), **not** a registration
  rejection. The library retries with its normal back-off and logs one `WARN` that names the fingerprint and what to check (the hub's
  `KRAKEN_HUB_TLS_CLIENT_CA`, the certificate's end, `hub.tls.ca-file`). A certificate that names no or several services, or another
  service than `service-name`, is `PERMISSION_DENIED` at `Register`: a rejection (`HubRegistrationRejectedException`). A client that presents **no** certificate to a hub that requires one gets the same transport
  failure; its `WARN` says that a hub which requires client certificates does that to a client that presents none, and to set
  `hub.tls.client-cert-file` and `client-key-file`.

**Rotation without a restart.** The files are read again on every (re)connect and once per check tick (3 s, the hub's heartbeat period).
A new connection attempt whose files cannot be used presents the last good certificate instead, with one `WARN` per problem. When the pair changed and is valid (the same checks as at startup, not expired, already valid), the instance first waits a **random delay**, uniform in
[0, min(30 s, 10 % of what is left of the certificate its streams present)] (none when that certificate has ended), so the replicas of a service
that read the renewed file within the same tick do not all deregister at once; the files are read again after the wait and the roll goes to the
pair that is current then. Then the hub connections **roll over to it
one hub at a time**: (1) the hub is told this instance is stopping (a `Register` with no types, as at shutdown), so it routes new work
to other instances; (2) the library waits up to 10 s for the work in flight on that stream (requests it sent and has no answer for,
requests it is handling; **not** the streams and subscriptions this instance itself consumes, see below); (3) the stream is closed gracefully (half-close, the hub's end awaited for 2 s); (4) it reconnects at once
(no back-off) with the new pair and waits up to 30 s for it to register again; only then the next hub is rolled, in target order. Write
the key first and the certificate second, or both with one rename each: a pair that does not match (a half-written replacement) is not
adopted and the current connection stays; the problem is logged at `DEBUG` when first seen and at `ERROR` (once) when the next check finds
it again, and the next check adopts the pair when it is complete.

- *Failed requests.* Requests accepted before the roll finish on the old stream. A sender that picks a hub in the middle of its roll gets
  `NO_HANDLER` from it when this instance was the only owner there, so a unary send **now fails over to another live hub that has a handler**
  (every hub answering `NO_HANDLER` returns the answer at once). A hub that has another live hub leaves this instance's own routing at once; the only hub leaves it after its work in flight finished.
- *One hub: a gap.* With a single hub there is nobody to take over: from the hub being told until the new stream is registered, the
  instance has no stream and takes no work. That is up to the drain (10 s at most, usually much less: only requests that are really in
  flight are waited for) plus the reconnect. Its own sends wait for the new stream within the call's deadline; requests other services send to
  it meanwhile get `NO_HANDLER` from the hub (give such a service two instances, or two hubs). The random delay above keeps the replicas of a
  service from all being in that gap at the same moment.
- *Streams and subscriptions.* A streaming handler counts as work in flight (it is waited for, up to 10 s, then cut like any lost stream).
  A subscription this instance serves does not hold the roll: it ends with the old stream (the hub tells the subscriber
  `HANDLER_DISCONNECTED`), as at any reconnect. **The same holds on the consumer side:** streams (`queryBus.stream`) and subscriptions
  (`queryBus.subscribe`) this instance itself consumes through the hub being rolled are not waited for either (they have no end of their own,
  so waiting would always cost the full 10 s); the roll cuts them, the stream or the `updates` flow fails with `HubConnectionLostException`,
  and the consumer subscribes again, as after any lost stream. A scatter-gather in flight, which ends within its deadline, is waited for.
- *Requests not yet written* when the stream closes follow the usual lost-stream rules (`HubUnavailableException`, sent again on another hub).
- A roll that does not complete (the new stream does not register within 30 s) stops the rollover for that pair with one `ERROR`: the other
  hubs keep the old pair until the files change again (the hub that did not register keeps trying with the new pair on its own).

**Expiry.** The hub closes a certificate session at `notAfter` with `UNAUTHENTICATED` "certificate expired" and no grace. A service whose
files are renewed before that never meets it. Otherwise: with less than a tenth of its lifetime (24 hours at most) left and no renewed
file, the library logs one `WARN` per certificate; when the hub closes the session at `notAfter` the library treats it as a normal
reconnect (it is **not** a registration rejection: no back-off at the cap) and reads the files again; if they still hold the expired
certificate, the attempt fails locally, without a handshake, as a registration rejection (`UNAUTHENTICATED` "client certificate ... expired
at ...": one `ERROR`, then retried at the back-off cap), and a renewed file is picked up by the next attempt.

**What you can read.** `HubConnections.authStatus()` has `kind = CERTIFICATE`, `certificateService`, `certificateFingerprint` (the first 8 hex characters of
the leaf's SHA-256, the only form of the certificate that is logged or shown; the hub's topology shows the same value), `certificateExpiresAt`
(`notAfter`) and `certificateRotations` (pairs adopted since start). With Micrometer the starter binds
`kraken_hub_client_certificate_rotations_total`. Logs name a certificate only by that fingerprint and the service; no key material
is ever logged, and `toString()` of the certificate objects shows neither key nor certificate.

**Divergences from the Rust library** (same design, ecosystem differences): the Kotlin starter reports through `authStatus()` and a
Micrometer counter where Rust has `hub_auth()`; the status fields are `certificate*` next to the existing `tokenExpiresAt`; the JDK's
PKIX validation rules apply to the hub's certificate (the Rust library uses rustls).

### Handlers

```kotlin
@Component
class WorkoutHandlers(private val repo: WorkoutRepository, private val events: EventBus) {

    // @Transactional handlers are plain (non-suspend) functions: see "Transactions" below.
    @CommandHandler
    @Transactional
    fun handle(cmd: LogWorkout, ctx: MessageContext): LogWorkoutResult {
        val workout = repo.save(Workout.from(cmd, ownerId = ctx.userId))
        events.publishBlocking(WorkoutLogged(workout.id, workout.ownerId, workout.loggedAt), key = workout.id)
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

`Workout`, `WorkoutRepository` and `NotFoundException` are illustrative service types, not library classes.

Messages are plain data classes implementing the marker interfaces from the api module:

```kotlin
@MessageType("workouts.LogWorkout")
data class LogWorkout(val name: String) : Command<LogWorkoutResult>
```

### Sending

```kotlin
val logged = commandBus.send(LogWorkout("legs"))                 // suspend
val loggedNow = commandBus.sendBlocking(LogWorkout("legs"))      // blocking, for MVC controllers
val workout = queryBus.send(GetWorkout(id))
eventBus.publish(WorkoutLogged(id, owner, at), key = id)         // inside the caller's Mongo transaction

// Per call: priority, timeout (the total budget with retry), routing key, retry policy.
val urgent = commandBus.send(
    LogWorkout("legs"),
    SendOptions(priority = 10, timeout = Duration.ofSeconds(2), routingKey = id, retry = RetryPolicy(maxAttempts = 3)),
)
val quick = queryBus.sendBlocking(GetWorkout(id), SendOptions(retry = RetryPolicy.NONE)) // no retry for this call
```

## Semantics worth knowing

- **Typed facades.** `Command<R>` and `Query<R>` carry the response type; it is read from the interface type argument at
  runtime. Generic responses such as `List<X>` are erased, so wrap them in a response class. Builder call.
- **Local short-circuit.** A command or query whose handler lives in the same service is invoked locally and does not go
  through the hub. The handler runs on the caller's thread and coroutine, so it joins the caller's thread-bound
  transaction (a failure in it rolls back the caller's writes too) and sees the caller's security context. A query on a
  type this service declares `@QueryHandler(shared = true)` is the exception: it always goes through the hub (see
  Scatter-gather below), and so is a streaming query (see Streaming queries).
- **Incoming hub requests** run on virtual threads, at most `hub.max-concurrent-handlers` at once, never on the threads
  that read the hub stream; a handler may block (for example on a nested `sendBlocking`).
- **Correlation id.** Every message carries a `correlationId`. Messages sent while handling a hub request or a Kafka
  event reuse the incoming id (carried in the coroutine context and mirrored into the SLF4J MDC key `correlationId`);
  otherwise the MDC value is used, else a fresh UUID.
- **Request ids.** `Request.id` is fresh per attempt. A retried command must carry its own business id in the payload.
- **Identity.** The caller's bearer token is forwarded on the hub path only (`security.forward-jwt`). Requests are
  trusted by default: a request without a token is anonymous and still handled. See [Security](#security) for
  `@RequiresAuthentication`. Builder call.
- **Events** carry `userId` only, not the token. Events published from an `@EventHandler` carry an empty `userId`:
  the consumer has no forwarded token, only the incoming event's `userId` in `MessageContext`.
- **Outbox ordering.** Events with the same key are published in order per publishing instance, using a `seq` field on the
  outbox document. Builder call.
- **Outbox failures.** A send failure that retrying cannot fix (record too large, serialization, invalid topic,
  authorization) or the `outbox.max-attempts`-th failure marks the document failed (`failedAt`), logs it once at error and
  moves on; other failures stop the batch and are retried on the next poll. Failed documents stay in `outbox` (the TTL
  only removes sent ones); clear `failedAt` and `attempts` to re-queue one.
- **Consumer idempotency.** The consumer records processed event ids in `processed_events` and skips duplicates. A handler
  gets up to `max-attempts` invocations with a 1 s back-off; a record that still fails becomes a dead letter (see
  [Dead letters](#dead-letters)). A payload that does not decode into the handler's class is not retried.
- **Transactions.** The library invokes `@EventHandler`s inside its own Mongo transaction (idempotency marker plus the
  handler's writes), on the Kafka listener thread via `runBlocking`; a handler that relies on it must not switch threads.
  The `MongoTransactionManager` is thread-bound: Spring only carries a transaction across suspensions for reactive
  transaction managers, so a `@Transactional suspend` function loses its transaction at the first real suspension (an
  awaited `send`, `delay`, `withContext` to another dispatcher) and later writes are not part of it. Rules:
  - make `@Transactional` handlers non-suspend functions, or make sure they do not suspend before their last write;
  - `eventBus.publish`/`publishBlocking` only insert into `outbox` and never suspend, so they are safe inside the
    transaction;
  - to send a command or query as well, commit first and send afterwards, for example a non-suspend `@Transactional`
    service method that does the writes, called from the handler, followed by `commandBus.send(...)`:

    ```kotlin
    @CommandHandler
    suspend fun handle(cmd: CloseWorkout, ctx: MessageContext): Unit {
        val workout = workouts.close(cmd.workoutId)          // @Transactional, non-suspend: commits on return
        commandBus.send(NotifyOwner(workout.ownerId))        // after the commit
    }
    ```
- **Shutdown.** The hub connection, outbox publisher and Kafka consumer start before the web server accepts traffic and
  stop after its graceful drain. On stop the instance first announces an empty registration so the hub stops routing to
  it, waits up to 5 s for incoming handlers still running, then closes the stream.
- **Handshake.** `Register` carries `initial_permits` and `client_version` `kraken-messaging/<version>`. The hub answers
  every accepted `Register` with a `RegisterAck` (hub id, heartbeat interval and timeout, default, maximum and per-type
  request timeouts) before the echo of the heartbeat that follows it; the ack of the stream's first `Register` completes
  the handshake and its heartbeat interval is the stream's. A stream without an ack within 10 s
  (`HubProtocol.HANDSHAKE_TIMEOUT`) fails ("registration not confirmed in time") and the connection reconnects.
  `HubConnection.ack` holds the latest ack (per hub: `HubConnections.connections()`). The hub, the protocol and this
  library ship together: a hub and a client of different builds are not supported.
- **Timeouts.** Unless `hub.request-timeout` is set, requests go out with `timeout_ms = 0` and the hub decides (default
  or per-type override). The client gives up locally only when the hub never answers: after the hub's deadline bound
  from the ack (`min(timeout, override or max)`, or `override or max` for 0) plus 1 s, with `MessageTimeoutException`.
- **Dead connections.** A hub stream whose heartbeats stay unanswered for three heartbeat intervals (9 s with the hub's
  default of 3 s) is closed and re-established; HTTP/2 keepalive pings (every 10 s, 5 s timeout, also without calls) run on the
  channel.
- **Lost connections.** A request the stream never accepted (including one still queued on the client when the stream
  broke) fails with `HubUnavailableException` (the handler did not run, retrying is safe; with several hubs it is sent to
  another hub first). A request written to a stream that broke before the answer fails with
  `HubConnectionLostException`: the outcome is unknown, as with a timeout.
- **Flow control.** `Register` carries `initial_permits = hub.max-concurrent-handlers` (at least 1). The hub forwards to
  this instance only against permits and queues the rest per instance, highest `Request.priority` first, each request keeping its own deadline; every completed handler pays
  one permit back on the stream the request arrived on. Permits go back at once while the hub holds at most
  `min(16, initial_permits / 2)` of them for the stream (so a single permit is never held back), otherwise batched (one
  `Permits` message per 16 grants or 50 ms). A slow handler therefore means latency, not timeouts.
- **Busy handlers.** A full hub queue or session buffer answers `HandlerBusyException` (`RESOURCE_EXHAUSTED`): the
  request was rejected before any handler ran, so retrying later is safe.
- **Retry** (opt-in, `hub.retry.*` or `SendOptions.retry`). Only errors `RetryClassification.isRetryable` allows are
  retried: for commands `HubUnavailableException`, `NoHandlerException`, `HandlerBusyException`; for queries also
  `HandlerDisconnectedException`, `MessageTimeoutException`, `HubConnectionLostException`. A command is never retried after
  a timeout or a lost connection (it may have run). Each attempt is a new request (fresh id, same correlation id); all
  attempts share one budget, the chosen timeout or else the hub's deadline for the type (its per-type override or
  default), each sending the remaining budget as `timeout_ms`. A retry starts only if at least
  `max(initial-backoff, 250ms)` of the budget is left after its backoff; otherwise the last error is thrown at once.
  Local handlers (same instance) are never retried.
- **Hub errors.** The hub answers malformed or duplicate in-flight requests with `INVALID_REQUEST`; other hub errors map to
  the exception types in `io.damoreira.kraken.messaging` (`NoHandlerException`, `UnauthenticatedException`, ...).

## Multiple hubs

kraken-hub reaches high availability through several independent hubs that never talk to each other (ADR 0016 in the
umbrella repo): every instance connects to every hub, and the client decides where each request goes.

```yaml
kraken.messaging.hub.address: static://hub-1:7000,static://hub-2:7000   # or: dns:///kraken-hub:7000
```

- **One connection per hub.** `HubConnections` opens one `Connect` stream per target, each with its own handshake,
  heartbeat and reconnect backoff, and sends the identical `Register` on every one (re-sent on every
  reconnect). One target behaves exactly like the single-hub client.
- **Targets.** A comma-separated list (blank entries and repeats dropped, order kept), or a single `dns:///name:port`
  (port required, not combinable with other entries) resolved at start and every 30 s: a new address gets a connection,
  a vanished one first announces an empty registration, then is closed once idle (at most 30 s later); an address that
  comes back before its connection is closed revives that connection instead of dialing a second one. Only one address
  family is used (rule shared with the Rust library): the IPv4 addresses when the answer has any, else the IPv6 ones,
  repeats dropped; a dual-stack name lists every hub under both families, and two streams to one hub with the same
  instance id would replace each other. A failed or empty resolution keeps the current connections.
- **Hub identity.** A hub is its `RegisterAck.hub_id`; an ack with an empty id is its target id: the bare `host:port` without `static://` (a resolved IPv6 address as `[ip]:port`, without zone; rule shared with
  the Rust library). When
  two targets answer the same id, the one that acknowledged first owns it for as long as it is kept, live or
  reconnecting, and the other is closed and parked (logged once). The hub keeps one session per instance id, so the
  second target's `Register` ends the first one's stream before its own ack arrives; the owner reconnects and the
  duplicate never takes over, so the two cannot evict each other back and forth. A parked target is retried only once
  its owner is gone (a DNS address no longer resolved, or a target now answering another id). Each hub replica needs
  its own `KRAKEN_HUB_ID`: replicas sharing one id are taken for one hub, and all but one are parked.
- **Live hubs** are the kept connections whose stream completed its handshake.
- **Routing.** A request without a routing key goes round-robin over the live hubs; a hub that is reconnecting is
  skipped. A request with a routing key (`SendOptions.routingKey`) goes to the live hub with the highest rendezvous score
  for (routing key, hub id), so a key keeps its hub while the hubs are stable and only the keys of a hub that leaves
  move; the hub's own consistent-hash ring then keeps it on one instance. Replies always go back on the stream the
  request arrived on.
- **Rendezvous score** (identical in the Rust library): `fmix64(fnv1a64(utf8(hub_id + "\u0000" + routing_key)))`, compared
  as unsigned 64-bit; equal scores go to the smaller hub id. `fnv1a64` is 64-bit FNV-1a (offset basis
  `0xcbf29ce484222325`, prime `0x100000001b3`); `fmix64` is MurmurHash3's 64-bit finalizer (`k ^= k >>> 33;
  k *= 0xff51afd7ed558ccd; k ^= k >>> 33; k *= 0xc4ceb9fe1a85ec53; k ^= k >>> 33`). Vectors: `score("hub-a",
  "order-1") = 0xd8d5618ff3a93f55`, `score("hub-b", "key-0") = 0xbd443f5acaf98254` (`RendezvousTest`).
- **Failover.** A request its hub could not take before it was written (the stream had just closed, or broke while the
  request was still queued on the client) is sent to another live hub; with no hub left the call waits for one. The
  call has one deadline: its own timeout (`SendOptions.timeout`, else `hub.request-timeout`; 5 s when neither is set);
  waiting ends there with `HubUnavailableException`, and a request sent again carries what is left of its
  `timeout_ms`, so failover never stretches a call beyond its timeout. A request already written fails with `HubConnectionLostException` (or the hub's `HandlerDisconnectedException`), and the retry policy
  applies. With no live hub at all: `HubUnavailableException`, as with one hub.
- **Permits.** A stream announces `ceil(hub.max-concurrent-handlers / N)` initial permits, where N = 1 (the hub being
  registered on) + the other hubs that are live or still on their first connect attempt (a hub down after a failed
  attempt is not counted). Whenever that count changes, every live stream is topped up to its current share:
  `Permits{count = max(0, ceil(max / N) - offered), grant = ceil(max / N)}`, offered = the stream's `initial_permits`
  plus its earlier top-ups; the grant raises the hub's permit cap on the session (otherwise its `initial_permits`) so
  the hub keeps the top-up. So with two hubs the
  survivor of an outage (or of a hub that failed its first attempt) serves at full capacity, flaps never add up, a hub
  that joins or returns gets `ceil(max / N)` on its new stream, and nothing is ever revoked (rule shared with the Rust
  library). Locally at most `hub.max-concurrent-handlers` handlers run in total, whichever hub sent the requests. Permits are paid back per stream.
- **Readiness.** `HubConnections.awaitRegistered(timeout)` is true only once **every** kept hub confirmed the latest
  registration; while one configured hub is down it returns false at the timeout, although the instance is already
  served by the others (`connected` / `liveHubIds()` tell that). The Rust library's equivalent may differ; see its
  README.
- **Status.** Every `hub.status-interval`, and whenever the live set changes, each live hub gets an `InstanceStatus`:
  `connected_hub_ids` (the ids of the live hubs), `handlers_in_flight` (over all hubs) and the event processors. The hub shows
  the report in its topology (`connectedHubIds`, `statusAgeMs`) and the dashboard flags instances missing a hub.
- **Shutdown** announces the empty registration on every hub, waits for the handlers, then closes every stream.

## Scatter-gather

A query type can be **shared** by several services (ADR 0017 in the umbrella repo): any number of services may own it.
A point query (`queryBus.send`) on a shared type goes to one instance of any owner (the hub round-robins over all of
them, also when the sender is itself an owner: a shared type is never short-circuited to the local handler); a
**scatter-gather** asks every live instance of every owner and returns what they answered.

```kotlin
@Component
class CatalogueSearch {
    @QueryHandler(shared = true) // every service that can answer declares it shared
    fun handle(q: SearchEverything): SearchHits = SearchHits(index.search(q.text))
}

// Every answer that arrived by the deadline, in arrival order:
val result: ScatterResult<SearchHits> = queryBus.scatterGather(
    SearchEverything("legs"),
    ScatterOptions(timeout = Duration.ofSeconds(2), maxResults = 0, priority = null),
)
result.parts.forEach { part -> println("${part.service}/${part.instance}: ${part.result ?: part.error}") }
println("${result.results.size} answers, ${result.failed} failed, ${result.timedOut} timed out")

// The same as a Flow: each part as it arrives; completes on the end.
queryBus.scatterGatherFlow(SearchEverything("legs")).collect { part -> render(part) }
```

- **API.** `suspend fun <R> scatterGather(query: Query<R>, options: ScatterOptions = ScatterOptions()): ScatterResult<R>`
  and `fun <R> scatterGatherFlow(query: Query<R>, options: ScatterOptions = ScatterOptions()): Flow<ScatterPart<R>>`.
  `ScatterOptions(timeout: Duration? = null, maxResults: Int = 0, priority: Int? = null, retry: RetryPolicy? = null)`,
  `ScatterResult<R>(parts, failed, timedOut)` (plus `results`, the successful values), `ScatterPart<R>(service,
  instance, result: R?, error: RemoteHandlerException?)`. The flow carries no counts: use `scatterGather` when
  `failed` / `timedOut` matter.
- **Targets.** Every live instance of every owner of the type; on an exclusive type, every instance of its one owner.
  The gather always goes through the hub, also when this instance owns the type (it is one of the targets then).
  Handlers need nothing new: each target receives an ordinary request (id `<id>#<n>`) and answers it as usual.
- **Results.** One part per target that answered, successes and failures, in arrival order. A failed part carries a
  `RemoteHandlerException` whose `code` is the hub's (`HANDLER_ERROR` for a handler exception, `HANDLER_DISCONNECTED`,
  `RESOURCE_EXHAUSTED`, ...); a part whose payload does not decode into `R` is a failed part too (code
  `ERROR_CODE_UNSPECIFIED`), never a failure of the whole call, so `failed` counts every part with an error. Targets
  that had not answered by the deadline count in `timedOut`. Partial answers are answers: check `failed + timedOut`
  for "all or nothing".
- **Options.** `timeout` is the deadline of the whole gather (same rules as `SendOptions.timeout`: default
  `request-timeout`, else the hub's default; zero is unset). `maxResults > 0` ends the gather after that many
  successful parts. `priority` applies to every target request.
- **Errors.** No owner: `NoHandlerException`; a command type: `InvalidMessageException`. With several hubs a gather
  goes to one hub (round-robin). If the stream to that hub breaks before the end the call fails with
  `HubConnectionLostException`.
- **Retry.** The configured or per-call `RetryPolicy` applies to the whole gather with the query rules
  (`HubConnectionLostException`, `NoHandlerException`, ... are retryable). A retried gather is a new gather: its result
  holds only its own parts. The flow retries only until its first part; after that an error ends the flow, so no part
  is ever emitted twice.
- **Shared registration.** Every registrant of a type must agree: exclusive by one and shared by another (even within
  one service) is refused by the hub with `ALREADY_EXISTS` at register time, logged and surfaced like any registration
  conflict (`HubRegistrationRejectedException` from `awaitRegistered`). One service declaring a type both ways fails at
  startup. Commands are never shared.

## Streaming queries

A **streaming query** (ADR 0017, P4b) is answered by one handler instance as a stream of parts under the collector's
back-pressure: an export, a long listing, a search that trickles. The query implements `StreamingQuery<R>` (`R` is
the element type), the handler returns `Flow<R>`, and the caller collects a `Flow<R>` from `queryBus.stream`.

```kotlin
@MessageType("workouts.ExportWorkouts")
data class ExportWorkouts(val ownerId: String) : StreamingQuery<WorkoutRow>

@Component
class WorkoutExport(private val repo: WorkoutRepository) {
    @QueryHandler // shared = true works here too: one instance of any owner answers
    fun handle(q: ExportWorkouts): Flow<WorkoutRow> = repo.streamByOwner(q.ownerId).map { it.toRow() }
}

// Each part as the handler produces it, paced by credits; completes when the handler's flow does.
queryBus.stream(ExportWorkouts(ownerId), StreamOptions(initialCredit = 32, timeout = Duration.ofSeconds(30)))
    .collect { row -> writer.write(row) }
```

- **API.** `fun <R> stream(query: StreamingQuery<R>, options: StreamOptions = StreamOptions()): Flow<R>` and
  `StreamOptions(timeout: Duration? = null, maxResults: Int = 0, priority: Int? = null, initialCredit: Int = 64,
  retry: RetryPolicy? = null)`. The flow is cold and unbuffered: each collection sends one request, and the next part
  is read only after the collector consumed the previous one. A controller may return the flow: the caller's bearer
  token and trace context are read when `stream` is called, not when the flow is collected.
- **Handler.** `@QueryHandler` (plain or `shared = true`) on a method taking the `StreamingQuery<R>` (optionally with
  a `MessageContext`) and returning `Flow<R>`; another return type, `@CommandHandler`, or a `Flow` returned for a plain
  `Query`, fails at startup. The library
  collects the flow and sends one part per element, **suspending the collection inside `emit` while the hub granted no
  credit**, then the end. An exception from the flow ends the stream with the handler's error, mapped exactly like a
  unary failure (`HANDLER_ERROR` with `IllegalArgumentException` / validation details, `AccessDeniedException`, ...);
  `@RequiresAuthentication` is checked before the flow starts. The handler counts against `max-concurrent-handlers`
  for the whole stream. A part that waits for credit longer than `hub.stream-credit-timeout` (10 min by default, the
  hub's default maximum stream duration) means the hub forgot the stream (a race, a lost `Flow` or `Cancel`): the
  library ends it with `TIMEOUT` ("no credit within ... ms"), the flow seeing a `CancellationException` as at a hub
  `Cancel`, and frees the handler slot and the hub permit.
- **Routing.** Always through the hub, also when this instance owns the type: one instance of the owner (one of every
  owner's instances on a shared type), chosen like a point query. A `StreamingQuery` is not a `Query`: it cannot be sent with `send` or gathered, and a unary request
  that reaches a streaming handler is answered `INVALID_REQUEST`.
- **Credits.** `initialCredit` is the window: the handler may run that many parts ahead of the collector. Each time
  the collector consumed half of the window the library grants `initialCredit / 2` more (a `Flow` message), so a
  collector that keeps up never stalls the handler and a slow one holds it at the window. A handler sending beyond its
  credit (never this library) ends the stream with `INVALID_REQUEST`.
- **Options.** `timeout` is the **idle** timeout: the hub ends the stream with `TIMEOUT` when no part arrives within it
  (reset by every part; a queue wait at the hub counts too; it does not run while the credit is 0, so a collector that
  holds the handler at the window is never an idle handler), same defaults as `SendOptions.timeout`. It is sent whole
  as `timeout_ms` on every attempt: with retry the budget bounds the attempts before the first part (and, with several
  hubs, the wait for a live one), never the idle timeout the stream runs with. `maxResults > 0`
  ends the stream after that many parts (the hub cancels the handler). `priority` is the forwarded request's. The hub
  also bounds a stream's whole life (`KRAKEN_HUB_MAX_STREAM_DURATION`, 10 min by default): `CANCELLED`.
- **Errors.** Before any part, as for a unary query: no owner `NoHandlerException`, a command type
  `InvalidMessageException`; with several hubs the stream goes to one hub (round-robin). An end with an error fails the
  flow **exactly like a unary `send` fails on the same hub code**: `HANDLER_ERROR` (the handler's flow failed)
  `RemoteHandlerException`, `TIMEOUT` (idle) `MessageTimeoutException`, `HANDLER_DISCONNECTED` (the instance left
  mid-stream: a stream never moves to another instance) `HandlerDisconnectedException`, `RESOURCE_EXHAUSTED` (the
  instance's hub queue was full) `HandlerBusyException`, `INVALID_REQUEST` `InvalidMessageException`, and `CANCELLED`
  (the hub's maximum stream duration) the new `StreamCancelledException`. The parts emitted before it stay emitted. A
  part that does not decode into `R` fails the flow with `InvalidMessageException` and cancels the stream. The stream
  to the hub breaking mid-stream is `HubConnectionLostException`. Parts carry the handler's `seq` (1, 2, 3, ...) and
  the end carries the hub's count of parts: the hub drops a part it could not deliver within 5 s on a full buffer, so
  the library checks both and fails the flow with `InvalidMessageException("stream <type> lost N part(s)")` on a jump
  in `seq` (the stream is cancelled) or an end counting more parts than arrived (the stream had ended; nothing is sent).
  An exception thrown **by the collector** is the collector's own failure and reaches it unchanged, also when it is a
  `HubUnavailableException` or `HubConnectionLostException` of a nested send inside `collect {}`: the stream is
  cancelled and never moved to another hub, since its request was written.
- **Cancel.** Cancelling the collector (`take(n)`, a cancelled coroutine) or throwing in it sends `Cancel` to the hub,
  which cancels the handler's coroutine (its flow sees a `CancellationException`); nothing of the stream follows. The
  hub cancels the handler the same way at `maxResults`, at the idle timeout, at the max duration and when the
  collector's instance disconnects (`sender disconnected`); the handler's own hub stream closing cancels its streams
  too. A hub `Cancel` also stops a unary or gather handler whose answer is no longer wanted (a gather past
  `maxResults` or its deadline, or whose sender disconnected). A blocking (non-`suspend`) handler is not interrupted
  by any of these: it runs to its end and its reply is dropped; only a suspending handler stops at its next suspension
  point.
- **Retries.** The configured or per-call `RetryPolicy` applies with the query rules only until the first part, to an
  end with an error as to an error before any part (`HandlerDisconnectedException`, `MessageTimeoutException`,
  `HandlerBusyException`, `NoHandlerException`, `HubConnectionLostException`, ... are retried; handler errors,
  `StreamCancelledException` and the rest are not): a retried stream is a new request (fresh id; with several hubs,
  round-robin to another one), so no part is ever emitted twice. After the first part nothing is retried: the
  flow throws, a broken hub stream as `HubConnectionLostException`. The budget covers the attempts up to the first part
  (and the wait for a live hub); every attempt sends the chosen `timeout` whole as its idle timeout, never what is
  left of the budget.

## Subscription queries

A **subscription query** (ADR 0017, P4c) is an initial result followed by the updates the handlers push, until the
subscriber cancels or the handlers complete: a screen that shows an account and refreshes when it changes, a status page.
The query implements `SubscriptionQuery<I, U>` (`I` the initial result, `U` an update), the handler returns the initial
result `I`, every handler instance pushes updates through the `QueryUpdateEmitter`, and the caller gets a
`SubscriptionResult<I, U>` from `queryBus.subscribe`.

```kotlin
@MessageType("accounts.WatchAccount")
data class WatchAccount(val accountId: String) : SubscriptionQuery<AccountView, AccountChanged>

@Component
class AccountWatch(private val repo: AccountRepository) {
    @QueryHandler // shared = true works here too
    fun handle(q: WatchAccount): AccountView = repo.view(q.accountId) // the initial result
}

@Service
class Accounts(private val repo: AccountRepository, private val emitter: QueryUpdateEmitter) {
    @Transactional
    fun deposit(accountId: String, amount: Money) {
        val account = repo.deposit(accountId, amount)
        // Inside the transaction: delivered after the commit, dropped on a rollback.
        emitter.emit<WatchAccount, AccountChanged>({ it.accountId == accountId }, AccountChanged(accountId, account.balance))
    }
}

// Returns once the initial result arrived; the updates follow as a flow.
val result = queryBus.subscribe(WatchAccount("acc-1"), SubscriptionOptions(initialCredit = 32))
render(result.initial)
result.updates.collect { change -> render(change) }   // completes when the handlers do; cancel() or take(n) cancel it
```

- **API.** `suspend fun <I, U> subscribe(query: SubscriptionQuery<I, U>, options: SubscriptionOptions =
  SubscriptionOptions()): SubscriptionResult<I, U>` and `SubscriptionOptions(timeout: Duration? = null, maxResults: Int =
  0, priority: Int? = null, initialCredit: Int = 64, retry: RetryPolicy? = null)` (`maxResults >= 0`, `initialCredit >= 1`).
  `SubscriptionResult(val initial: I, val updates: Flow<U>)` has `fun cancel()`; `initial` is a value, `subscribe` having
  returned only once the initial part arrived. A controller may return `updates` (an SSE endpoint): the caller's bearer
  token and trace context are read when `subscribe` is called, not when the flow is collected.
- **Handler.** `@QueryHandler` (plain or `shared = true`) on `fun handle(q: SubscriptionQuery<I, U>[, ctx: MessageContext]): I`
  answers the **initial result**; a handler returning a `Flow`, or `@CommandHandler` on a subscription query, fails at
  startup. The hub sends every live instance of the type a `SUBSCRIPTION` request, and the library registers the
  subscription (the decoded query, its request id and the hub stream it came from); the initial result is a separate
  ordinary request answered by the same method, which the library recognises by the hub's request id (`<id>#0`). A unary
  request for the type is answered `INVALID_REQUEST` ("is a subscription query: send it with QueryBus.subscribe"), a
  `SUBSCRIPTION` request for a type without a subscription handler ends with `INVALID_REQUEST`, and
  `@RequiresAuthentication` is checked when the subscription is registered and again on the initial request.
- **`QueryUpdateEmitter`.** An auto-configured bean: `emit(queryType, filter, update)`, `complete(queryType, filter)` and
  `completeExceptionally(queryType, filter, cause)`, plus reified overloads (`emitter.emit<WatchAccount, AccountChanged>(
  { it.accountId == id }, update)`, `emitter.complete<WatchAccount> { it.accountId == id }`). They deliver to every open
  subscription of that type **on this instance** whose decoded query passes `filter`, in call order: an update is one
  part (encoded with the library codec), `complete` ends the subscription cleanly, `completeExceptionally` ends it with
  `HANDLER_ERROR` and `"<SimpleName>: <message>"` (the subscriber's flow fails with a `RemoteHandlerException`), and both
  remove it. With no matching subscription the call does nothing, so a service may emit unconditionally. A filter that
  throws skips its subscription (logged), never the others.
- **Transactions.** Inside an active Spring transaction (`TransactionSynchronizationManager.isActualTransactionActive()`:
  `@Transactional`, `TransactionTemplate`) every emit and complete is deferred to `afterCommit` and dropped on rollback,
  so a subscriber never sees an update of data that was never committed; the open subscriptions and the filter are
  evaluated at delivery, after the commit, and a failure there is logged, never thrown at the committer. Outside a
  transaction delivery is immediate, and a reactive (Reactor-context) transaction is not seen.
- **Targets, and the at-most-once caveat.** Every live instance of every owner, **as they are
  when the subscription opens**: an instance that registers later is never added (an update emitted on a new instance is
  lost to that subscriber), a target that leaves is not replaced, and the hub never resubscribes. After a rolling
  restart a subscription listens to fewer instances than serve the type, silently until the last one leaves
  (`HandlerDisconnectedException`); a subscriber that needs every instance resubscribes. One of them, chosen like a point
  query, answers the initial result. A subscription is passive: it holds no handler permit, waits in
  no hub queue and is not counted as an in-flight request after the initial result.
- **Credits and the hub's buffer.** `initialCredit` is the window: the hub relays that many parts (the initial result
  included) before the collector consumed any, and holds further updates for the subscriber, at most 256
  (`KRAKEN_HUB_SUBSCRIPTION_BUFFER`). One more ends the subscription with `RESOURCE_EXHAUSTED`, which the flow throws as a
  `HandlerBusyException` carrying the hub's reason; what the hub buffered is dropped. The library grants `initialCredit /
  2` more each time that many parts were consumed, the initial result counting as consumed once `subscribe` returned
  it (so a window of 1 cannot stall). No credit goes to the handlers: updates are pushed, an emitter is shared by every
  subscriber of its instance, and a slow subscriber is the hub's problem, not theirs. A result that is **dropped without
  being collected** leaves the hub buffering until it reaches that limit and ends the subscription; collect it or
  `cancel()` it.
- **Options.** `timeout` bounds the **initial result** only (the hub queue wait plus the handler), sent as `timeout_ms`
  with the same defaults as `SendOptions.timeout`; there is no idle timeout, a quiet subscription is not a broken one.
  `maxResults > 0` ends the subscription after that many **updates** (the initial result does not count) with a clean end
  and cancels the handlers' side. `priority` is the initial request's. The hub may bound a subscription's whole life
  (`KRAKEN_HUB_MAX_SUBSCRIPTION_DURATION`, off by default): `StreamCancelledException`.
- **Errors.** `subscribe` fails before the initial result as a unary query does: no owner `NoHandlerException`, a command type
  `InvalidMessageException`, the initial request failing `RemoteHandlerException` / `MessageTimeoutException` /
  `HandlerDisconnectedException`, an instance already serving its limit (the hub's, or this library's
  `hub.max-subscriptions`) `HandlerBusyException`. After it, `updates` completes on a clean end and throws what a unary
  error with the same code throws: `HANDLER_ERROR` (`completeExceptionally`, or a failing target) `RemoteHandlerException`,
  `HANDLER_DISCONNECTED` (the last handler instance left) `HandlerDisconnectedException`, `RESOURCE_EXHAUSTED`
  `HandlerBusyException`, `CANCELLED` `StreamCancelledException`; the stream to the hub breaking is
  `HubConnectionLostException`. An update that does not decode into `U` fails the flow with `InvalidMessageException` and
  cancels the subscription; an initial result that does not decode fails `subscribe` the same way and cancels it. The
  library checks the parts' `seq` (the hub numbers them 1, 2, 3, ...) and the end's `parts` count: a gap fails the flow with
  `InvalidMessageException("subscription <type> lost N part(s)")`.
- **Cancel.** Cancelling the collector (`take(n)`, a cancelled coroutine), throwing in it, or `SubscriptionResult.cancel()`
  sends `Cancel` to the hub once, which cancels every target: the handlers' subscriptions are removed. `cancel()` also
  completes `updates`; `updates` can be collected **once**. On the handler side a hub `Cancel` removes the subscription, a lost
  hub stream removes every subscription it carried (the hub forgot them, and they are not restored on reconnect), and
  shutting the application down ends them with a clean `ReplyEnd` (best effort: the subscriber's flow completes once every
  other target has too, or fails with `HandlerDisconnectedException` if the stream closed first).
- **Retries.** The configured or per-call `RetryPolicy` applies with the query rules only until the initial result, to an
  end with an error and to a gate `Error` as to an error before any part (`NoHandlerException`, `HandlerBusyException`,
  `HandlerDisconnectedException`, `MessageTimeoutException`, ... are retried; handler errors and the rest are not): a retried
  subscription is a new request with a fresh id (with several hubs, round-robin to another one), so no update is
  delivered twice. After the initial result nothing is retried; resubscribing is the caller's call (it gets a fresh
  initial result).
- **Several hubs.** A subscription goes to one hub (round-robin), chosen and failed over like a gather.

## Event processors

Every `@EventHandler` belongs to an **event processor** (P6a, ADR 0022 in the umbrella repo). A processor is its own Kafka
consumer, in its own consumer group, on the events topic: its own position, its own failure domain, its own idempotency markers
and dead letters. A slow projection in one processor never holds a notifier in another.

```kotlin
@Component
@ProcessingGroup("orders")              // every @EventHandler of this class runs in processor "orders"
class OrdersProjection(private val orders: OrderRepository) {
    @EventHandler fun on(event: OrderPlaced) = orders.insert(event.toRow())
    @EventHandler fun on(event: OrderShipped) = orders.markShipped(event.orderId)
}

@Component
class Notifier {                         // no annotation: processor "default"
    @EventHandler fun on(event: OrderPlaced) = mail.confirm(event)
}
```

- **Names.** `@ProcessingGroup("name")` on a handler class; the name matches `^[a-z0-9][a-z0-9-]{0,47}$` and is not `default`. A
  bad name fails the startup naming the handler. Classes without the annotation are in processor `default`. Command and query
  handlers of an annotated class are not affected.
- **Groups.** Every processor, `default` included, uses `<service group>.<name>`, the service group being
  `kraken.messaging.kafka.group-id`, else the service name (`orders-service.default`, `orders-service.orders`), so a processor
  starts from the earliest offset the first time it runs.
- **Markers.** Every processor, `default` included, writes `processor:<name>:<eventId>` in `processed_events`, so one event type
  handled in two processors is handled by both, each exactly once. A marker a processor writes for a Kafka record also carries
  `processor`, `topic`, `partition` (Int32) and `offset` (Int64): a reset deletes exactly the markers of its replay range
  ([Processor control](#processor-control)).
- **Sagas run only in `default`** (their stage 2 follows the default processor's stage 1). A named processor runs
  only its handlers.
- **Which processors start.** At startup, one consumer per processor that has a handler; `default` also when sagas run. A
  service with no `@EventHandler` and no saga starts no consumer.
- **Attempts and back-off** (`kafka.consumer.max-attempts`, 1 s between attempts) are shared by all processors.
- **External sources and webhooks** dispatch locally to the handlers of every processor, each in its own transaction under its
  own marker (`processor:<name>:<dedupeId>`), and to the sagas once. Their records are never dead-lettered. The source names
  `processor`, `saga`, `ingress` and `failed` are refused (a partner source at startup, a webhook at the call): their markers
  would collide with the library's own.

**Moving an existing handler into a named processor runs it again over the whole topic.** The named processor's group is new,
so it starts at the earliest offset the topic still holds, and its markers (`processor:<name>:<eventId>`) are a new family,
so none of those records is a duplicate to it: the handler runs once more for every event in the topic's retention. A
projection that rebuilds idempotently may want exactly that; a handler with effects outside MongoDB (an email, a payment
call) repeats every one of them. For such code either leave the handler where it is and put only new handlers in the named
processor, or commit the new group's offsets at the end of the topic before you deploy it:
`kafka-consumer-groups --bootstrap-server <broker> --group <group>.<name> --topic <events topic> --reset-offsets --to-latest --execute`
(`<group>` is `kraken.messaging.kafka.group-id` or the service name; the group must have no running member). Records produced
between that reset and the moment the old instances stop are handled twice, once by the old `default` and once by the new
processor, so reset right before the rollout. Once it runs, a reset to `latest` through the hub does the same without a shell
([Processor control](#processor-control)), but by then it has already run once over the topic.

**Status at the hub.** Every status report (`hub.status-interval`, 5 s) carries one `ProcessorStatus` per started processor to
every hub: name, group,
topic, state, the partitions assigned to this instance with position, committed offset, high watermark and lag, records processed
and failed since start, the dead-letter count, the last error (message only, never a payload), the event types and the
dead-letter mode. The hub shows them in `GET /api/v1/topology`, `GET /api/v1/processors` and the dashboard's Processors page. The states:

| State | When |
|---|---|
| `STARTING` | started, no assignment yet (an assignment of zero partitions counts: an idle group member is `RUNNING`), or between a revocation and the next assignment |
| `RUNNING` | otherwise |
| `FAILING` | for 60 s after a record exhausted its attempts, or a record could not be acknowledged (its letter could not be parked, the DLT could not be reached); wins over `STARTING` and `RUNNING` |
| `PAUSED` | paused through processor control (P6b): still in its group, consuming nothing; wins over `FAILING`, `STARTING` and `RUNNING` |
| `STOPPED` | the processor is stopping or stopped; also while a reset holds its lock (P6b), with `operation_id` set |

A report also carries `operation_id` (the reset holding the processor's lock, empty when none), `replaying` (a replay
range is open on this instance's partitions) and per partition `replay_until` (the end of its open replay range, 0 when none).

The report is built from memory and never waits: the assignment comes from the rebalance listener, the position from the last
acknowledged record (`-1` until one), the committed offsets and high watermarks from one Kafka `Admin` client (same connection
settings as the consumers) refreshed at most every 5 s on a background thread, and the dead-letter count from
`countDocuments({processor})` every 30 s and after a park. Lag is `high watermark - committed` (`- position` without a commit),
`-1` when unknown, never negative. `EventProcessors.processorStatuses()` (bean `ProcessorStatusSource`) gives the same list to an
application that wants it.

## Dead letters

A record a processor cannot handle is not only logged: by default (`kraken.messaging.kafka.dead-letter.mode: collection`)
it is **parked** in the service's MongoDB collection `dead_letters`, the later records of the same Kafka key wait behind it, and an
operator retries or discards it through the `DeadLetters` bean.

| Reason | What happened | Blocks its key | Retry |
|---|---|---|---|
| `HANDLER_FAILED` | stage 1 failed on every attempt (once for a payload the handler cannot decode, `InvalidMessageException`) | yes | yes |
| `SEQUENCED` | an earlier letter of its key is parked in this processor: stage 1 did not run (**the sagas still saw it** in `default`) | yes | yes |
| `INVALID` | no `type` header, or a value that is not a JSON object | no | no (discard) |

A type the processor does not handle is skipped, not a letter. A record without a key is never sequenced. A record that
only failed in the saga stage (stage 1 done, the saga store down on every attempt) is logged and skipped: stage 1 has nothing to
retry, and parked saga events have their own `saga_failures`.

**Never lost, parked once.** The letter is a single insert outside any transaction after the last attempt, `_id`
`<processor>:<topic>:<partition>:<offset>`; the offset is acknowledged only once the letter exists. When MongoDB is down the record
stays unacknowledged and is redelivered every second until the letter is written (the redelivery parks it, it does not run the
handler again; after a rebalance the new owner may run it once more, which stage 1's marker makes harmless). A redelivery of a record
already parked (a crash before the commit) finds its own letter and is acknowledged.

**Sequencing.** Each processor keeps the keys of its letters on its assigned partitions in memory (loaded on assignment, added to
on every park). A record whose key is in that set is checked against the collection: when the key still has a head letter (the
lowest offset that is not `INVALID`), the record is parked `SEQUENCED`; otherwise (retried, discarded or expired, possibly on
another instance) the key is forgotten and the record handled. **A letter that expires** (`dead-letter.retention`, 30 days)
**unblocks its key.**

```kotlin
@RestController
class DeadLetterAdmin(private val deadLetters: DeadLetters) {
    @GetMapping("/admin/dead-letters") fun list(@RequestParam processor: String?) = deadLetters.list(processor, limit = 100)
    @PostMapping("/admin/dead-letters/{id}/retry") fun retry(@PathVariable id: String) = deadLetters.retry(id).toString()
    @PostMapping("/admin/dead-letters/retry-all") fun retryAll(@RequestParam processor: String) = deadLetters.retryAll(processor)
    @DeleteMapping("/admin/dead-letters/{id}") fun discard(@PathVariable id: String) = deadLetters.discard(id)
}
```

`DeadLetters` (blocking; call it from a request thread):

| Call | Result |
|---|---|
| `list(processor: String? = null, limit: Int = 100): List<DeadLetter>` | sorted by `parkedAt`, then `_id` |
| `get(id): DeadLetter?`, `count(processor: String? = null): Long` | |
| `retry(id): RetryResult` | `Retried`, `Duplicate` (stage 1 had run: the letter is deleted), `Failed(message)` (the letter stays with `attempts + 1`, the new `error`, `lastAttemptAt`, reason `HANDLER_FAILED`), `Blocked(headId)` (retry the key's head first), `NotRetriable` (`INVALID`), `NotFound` |
| `retryAll(processor): RetryAllResult(retried, failed, blocked, skipped)` | per key in offset order, a key stopping at its first failure (its later letters count as blocked); then the keyless letters; `INVALID` letters skipped |
| `discard(id): Boolean` | deletes the letter; discarding a key's head makes the next letter of the key retriable |

A retry runs the processor's stage 1 (all its handlers for the type) once, **in the marker transaction, together with the letter's
deletion**: handled and deleted commit together or not at all, so a retry is exactly-once with respect to stage 1, also when two
instances retry the same letter. The sagas are not run again. A key stays blocked for new records until all its letters are retried
or discarded. Any instance of the service can serve the calls for the processors it runs; a processor it does not run (or a letter
of one) is an `IllegalArgumentException` naming it. In modes `topic` and `log` every call is an `IllegalStateException` naming the
mode.

**Mode `topic`** produces the failed or `INVALID` record to `<topic>.dlt` (same key, value and headers, plus `kraken-dlt-processor`,
`kraken-dlt-reason`, `kraken-dlt-error`, `kraken-dlt-original-topic`, `-partition` and `-offset`) and acknowledges it only after the
broker did. Nothing is sequenced and there is no admin API: the topic belongs to your tooling. **The DLT must exist, or broker
auto-creation must be on.** **Mode `log`** logs (never the payload) and skips: nothing is parked.

The document (the same from the Rust library; no `_class`, an absent field is absent):

| Field | Type | |
|---|---|---|
| `_id` | string | `<processor>:<topic>:<partition>:<offset>` |
| `processor`, `topic` | string | |
| `partition` / `offset` | Int32 / Int64 | |
| `key` | string | absent without a key |
| `eventId`, `eventType` | string | the `id` and `type` headers, absent when missing |
| `headers` | document | every record header (lossy UTF-8; a repeated name keeps its last value) |
| `payload` / `payloadBinary` | string / BinData | the value as text when it is UTF-8, else binary; both absent for an empty value |
| `reason` | string | `HANDLER_FAILED`, `SEQUENCED`, `INVALID` |
| `error` | `{type, message}` | exception simple name and message (at most 2000 characters); `Sequenced` / `InvalidMessage` |
| `blockedBy` | string | `SEQUENCED` only: the head letter when it was parked |
| `attempts` | Int32 | handler attempts so far |
| `parkedAt`, `lastAttemptAt`, `expireAt` | Date | `expireAt = parkedAt + retention` (TTL index) |
| `replay` | bool | `true` when parked while its processor replayed the record (P6b): its retry is a replay too; absent otherwise |

Indexes: `dead_letters_processor_key_offset`, `dead_letters_processor_parked`, `dead_letters_expire` (TTL), created at startup.

## Processor control

The hub (ADR 0023 in the umbrella repo) pauses, resumes and resets a processor of a service **on every
instance**, and lists, retries and discards its dead letters, from its dashboard or HTTP API (writes need the hub's write token
or an ACL credential with `control = true`; see the hub's documentation). The hub keeps nothing: the state of each processor is a
document in the service's own MongoDB, `processor_control`, which every instance reads at start (before its consumers poll) and
every 5 s, so a pause outlives every hub and restart and reaches instances that start later. A `ControlRequest` is only the fast
path. The bean `ProcessorControl` does all of it; its `pause(processor)` and `resume(processor)` do the same from your own code
(`pausedBy: "local"`).

- **Pause** stops consuming every assigned partition **without leaving the consumer group** (no rebalance): the record in hand
  finishes, nothing else starts, partitions assigned later start paused. The status says `PAUSED`. **Resume** undoes it. A pause
  holds only Kafka consumption: the local dispatch of external sources, webhooks and saga notices still runs the processor's
  handlers, and the `DeadLetters` admin calls keep working.
- **Reset** moves the processor's consumer group to `earliest`, `latest`, a timestamp or explicit offsets per partition, as a
  **leased operation**: every instance is prepared (it takes the processor's lock in `processor_control` for a 30 s lease, the hub
  renews it every 10 s, and leaves the group: `STOPPED` with the operation id), one executes, every one is finished (it releases
  the lock and consumes again, still paused if it was). An instance whose lease passes without a renewal (the hub died) releases it
  and resumes by itself; an instance that starts while the lock is held stays out of the group until it clears. A second reset
  while one holds the lock is refused (`LOCKED`). A renewal that arrives after the finish never takes the lock again: an
  operation that ended on an instance (finished, aborted, its lease passed, its lock lost) is remembered for at least
  max(2 x lease, 60 s), and a prepare of it, or a renewal whose lock is gone, is refused `NOT_PREPARED`.
- **The execute** resolves the target per partition (Kafka `Admin.listOffsets`; a timestamp past the last record means the end),
  reads the group's committed offsets ("before"), refuses while the group has any active member (`GROUP_NOT_EMPTY`, nothing
  changed), runs the processor's `@ResetHandler`s, deletes the processor's markers in the replay ranges, writes the ranges to the
  document and only then commits the target offsets (`Admin.alterConsumerGroupOffsets`). Right after the reset handlers and
  again before the commit, the executor must still extend its lease (and be within 120 s): otherwise it stops with `FAILED`
  "lease lost: ...", before touching markers or offsets. **A refusal after the reset handlers ran is not "nothing changed"**:
  their effects stay, and the message says what else happened, e.g. `reset handlers ran (2), 37 markers deleted, offsets not
  moved: retry the reset`. Retry the reset (the handlers run again), or check the projection.
- **A reset to anything but `latest` is a replay**: per partition, the records from the target up to where the group was
  (`[target, before)`) are handled again, by whichever instance owns the partition, exactly once overall (their markers were
  deleted). In the range **the sagas never run** (stage 2 is skipped) and **handlers marked `@DisallowReplay` do not run**; past
  the range everything is as usual. Once an instance has consumed past the end of a range it removes the range; the status shows
  `replaying` until then. A reset does not touch the processor's dead letters. A record parked while it was replayed keeps that
  in its letter (`replay: true`), so its retry runs stage 1 as a replay too: without its `@DisallowReplay` handlers.

```kotlin
@Component
@ProcessingGroup("orders")
class OrdersProjection(private val orders: OrderRepository) {
    @EventHandler fun on(event: OrderPlaced) = orders.insert(event.toRow())

    // Once per reset that replays something, on the instance that executes it, before any marker or offset changes.
    // Throwing (an Error too) fails the reset (RESET_HANDLER_FAILED) before any marker or offset changes; what the handlers
    // that ran did stays, and the refusal says how many ran. No parameter, or a ResetContext.
    @ResetHandler fun clear(ctx: ResetContext) {
        if (ctx.target == ResetTarget.Earliest) orders.deleteAll() // the whole topic replays: rebuild from scratch
    }
}

@Component
@ProcessingGroup("orders")
class OrderMails(private val mail: Mail) {
    @DisallowReplay                      // or on the class: a replay never sends these again
    @EventHandler fun on(event: OrderPlaced) = mail.confirm(event)
}
```

`ResetContext(processor, operationId, target, ranges)`: `target` is `ResetTarget.Earliest`, `Latest`, `Timestamp(timestampMs)` or
`Offsets(map of partition to offset)`; `ranges` the `ReplayRange(topic, partition, from, until)` the reset replays (never empty: a
reset that replays nothing runs no reset handler). A `@ResetHandler` belongs to its class's processor (`@ProcessingGroup`, else
`default`) and may suspend.

The `processor_control` document (the same from the Rust library; every field is always written):

| Field | Type | |
|---|---|---|
| `_id` | string | the processor name |
| `paused` | bool | |
| `pausedAt`, `pausedBy` | Date, string | null when not paused; `pausedBy` is the hub's control id, or `local` |
| `operation` | `{id, kind: "reset", leaseUntil, preparedAt}` | the reset holding the lock, null when none; expired once `leaseUntil` passed |
| `replay` | array of `{topic, partition: Int32, from: Int64, until: Int64}` | the open replay ranges, `[]` when none |
| `updatedAt` | Date | |

Every control request is logged at INFO with its control id, processor, action and result. Requests run off the hub stream, one
at a time per processor (a renewal of the lease this instance holds is answered at once, and the instance executing a reset
extends its own lease while it runs). Dead letters through the hub map to `DeadLetters` one to one (`list` without payloads or
headers, `retry`, `retryAll`, `discard`); an instance answers only for the processors it runs and only in mode `collection`.

## Sagas

A saga is a long-running process that reacts to events, keeps its own state, sends commands and events, waits for an answer and
handles the answer that never comes. It is Axon's `@Saga` model (`@StartSaga`, `@SagaEventHandler` with association properties,
`@EndSaga`, deadlines), built mainly to drive **integrations with external systems**. Sagas run inside your service, against its
MongoDB and its Kafka consumer; the hub is not involved except to carry the commands a saga sends to another service.

Everything starts and ends with kraken messages. A saga never calls an external system itself: it sends a command, an
**adapter** in the same service (an ordinary `@CommandHandler` made idempotent) makes the call, and the external system's answer
comes back as an ordinary kraken event through a partner topic, a webhook or polling. The full model, the delivery guarantees,
the configuration and the metrics are in `../kraken/docs/02-messaging.md` ("Sagas and deadlines"); the decision and every
builder ruling are in `../kraken/docs/adr/0021-sagas-in-the-library.md`; a working example with a fake partner is in
[examples/README.md](examples/README.md) ("The postcard saga").

### A minimal saga

```kotlin
@Saga
class PaymentSaga {
    var orderId: String? = null                 // state: plain properties, persisted as JSON after each handled message
    var paymentRef: String? = null
    var timeoutDeadline: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: OrderPlaced, saga: SagaLifecycle, commands: SagaCommands, deadlines: DeadlineManager) {
        orderId = e.orderId
        paymentRef = "pay-${e.orderId}"
        saga.associateWith("paymentRef", paymentRef!!)             // later events are routed here by paymentRef
        commands.send(RequestCharge(paymentRef!!, e.amount))       // recorded now, sent after the commit
        timeoutDeadline = deadlines.schedule(Duration.ofMinutes(15), "charge-timeout")
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "paymentRef")
    fun on(e: ChargeSucceeded, deadlines: DeadlineManager) {
        timeoutDeadline?.let(deadlines::cancel)
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "sagaId")
    fun on(e: SagaCommandFailed, commands: SagaCommands) {
        commands.publish(OrderPaymentFailed(orderId!!, e.reason.name), key = orderId)
    }

    @EndSaga
    @DeadlineHandler("charge-timeout")
    fun timeout(commands: SagaCommands) {
        commands.send(CancelCharge(paymentRef!!))
    }
}
```

- The class needs a no-argument constructor and is found by scanning the application's packages (the packages of your
  `@SpringBootApplication` class). It is not a Spring bean. Put it in the service's own package, not in a library.
- `@SagaEventHandler(associationProperty = "orderId")` looks the event's `orderId` up as the association `orderId = <value>` among
  the live instances; every match handles the event. `keyName` renames the association, `associationHeader` routes by a message
  header instead, `payloadType` narrows a handler declared on a supertype.
- `@StartSaga` creates an instance when no live instance matched the event; with `forceNew = true` it always does. At most one live
  instance exists per start association otherwise.
- Handler parameters after the event: `SagaLifecycle`, `SagaCommands`, `DeadlineManager`, `MessageContext` and any Spring bean by
  type, in any order. A `@DeadlineHandler`'s first parameter, when it is not one of those, receives the deadline's payload.
- Handlers are plain (non-`suspend`) functions: they run inside the saga's thread-bound Mongo transaction. Several handlers of one
  event type run in **method-name order**.
- `commands.send` and `commands.publish` only record the message; it leaves **after** the transaction committed, in call order,
  at least once, with the header `idempotency-key = <sagaId>:<seq>`. A saga does not wait for a reply.
- State, associations, scheduled deadlines and recorded messages are written together, once, after the handlers returned. An
  exception rolls all of it back and the event is retried (5 attempts), then parked (see [Operating sagas](#operating-sagas)).
- The startup fails, naming the class and method, for a suspend handler, a saga without a `@StartSaga` handler, an association
  property that does not exist on the event, a handler parameter nothing can fill, and a state the library's codec cannot write
  and read back.

### An idempotent adapter

The saga's `RequestCharge` is handled by an adapter in the same service. `@IdempotentHandler` records the call in
`external_calls` under the command's idempotency key, so a command the relay sends twice (a crash between sending and marking it
sent) reaches the external system once:

```kotlin
@Component
class PaymentAdapter(private val gateway: PaymentGateway) {

    @CommandHandler
    @IdempotentHandler                                             // key: the idempotency-key header (<sagaId>:<seq>)
    fun charge(cmd: RequestCharge, ctx: MessageContext) {
        val key = checkNotNull(ctx.headers["idempotency-key"])
        try {
            gateway.charge(cmd.paymentRef, cmd.amount, idempotencyKey = key)        // pass the key on when the partner takes one
        } catch (e: GatewayUnavailable) {
            throw RetryableExternalException("gateway down: ${e.message}", e)      // the relay backs off and calls again
        }
    }
}
```

- Throw `RetryableExternalException` for a failure a later attempt may cure (a timeout, a 503, a 429). **Do not retry inside the
  adapter**: the saga relay owns the backoff (`sagas.relay.*`), so retries never stack and no relay thread sleeps in your code.
  Any other exception is permanent: it is stored, repeats of the key throw it again, and the saga hears `SagaCommandFailed(FAILED)`.
- If the process dies in the middle of a call, the record stays `IN_PROGRESS`. After `external.calls.lease` (5 minutes) the
  outcome is unknown. By default (`externalIdempotent = false`) the call becomes `UNKNOWN`, is never repeated on its own and the
  saga hears `SagaCommandFailed(UNKNOWN_OUTCOME)`, so it decides (poll the partner, compensate, ask a person). If the partner
  de-duplicates by the key you pass it, declare `@IdempotentHandler(externalIdempotent = true)` and the call is repeated. Throw
  `ExternalCallUnknownOutcomeException` yourself when you see a connection break after the request went out: the record stays
  `UNKNOWN` and a redelivery reports it again. A call that ran but whose `SUCCEEDED` could not be written (the store went away)
  is an unknown outcome too, never a failed call (retryable with `externalIdempotent = true`). A call that outlives its lease
  no longer owns the record (its writes match on the random `owner` token it wrote, which a settled record no longer has): it
  never overwrites or deletes the record of the call that took over, gave it up or inserted it afresh after a reset, and a body that returned then reports an unknown outcome (retryable with `externalIdempotent = true`).
- `@IdempotentHandler(key = "'poll:' + message.jobId + ':' + message.attempt")` takes a SpEL key over `message` and `headers`.
- The handler must not be `suspend`. Use `eventBus.publishBlocking(...)` to publish a result.
- The interceptor and its collection exist when a class in the application's packages has an `@IdempotentHandler` method; set
  `kraken.messaging.external.calls.enabled=true` for an adapter outside them.
- A saga command carries **no user token**: it only reaches handlers without `@RequiresAuthentication` (the same limit as events
  published from handlers, see [Known limitations](#known-limitations)); otherwise the saga hears `SagaCommandFailed(DENIED)`.

### Every handler of a saga command must be idempotent

The relay retries after a timeout or a lost connection (the command may or may not have run, and a saga command carries an
`idempotency-key`, so a retry is safe for an idempotent target), and re-sends after a crash between sending and marking it sent.
**A handler of a saga command that is neither an `@IdempotentHandler` nor naturally idempotent can run twice** for one command.
Use `@IdempotentHandler`, or make the effect idempotent on `headers['idempotency-key']` yourself.

Two more things about the adapter:

- The body of an `@IdempotentHandler` **`@EventHandler`** runs inside the ordinary handlers' stage-1 Mongo transaction, so a slow
  call there holds that transaction open (MongoDB's lifetime limit, 60 s by default, applies). The `@CommandHandler` adapter the
  relay calls runs in no Mongo transaction: prefer it for anything slow.
- `idempotency-key` is just a header that any sender of the command can set, a hub client that is not a saga included. Keep
  adapters internal: do not expose their command types to services or clients you do not trust.

### Failed commands: `SagaCommandFailed`

A command that failed for good comes back to the saga as the event `SagaCommandFailed(sagaId, commandType, idempotencyKey, reason,
message)`, delivered locally (never published to `kraken.events`). `reason` is `NO_HANDLER`, `DENIED`, `INVALID`, `FAILED`,
`UNKNOWN_OUTCOME` or `EXHAUSTED` (transient failures that outlasted `sagas.relay.max-attempts`). The runtime delivers it to the
saga instance that sent the command **by its id**: the saga needs no association, only a handler
(`@SagaEventHandler(associationProperty = "sagaId")`, the name of the event's property; a saga that also associated itself with
`sagaId` keeps working). When the saga type has no handler for it, or the saga has ended, and no ordinary `@EventHandler` took
it either, it is logged at error and counted in `kraken_saga_command_failures_total`. A permanently failed step does not hold back
the saga's later steps.

### Hearing from the partner: a topic of its own

When the partner produces to **its own Kafka topic** (not `kraken.events`):

```yaml
kraken:
  messaging:
    external:
      kafka:
        sources:
          printer:                       # the source's name
            topic: printer.events
            # bootstrap-servers: partner-kafka:9092   # default: kraken.messaging.kafka.bootstrap-servers
            # properties: { security.protocol: SSL }  # extra consumer properties
```

```kotlin
@Component("printer")                    // the bean carries the source's name
class PrinterTranslator(private val codec: JsonCodec) : ExternalRecordTranslator {
    override fun translate(record: ConsumerRecord<String, ByteArray>): TranslatedEvent? {
        val n = codec.decode(record.value(), PrinterNotification::class)
        return TranslatedEvent(PostcardPrinted(n.jobId, n.name), dedupeKey = n.deliveryId)    // null ignores the record
    }
}
```

The library runs one consumer per source (group `<service-name>.external.<name>`, `earliest` for a new group), translates each
record and dispatches the event **locally**: the service's `@EventHandler`s and sagas, never `kraken.events`. The `dedupeKey` (the
partner's own id) makes two records for one event apply once; without it the key is `<name>:<partition>:<offset>`. A translator
exception retries the record up to `kafka.consumer.max-attempts` times, then logs and skips it. `properties` cannot set the
servers, the group id, the deserializers or auto-commit: such an entry fails the startup, naming it. `kafka-clients` is an `api` dependency of the starter, so the
translator's `ConsumerRecord` compiles without declaring it.

### Hearing from the partner: a webhook

The library gives the verifier and the ingress; the service writes the endpoint (authentication, body shape and response codes
differ per partner). The five-line controller:

```kotlin
@RestController
class PartnerWebhook(private val verifier: WebhookSignatureVerifier, private val ingress: ExternalEventIngress,
                     private val codec: JsonCodec, @Value("\${partner.webhook-secret}") private val secret: String) {
    @PostMapping("/partner/webhook")
    fun receive(@RequestBody body: ByteArray, @RequestHeader headers: HttpHeaders): ResponseEntity<Void> {
        if (!verifier.verify(secret, null, headers.toSingleValueMap(), body)) return ResponseEntity.status(401).build()
        val n = codec.decode(body, PartnerNotification::class)
        ingress.accept("partner", n.deliveryId, PostcardPrinted(n.jobId, n.name))   // ACCEPTED or DUPLICATE: both are a 200
        return ResponseEntity.ok().build()
    }
}
```

- `verify(secret, previousSecret, headers, body)` accepts the shared secret in `X-Kraken-Webhook-Secret` or the HMAC-SHA256 of the
  **raw** body in `X-Kraken-Signature` (hex, or `sha256=<hex>`); the previous secret is accepted during a rotation; constant-time
  comparison; an empty secret verifies nothing. Verify the raw bytes before parsing.
- `accept(source, dedupeKey, event, headers = emptyMap())` dispatches locally and **synchronously** (handlers, then sagas) under
  the id `<source>:<dedupeKey>`, so the partner's retries apply once (`DUPLICATE`). Use the partner's own delivery or event id as
  `dedupeKey`. A failure is thrown: answer 5xx and let the partner retry. An event no handler or saga takes is `ACCEPTED` with a
  warning. It blocks: call it from a request thread.

### Hearing from the partner: long polling

When the partner offers neither: the saga schedules a polling deadline, the deadline handler sends a poll command, the adapter
asks the partner and publishes a result event when there is one, and the saga reschedules or ends. The adapter's key includes the
poll attempt, so a poll sent twice asks once:

```kotlin
// in the saga
@DeadlineHandler("poll-printer")
fun poll(commands: SagaCommands, deadlines: DeadlineManager) {
    commands.send(PollPrintJob(jobId!!, ++polls))
    deadlines.schedule(Duration.ofSeconds(2), "poll-printer")
}
// in the adapter
@CommandHandler
@IdempotentHandler(key = "'poll:' + message.jobId + ':' + message.attempt")
fun poll(cmd: PollPrintJob) {
    val job = printer.status(cmd.jobId) ?: return
    if (job.done) events.publishBlocking(PostcardPrinted(job.jobId, job.name), key = job.name)    // the saga ends on this event
}
```

Give every waiting saga a timeout deadline of its own, so an answer that never comes ends it: the postcard saga does.

### Testing a saga

`SagaTestFixture` (in `kraken-messaging-test`, `testImplementation`) drives one saga with events and time, in memory: no Kafka,
no MongoDB, no Spring context. It runs the library's own saga runtime over in-memory storage, so a saga that passes behaves the
same on MongoDB; every step encodes and decodes the saga state as JSON, so state the codec cannot carry fails the test.

```kotlin
class PaymentSagaTest {
    private val t0 = Instant.parse("2026-11-03T10:00:00Z")

    @Test
    fun `a charge that never succeeds is cancelled when the deadline falls due`() {
        SagaTestFixture(PaymentSaga::class)                             // beans a handler asks for go after the class: SagaTestFixture(PaymentSaga::class, someBean)
            .givenCurrentTime(t0)
            .whenEvent(OrderPlaced("o1", 42))
            .expectActiveSagas(1)
            .expectAssociation("paymentRef", "pay-o1")
            .expectDispatchedCommands(RequestCharge("pay-o1", 42))
            .expectScheduledDeadline("charge-timeout", t0.plus(Duration.ofMinutes(15)))
            .whenTimeElapses(Duration.ofMinutes(15))
            .expectDispatchedCommands(RequestCharge("pay-o1", 42), CancelCharge("pay-o1"))   // everything sent since the last given…
            .expectSagaEnded()
    }

    @Test
    fun `a command that failed for good ends the saga and says so`() {
        SagaTestFixture(PaymentSaga::class)
            .whenEvent(OrderPlaced("o2", 10))
            .whenCommandFailed(RequestCharge("pay-o2", 10), SagaCommandFailed.Reason.UNKNOWN_OUTCOME, "gateway crashed")
            .expectPublishedEvents(OrderPaymentFailed("o2", "UNKNOWN_OUTCOME"))
            .expectNoScheduledDeadlines()
            .expectSagaEnded()
    }
}
```

Steps: `givenEvents(...)`, `givenCurrentTime(instant)`, `whenEvent(event, headers = emptyMap())`,
`whenCommandFailed(command, reason, message = "")` (delivers the `SagaCommandFailed` the relay would for a command the saga sent),
`whenTimeElapses(duration)` (fires every deadline that falls due on the way, each at its own due time, so a polling deadline fires
once per interval). Expectations: `expectActiveSagas(n)`, `expectAssociation(key, value)`, `expectDispatchedCommands(...)`,
`expectPublishedEvents(...)`, `expectScheduledDeadline(name, at)`, `expectNoScheduledDeadlines()`, `expectSagaEnded()`. The dispatch
expectations cover what was sent since the last `given…` step, in order. A handler that throws fails the step with an
`IllegalStateException`; a failed expectation is an `AssertionError`. The adapter and the controller are tested the usual way (an
adapter against a fake of the partner, the controller with a mock request and a computed signature, which
`WebhookSignatureVerifier.hmacHex(secret, body)` gives you); the example has both.

### Operating sagas

- **A parked event.** An event whose saga handler failed all its attempts is written to `saga_failures` (with its marker, so the
  consumer moves on), counted in `kraken_saga_events_total{outcome="parked"}` and logged at error. Inject `SagaFailures` and call
  `list(type?)`, `retry(id)`, `retryAll(type)` or `discard(id)` once the cause is fixed (a retry of a saga type this service does
  not run throws `IllegalStateException` and leaves the event as it was). Nothing retries a parked event by itself;
  one nobody retries or discards is deleted `sagas.failed-retention` (30 days) after its last failure, as is a `saga_dispatch`
  entry given up on.
- **Metrics** (with a `MeterRegistry` bean, in a service that uses sagas or idempotent handlers): the saga rows of
  [Metrics](#metrics). A `kraken_saga_events_total{outcome="no_match"}` that grows means an association that does not line up.
  Logs carry `sagaType`, `sagaId` and `correlationId` in the MDC while a saga handles a message.
- **Deadlines** fire at least once, after their due time, from any instance of the service (a lease makes each fire on one), also
  after a restart, and never for a cancelled deadline or an ended saga.
- Configuration: the `sagas.*`, `deadlines.*` and `external.*` rows of the [configuration table](#configuration).

### Gotchas

- **The translator bean carries the source's name** (`@Component("printer")` or a `@Bean` method named `printer`). A source with
  no such bean fails the startup naming it. For the same reason do not name another bean `printer` (the example's client bean is
  called `printerClient`).
- **Partner topics are not created by the library**, and the sandbox's Kafka does not auto-create topics: create the partner's
  topic first (the examples have a one-shot `printer-topic-init` for `printer.events`).
- **Use the library's `JsonCodec` bean** to read a webhook body or a partner record. It is the codec that writes your messages and
  your saga's state, with the same conventions; an `ObjectMapper` of your own may read the same JSON differently.
- **A saga only sees events consumed after it is deployed.** There is no replay of past events into a new saga type.
- **Ordinary `@EventHandler`s run first, and a failing one does not hide the event from the sagas.** The saga stage runs even
  when an ordinary handler failed, and the failure is then thrown, so the consumer retries and then dead-letters the record by its
  mode ([Dead letters](#dead-letters)); the sagas have applied the event once by then (their own markers make the redeliveries safe).
- **A step that must not run after the step before it failed** is sent from the handler of the event that says the previous step
  succeeded, not recorded in the same handling: the relay sends a saga's later steps after a permanently failed one.
- **Leases need synchronised clocks.** The relay, deadline and `external_calls` leases are timestamps compared across instances;
  keep the clocks in step (NTP).
- **A partner that recreates its topic needs a `dedupeKey`.** Without one the dedupe id is `<name>:<partition>:<offset>`; a recreated
  topic starts again at offset 0 and its records collide with the markers of the old ones for up to `idempotency.retention` (30
  days) and are skipped silently.
- **A saga sends through `SagaCommands` only.** A handler parameter of type `CommandBus`, `QueryBus`, `EventBus`, `RawCommandSender`,
  `RawEventPublisher` or `HubBus` (or a subclass or implementation of one) fails the startup: it would send at once, outside the
  saga's transaction, with no relay ordering, retry or idempotency key.
- **Saga commands have no caller.** A target with `@RequiresAuthentication` refuses them (`DENIED`).
- **Handlers of one event type run in method-name order**, not declaration order: name them so that order is the one you want.
- **Saga state is JSON.** Use `var` properties with defaults (or nullable ones) and types the library's codec can write; the
  startup check round-trips a fresh instance. Do not keep a Spring bean or a client in a saga: take it as a handler parameter.
- **At most 999 messages** can be sent or published by one handling.
- **A saga is for one service.** To talk to another service send it a command or publish an event; a saga never reads another
  service's database.

## Metrics

With Micrometer and a `MeterRegistry` bean (Boot's actuator brings both) the starter registers the meters below; without a
registry nothing is recorded. The names are the series as Prometheus shows them (dots become `_`, a unit adds its suffix,
counters end in `_total`), **identical to the Rust library's** (ADR 0026), so one dashboard serves both. Every label value
comes from a bounded set: a processor, a registered type, a saga type, a deadline name, a kind, an outcome; never an id.
Histograms are `_bucket{le}`, `_sum` and `_count`, with the hub's bucket boundaries (1 ms to 60 s).

| Series | Type | Labels | Meaning |
|---|---|---|---|
| `kraken_outbox_pending` | gauge | | Outbox documents not sent and not given up on, counted by the relay thread every 5 s. Every instance reports the same database: aggregate with `max by (job)`. |
| `kraken_outbox_oldest_age_seconds` | gauge | | Age of the oldest of them as the relay's latest poll saw it (it grows while the relay is stuck), 0 when none. |
| `kraken_outbox_published_total` | counter | `outcome` (`ok`, `failed`) | Send attempts; a retried document counts each attempt. |
| `kraken_events_consumed_total` | counter | `processor`, `type`, `outcome` | Each record's final outcome in each processor: `handled`, `duplicate` (its marker existed, or its own dead letter did), `skipped` (a type the processor does not handle), `parked` (handed to the dead letters, mode `collection` or `topic`), `failed` (given up without a letter: mode `log`, or only the saga stage failed). `type` is `other` for a type the processor does not handle or a record without one. |
| `kraken_event_handler_duration_seconds` | histogram | `processor`, `type` | One stage-1 run (the marker transaction with the processor's handlers) of a consumed record, every attempt; not the saga stage. |
| `kraken_processor_lag` | gauge | `processor`, `partition` | The lag the processor status reports, per partition assigned to this instance; a partition whose lag is not known yet has no series. A partition that leaves the assignment (or whose processor goes) reads 0 until an export read it, then has no series. Prometheus keeps a series that stops at its last value for 5 minutes, so aggregate with the max per partition first: `sum by (job, processor) (max by (job, processor, partition) (kraken_processor_lag))`. |
| `kraken_processor_state` | gauge | `processor`, `state` | 1 for the current state (`starting`, `running`, `paused`, `failing`, `stopped`), 0 for the other four. |
| `kraken_dead_letters` | gauge | `processor` | The processor's dead letters as the status last counted them (mode `collection`; 0 otherwise). |
| `kraken_hub_client_request_duration_seconds` | histogram | `kind` (`command`, `query`), `type`, `outcome` | One unary command or query call through the hub, from the call to its result, retries included (saga commands too). `outcome`: `ok`, the hub's `no_handler`, `timeout`, `handler_disconnected`, `handler_error`, `invalid_request`, `resource_exhausted`, `cancelled`, `denied`, or `unavailable` (no live hub, or the stream broke), `error` (also a request that cannot be encoded). Local handlers, scatter-gather, streaming and subscription queries are not observed. |
| `kraken_hub_client_handlers_in_flight` | gauge | | Incoming hub requests being handled or waiting for a handler permit, over all hubs. |
| `kraken_hub_client_connected_hubs` | gauge | | Hubs with an acknowledged stream. |
| `kraken_hub_client_token_fetches_total` | counter | `result` | Access token fetches (JWT only, [JWT](#authenticating-with-a-jwt-keycloak-client-credentials)). |
| `kraken_hub_client_certificate_rotations_total` | counter | | Client certificate pairs adopted (mTLS only). |
| `kraken_hub_client_credentials_refreshes_sent_total` | counter | | `ClientMessage.credentials` written to the hubs. |
| `kraken_sagas_started_total`, `kraken_sagas_ended_total` | counter | `type` | Saga instances started and ended. |
| `kraken_sagas_active` | gauge | `type` | Live saga instances, counted every 15 s. |
| `kraken_saga_events_total` | counter | `type`, `outcome` (`handled`, `no_match`, `conflict`, `parked`) | One (event, saga type) handling. |
| `kraken_saga_dispatch_pending` | gauge | | Commands and events saga handlers recorded that the relay has not dispatched yet, counted every 15 s. |
| `kraken_saga_command_failures_total` | counter | `type`, `reason` | A `SagaCommandFailed` nobody handled (`reason` as the wire enum, e.g. `NO_HANDLER`). |
| `kraken_deadlines_fired_total` | counter | `name` | Deadlines that ran their handler. |
| `kraken_deadlines_due` | gauge | `name` | Scheduled deadlines past their due time. |
| `kraken_external_calls_total` | counter | `handler`, `outcome` (`succeeded`, `replayed`, `failed`, `retryable`, `unknown`) | Calls of an `@IdempotentHandler`. |

Nothing on the hot paths waits for a metric: the hooks count in memory, and the gauges read values a background thread
already sampled (the outbox count on the relay's thread, the processor gauges every 5 s from the status the instance
reports to the hub, which refreshes offsets and dead-letter counts on its own thread). An application's own
`MessagingMetrics` bean replaces the Micrometer one.

**Exporting.** Use Boot's OTLP metrics export: add `io.micrometer:micrometer-registry-otlp` and
`org.springframework.boot:spring-boot-opentelemetry` (or `spring-boot-starter-opentelemetry`, which adds tracing) and
point it at the collector:

```yaml
management:
  otlp:
    metrics:
      export:
        url: http://otel-lgtm:4318/v1/metrics   # OTLP/HTTP
        step: 60s
```

The starter makes these defaults, below every setting of the application, and they apply to the whole application, its
own meters and traces included: `management.otlp.metrics.export.base-time-unit` `seconds` (Boot's default, milliseconds,
would turn the timers into `..._milliseconds` series; the application's own timers, `http.server.requests` for one, are
exported in seconds too), `management.otlp.metrics.export.publish-max-gauge-for-histograms` `false` (no `..._max_seconds`
series, which the Rust library does not have, for the application's timers either), and the resource identity, which
Prometheus turns into the `job` and `instance` labels of every series (the dashboards select and aggregate on them) and
which the application's traces carry too:

- `service.instance.id` = `kraken.messaging.instance-id`, the instance id the hub shows, unless `OTEL_RESOURCE_ATTRIBUTES`
  names one. The id is resolved once at startup and written as a value, so a `${random.uuid}` is the same in the hub
  registration and the metrics; without `kraken.messaging.instance-id` it is the `service.instance.id` of
  `OTEL_RESOURCE_ATTRIBUTES`, else a fresh UUID.
- `service.name` = `kraken.messaging.service-name`, the service the hub shows, unless `OTEL_SERVICE_NAME` or
  `OTEL_RESOURCE_ATTRIBUTES` names the service or `spring.application.name` is set: then Boot's own choice stands (the
  variable, else the application name), so the application's traces keep their name. **Keep `spring.application.name`
  equal to `kraken.messaging.service-name`, or unset**: otherwise the metrics' `job` is not the `service` of the hub's
  series, and the dashboards cannot join them.

These defaults are needed because Boot lets `management.opentelemetry.resource-attributes.*` win over the `OTEL_*`
variables; the starter sets none where a variable or the application already names the attribute. Keep
`spring.application.group` unset: Boot would add it as `service.namespace`, and `job` would become `<group>/<service>`.
service-a and service-b of the [examples](#examples) export this way when switched on.

## Test helpers (`kraken-messaging-test`)

- `InMemoryBuses(typePrefix, userId)`: implements `EventBus`, `CommandBus` and `QueryBus` in memory for unit tests without
  Docker. Messages still pass through the JSON codec. `register(handlerBean)` adds handlers; `published` lists events.
  A scatter-gather returns one part from the registered handler (service and instance `in-memory`); a streaming query
  collects the registered streaming handler's flow (honouring `maxResults`). A subscription query (`subscribe`) answers
  its initial result from the registered handler and takes its updates from `buses.emitter`, a local `QueryUpdateEmitter`
  (`emit`, `complete`, `completeExceptionally`, filtered like the real one; `buses.openSubscriptions` counts the open
  ones): no credits, and an emit is delivered at once, not after the commit of a surrounding transaction. `maxResults`,
  `cancel()` and `take(n)` end it as against a hub.
- `KrakenContainers.hubMtls(directory, services, serverCertificate, serverKey, clientCa, clientAuth = "required")`: the hub image
  (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`) in mode `enforce` with `[[service]]` tables and a client CA (P5c). Issue the hub's own
  pair from the client CA with both usages, so the image's health check (which presents it under `required`) passes. Generate the CA
  and the certificates at test time; the starter's own tests use BouncyCastle in test scope for that (the JDK's `keytool` cannot give a
  URI SAN and a seconds-long validity at once).
- `InProcessHub`: in-JVM stand-in for the hub (`start()`, `channel()`, `dropInstance(...)` (also with a `Status`), `lastRequestHeaders()`,
  `lastRequest()`, `registrationsFrom(...)`, `lastRegisterFrom(...)`, `acksSentTo(...)`, `echoHeartbeats = false` to
  simulate a hub that stopped answering), for hub-client tests without a container. It answers every
  `Register` with a `RegisterAck` (`heartbeatInterval`, `heartbeatTimeout`, `replyTimeout` as default, `maxTimeout`,
  `typeTimeouts`); `emitTimeouts = false` emulates a hub that loses requests. Flow control as the real hub: a first
  `Register` without permits is `INVALID_ARGUMENT`, permits, a per-instance queue by priority (`queueCapacity`, full:
  `RESOURCE_EXHAUSTED`), `permitsGrantedBy(...)`, `permitMessagesFrom(...)`, `permitsAvailable(...)`, `queuedFor(...)`,
  `forwardedOrder()`; permits never exceed the session's cap (`initial_permits`, raised by `Permits.grant`). Multi-hub: `lastStatusFrom(...)` (with the processors), `statusMessagesFrom(...)`,
  `requestsFrom(...)`, `sessionsOf(...)`,
  and `pauseReading = true` stops reading every stream. Like the real hub it keeps one session per instance id: a new
  stream registering an id replaces the older one (`sessionsReplacedOf(...)`, `streamsOpenedBy(...)`;
  `replaceSessions = false` keeps both), and `ackDelay` delays each `RegisterAck` as network latency would. Query modes
  (P4a): shared query types, scatter-gather with parts, end, `max_results`, the deadline and a `Cancel` to every forwarded
  target of a gather that ends early, as the real hub; an unknown request mode is `INVALID_REQUEST`, and `inject(instanceId,
  message)` writes any server message to an instance's stream. Streaming queries (P4b): one target, credits
  (`uint32`) and `Flow`, parts and ends relayed, `max_results`, the idle timeout (paused while the credit is 0),
  `maxStreamDuration`, cancels both ways and the gates as the real hub (a unary `Reply` to the target is ignored),
  `dropParts` drops relayed parts by `seq` (the real hub's full-buffer drop), and `openStreams()`,
  `creditOf(requestId)`, `flowsFrom(instanceId)` and `cancelsFrom(instanceId)` observe it. Subscription queries (P4c):
  every live instance of the type is a target as they are at the open, the initial request goes out as `<id>#0` through a
  permit and the queue, updates are renumbered behind the initial part under the sender's credit and the buffer
  (`subscriptionBuffer`, 256: one more ends it), `max_results` counts updates, cancels fan out both ways, a target leaving is
  silent unless it was the last (`HANDLER_DISCONNECTED`), `maxSubscriptionsPerInstance` (1024) refuses with
  `RESOURCE_EXHAUSTED`, `maxSubscriptionDuration` (off) ends with `CANCELLED`, and the gates are the real hub's;
  `openSubscriptions()`, `subscriptionTargetsOf(instanceId)`,
  `subscriptionCreditOf(requestId)` and `bufferedUpdatesOf(requestId)` observe it. Processor control (P6b):
  `control(instanceId, request)` sends a `ControlRequest` to the instance's session and awaits its `ControlReply` from that
  session, `controlRepliesFrom(instanceId)` lists every reply.
- `InProcessHub.auth` (P5a): `HubAuth.enforce(tokens)`, `HubAuth.permissive(tokens)` or `HubAuth.OFF` (default), with `tokens` a
  map from the plain test token to `AclEntry(service, handle, send)` (patterns: exact, `prefix.*`, `*`), answers like the real
  hub: in `enforce` the first `Register` is closed with `UNAUTHENTICATED` (no credentials, an empty token, a `jwt` on a hub
  without `services`, an unknown token) or `PERMISSION_DENIED` (another service name, a command, query or shared query outside `handle`), a later `Register`
  adding a forbidden type likewise, and a request outside `send` gets an immediate `Error{PERMISSION_DENIED}` in every
  request mode (`UNAUTHENTICATED` if the token left `auth` meanwhile). `prefix.*` needs something after the dot (it does not
  match `prefix.` itself). `auth` may be replaced at runtime and, like the real hub on an ACL reload, replacing it in `enforce`
  closes the live sessions the new ACL no longer allows (`UNAUTHENTICATED` for a token that left, `PERMISSION_DENIED` for a
  service name or handled type it no longer grants; their in-flight requests fail as on any disconnect); set
  `closeSessionsOnAuthChange = false` to keep them and test the per-request refusal instead. Like the real hub (unless `auth` is `OFF`), a `Register` naming the instance id of a live session of another service is closed with `PERMISSION_DENIED` (`instance id … belongs to another service`) and the owner is left alone. `permissive` allows and records. `authViolations()` lists every violation (code, message, enforced, a monotonic timestamp, so tests can assert
  the spacing of reconnects) and `tokensSeenFrom(instanceId)` the token of every `Register` received, refused ones included.
- `InProcessHub` and JWT credentials (P5b): `HubAuth.enforce(tokens, services)` / `permissive(tokens, services)` take the `[[service]]`
  tables as `AclEntry(service, handle, send)`. A `jwt` credential whose `azp` is the registering service and that has such an
  entry is accepted with its rights; another `azp` or a service without an entry is `PERMISSION_DENIED`, a token that is not three
  segments with an `azp` is `UNAUTHENTICATED`, and without `services` the hub answers "jwt credentials are not configured". The
  signature and `exp` are **not** checked (use the real hub image for that). `jwtsSeenFrom(instanceId)` lists the JWTs of the
  `Register`s and `credentialsSeenFrom(instanceId)` the `jwt` or `token` of every `ClientMessage.credentials`, and a refresh is judged
  like a `Register`'s against the session's own service. `KrakenContainers.hubJwt(...)` starts the hub image under test with JWT
  authentication (issuer, JWKS URI, `[[service]]` ACL file, leeway 0 s, expiry grace 2 s).
- `TestCertificates.generate(dir, names)`: a throwaway self-signed certificate made at test time with the JDK's `keytool`
  (PEM certificate, also the CA a client trusts, and PKCS#8 key; valid for `localhost` and `127.0.0.1` by default).
- `KrakenContainers`: unstarted Testcontainers definitions. `kafka()`, `mongo()` (single-node replica set, needed for
  transactions), `hub()` from the hub image under test, `kraken-hub:dev` or the one named by `KRAKEN_HUB_IMAGE`, with ports 7000 and 7001
  (every hub test runs on it: the hub and this library ship together), `hub(env)` for it with extra `KRAKEN_HUB_*`
  settings, `hubAcl(dir, acl, authMode, tls, env)` for it with access control and TLS (it writes the ACL file, with the
  SHA-256 of each token, into `dir` and copies it and the `TestCertificates` into the container, with
  `KRAKEN_HUB_AUTH_MODE`, `KRAKEN_HUB_ACL_FILE`, `KRAKEN_HUB_TLS_CERT` and `KRAKEN_HUB_TLS_KEY` set), `hubJwt(...)` and
  `hubMtls(...)` (below), and `hubImageAvailable()`.
- `KrakenTestKafka(bootstrap, typePrefix, topic).awaitEvent<T>()`: reads an event back from the topic.
- `SagaTestFixture(SagaClass::class, beans…)`: drives one saga with events and time, in memory; see [Testing a saga](#testing-a-saga).

```kotlin
val buses = InMemoryBuses(typePrefix = "workouts")
buses.register(WorkoutHandlers(repo, buses))   // the handler publishes events through the same in-memory bus
val result = buses.sendBlocking(LogWorkout("legs"))
```

The end-to-end test (`TwoServicesEndToEndTest`) starts Kafka, MongoDB, the real hub and two Spring Boot services. It is
skipped when `kraken-hub:dev` (or the image named by `KRAKEN_HUB_IMAGE`) is not present in the local Docker daemon. Build the image from the kraken-hub repo:

```bash
docker build -t kraken-hub:dev .
```

## Examples

`examples/` holds two tiny Spring Boot services built on this starter: service-a sends a command and a query over
the hub and reacts to an event, service-b owns the command and the query and publishes the event. They run in
Docker Compose from local builds together with the real `kraken-hub` image, Kafka and MongoDB, and are driven with
`curl`. service-b also runs a saga against a fake partner (the postcard printer) over all three inbound channels; see
[Sagas](#sagas) and [examples/README.md](examples/README.md).

## Known limitations

- Hub authentication is a static token per service (P5a), a Keycloak client-credentials JWT (P5b) or a client certificate (P5c, mTLS).
  A static token is a long-lived secret: rotate it through `hub.token-file`. An access token that Keycloak revokes keeps working until
  it expires (minutes); a certificate until its `notAfter` or until the hub's `[[service]]` table is removed (no CRL, no OCSP: issue
  short-lived certificates). A client key must be unencrypted PKCS#8 PEM. With a single hub, a certificate rotation leaves the instance
  without a stream for a moment (see the rotation notes above).
- Event processors (P6a) are observed, not controlled: no pause, resume, reset or replay, and retry or discard of dead letters only
  through the service's own `DeadLetters` bean (the hub's control plane is P6b). Attempts and back-off are shared by all processors;
  one consumer thread per processor and instance.
- No leader election for the outbox publisher; with several instances delivery is at-least-once and ordering is per
  publishing instance.
- A stream pins one handler instance for its whole life: a restart of that instance fails the stream
  (`HANDLER_DISCONNECTED`), and the library never resumes it. A subscription's update targets are fixed when it opens
  (at-most-once updates): an instance that starts later is never added and the library never resubscribes, so a
  subscriber that must hear every instance resubscribes itself (opt-in auto-resubscribe is future work).
- Library metrics are limited to the credential counters of P5b and the saga toolkit's (see [Sagas](#sagas)), both Micrometer and optional; there are no metrics for the buses, the outbox or the consumer yet. OpenTelemetry comes from the application.
- Configuration metadata is hand-maintained in `META-INF/spring-configuration-metadata.json`.
- `TraceHeaders` is only active with Micrometer Tracing on the classpath.
- Trace context is injected on outbound messages (hub requests, outbox events) but not yet extracted on inbound hub
  requests or Kafka records, so a handler's spans do not join the caller's trace.
- No service-to-service identity yet: commands sent from event handlers, sagas or scheduled jobs carry no token, so they can
  only reach handlers without `@RequiresAuthentication` (a `kraken-system` client-credentials token is future work).
- Sagas: Kotlin only (the Rust crate follows, S2); a new saga type does not see events from before its deployment (no replay);
  parked saga events are retried by a person through `SagaFailures`; a saga belongs to one service.
- Sagas, reviewed and left as they are: the smoke test does not check that a restart replays nothing twice (the tests do).


## CI/CD pipeline

Defined in [`.gitlab-ci.yml`](.gitlab-ci.yml); runs on every push and merge request on
`eclipse-temurin:25-jdk`.

```mermaid
flowchart LR
    build["build<br/>gradlew assemble"] --> test["test<br/>gradlew test (dind)"] --> publish["publish-api · tag api/vX.Y.Z<br/>Package Registry"]
```

| Stage | Job | Runs | Produces |
|---|---|---|---|
| build | `build` | always | compiled modules |
| test | `test` | always, with a `docker:dind` service; the hub end-to-end test is skipped when the hub image is not reachable | JUnit report |
| publish | `publish-api` | only on tags `api/vX.Y.Z` | `kraken-messaging-api`, `kraken-messaging` and `kraken-messaging-test` at `X.Y.Z` in the Package Registry |

Consumers resolve the artifacts from the group endpoint
`https://nexus.damoreira.ml/repository/maven-public/`. No Docker image is built: this
is a library; the example services are built from source by the sandbox compose.

## Contributing

Branches `feat/<topic>`, `fix/<topic>`, `chore/<topic>` off `main`, merged by merge request with a
green pipeline; conventional commit messages. Design changes go to the umbrella repo first.
Conventions: [`../kraken/docs/04-repositories.md`](../kraken/docs/04-repositories.md).

## License

[MIT](LICENSE) © 2026 Guilherme D'Amoreira.
