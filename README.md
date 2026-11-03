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
- [Upgrading](#upgrading)
- [Using the starter](#using-the-starter)
- [Semantics worth knowing](#semantics-worth-knowing)
- [Multiple hubs](#multiple-hubs)
- [Scatter-gather](#scatter-gather)
- [Streaming queries](#streaming-queries)
- [Subscription queries](#subscription-queries)
- [Sagas](#sagas)
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

## Upgrading

**To sagas (S1).** Nothing breaks: a service with no `@Saga` class and no `@IdempotentHandler` method gets none of the new beans or
collections. What changes: `EventConsumer` now runs events through a shared `LocalEventDispatcher` (ordinary handlers first, as
before; then the saga stage); a record that no `@EventHandler` wants but a saga does is processed instead of skipped.
`kraken-messaging` exposes `kafka-clients` as an `api` dependency (the translator signature), and `kraken-messaging-test` depends
on spring-data-mongodb (`SagaTestFixture`). `SendOptions` gained `headers` (default empty; `authorization`, `traceparent`,
`tracestate`, `correlation-id` and `correlationId` are reserved). New keys: `sagas.*`, `deadlines.*` and `external.*`. New
collections, created on first start of a service that uses them: `sagas`, `saga_dispatch`, `saga_dispatch_leases`, `deadlines`,
`saga_failures`, `external_calls`. A `HandlerInvoker` built by hand takes an optional `IdempotencyInterceptor`; the
`EventConsumer` an optional `LocalEventDispatcher`.

**To client certificates (P5c).** Nothing breaks: a service that sets no `hub.tls.client-cert-file` behaves as before. New settings (both
or neither): `kraken.messaging.hub.tls.client-cert-file`, `kraken.messaging.hub.tls.client-key-file`; a client certificate implies TLS
and cannot be combined with a static token or OAuth2 (the startup fails naming both). `HubConnections.authStatus()` gained the kind
`CERTIFICATE` (an exhaustive `when` over `HubAuthKind` needs the new branch) and the fields `certificateFingerprint`,
`certificateExpiresAt`, `certificateRotations` (with defaults, so constructing `HubAuthStatus` still compiles). `HubTls` and `HubSettings`
gained `clientCertificate`; `HubConnections` three optional timing parameters. **Behaviour change for every service:** a unary send that
gets `NO_HANDLER` from one hub now tries the other live hubs before answering `NO_HANDLER` (when no hub has a handler the answer is the
same and as fast as before). `InProcessHub.dropInstance(instanceId, status)` ends a stream with a given status, and
`KrakenContainers.hubMtls(...)` starts the hub image with a client CA.

**To JWT authentication (P5b).** Nothing breaks: a service that sets no `hub.oauth2.token-uri` behaves as before (static token or
none). `Register` now declares the capability `credentials-refresh` (a hub that does not know it ignores it); with a static
token file the new token is also sent on live streams when the file changes. To switch a service to Keycloak, remove its
`hub.token`/`token-file` and set `hub.oauth2.*` (both together fail the startup). `InProcessHub` changes: `HubAuth.enforce` and
`permissive` take an optional `services` list; a `jwt` credential is now accepted when `services` is set (before, every `jwt` was
refused), and the ack lists `credentials-refresh` for a registrant that declared it. `HubSettings` gained `tokenProvider`.

**To hub access control and TLS (P5a).** Nothing breaks while the hub runs with `KRAKEN_HUB_AUTH_MODE=off` (the
default) or `permissive`, and without TLS: configuration and behaviour are as before. New settings (all optional):
`hub.token`, `hub.token-file`, `hub.tls.enabled`, `hub.tls.ca-file` and `hub.tls.server-name`; the environment forms are
`KRAKEN_MESSAGING_HUB_TOKEN` (relaxed binding) and, when the property is unset, `KRAKEN_HUB_TOKEN` (likewise
`..._TOKEN_FILE`). What changes for existing code:

- A stream the hub closes with `UNAUTHENTICATED` or `PERMISSION_DENIED` is now a **registration rejection**, like
  `ALREADY_EXISTS` and `INVALID_ARGUMENT` always were: `HubConnection.lastRejection` holds the status,
  `awaitRegistered` throws `HubRegistrationRejectedException`, reconnects wait the backoff cap (`hub.reconnect-backoff.max`)
  instead of restarting from the floor, and the rejection is logged at `ERROR` once per distinct message, then at `DEBUG`
  (that applies to the two older codes as well, which used to log an `ERROR` line per attempt). Before, these two codes
  were ordinary stream closures retried from the floor. `lastRejection` is cleared once a stream completes its handshake
  again, so a fixed token or ACL is not held against the connection.
- A request the hub answers `Error{PERMISSION_DENIED}` (type outside the token's `send`) was already an
  `AccessDeniedException` and is never retried; nothing changes there.
- A stream that fails before any handshake in the way a TLS mismatch fails (plain text to a TLS hub, TLS to a plain hub)
  now says so in the `WARN` line.
- Roll out in this order: hub in `permissive`, services with their tokens, watch the hub's violation counter, then `enforce`
  (see [Hub security](#hub-security-token-acl-and-tls)). A client with a token works against every older hub, which ignores it.
- API: `HubSettings` gained a last, defaulted parameter `token` (a `HubToken`, redacted in `toString`); `KrakenMessagingProperties.Hub`
  gained `token`, `tokenFile` and `tls`; the default `krakenHubChannelFactory` bean is now a `DefaultHubChannelFactory` built
  from `hub.tls.*` (an override bean of that name still replaces it); `HubChannelFactory` and `HubConnection(s)` are
  unchanged. `InProcessHub` gained `auth`, `KrakenContainers` gained `hubP5a(...)` and `TestCertificates` is new (see
  [Test helpers](#test-helpers-kraken-messaging-test)). `spring-boot-actuator` is an optional (`compileOnly`) dependency:
  with it on the classpath the token is masked in `configprops` and `env`.

**To subscription queries (P4c).** Nothing breaks: existing handlers, senders and configuration behave as before. On the
wire `Register` also declares the `subscription` capability (an older hub ignores it), so a P4b hub or older makes
`QueryBus.subscribe` throw `UnsupportedByHubException("subscription")` while everything else keeps working. New setting
`hub.max-subscriptions` (1024). Two beans are new and need no configuration: `SubscriptionRegistry` and the
`QueryUpdateEmitter` that services inject to push updates (both exist even with `hub.enabled=false`; emitting without
subscribers is a no-op). `QueryBus` gained `subscribe` with a default body (a custom `QueryBus` compiles and throws
`UnsupportedOperationException` until it overrides it); `HubTransport` gained `subscribe` the same way;
`HubConnection` and `HubConnections` gained two optional last constructor parameters (`subscriptions`,
`inboundSubscription`); `HandlerMethod` gained `subscription`; `HandlerBusyException` gained an optional `detail`;
`InProcessHub` serves subscriptions too (see [Test helpers](#test-helpers-kraken-messaging-test)). A subscription holds no
handler permit and does not count against `max-concurrent-handlers`.

**To streaming queries (P4b).** Nothing breaks: existing handlers, senders and configuration behave as before. On the
wire `Register` also declares the `streaming` capability (an older hub ignores it). Two behaviours are new for
existing code: the hub may now send `Cancel` for a unary handler whose answer is no longer wanted (a scatter-gather
target past `maxResults` or past the deadline, or whose sender disconnected), which this library honours by cancelling
the handler's coroutine (a suspending handler sees a `CancellationException` at its next suspension point; a blocking,
non-`suspend` handler is **not** interrupted, it runs to its end and its reply is dropped; a handler that must not be
interrupted mid-way wraps that part in `withContext(NonCancellable)`); and the permit of an incoming request goes back
to the hub when its handler coroutine completes, whichever way. New setting `hub.stream-credit-timeout` (10 min): a
streaming handler waiting that long for credit ends its stream with `TIMEOUT`. `QueryBus` gained `stream`
with a default body (a custom `QueryBus` compiles and throws `UnsupportedOperationException` until it overrides it);
`HubConnection` and `HubConnections` gained an optional last constructor parameter `inboundStream`; `HubSettings` gained
`streamCreditTimeout` (defaulted); `HubErrors.toException` gained an optional `feature` parameter; `HandlerMethod`
gained `streaming`.

**To scatter-gather (P4a).** Nothing breaks: existing handlers, senders and configuration behave as before. What
changes on the wire: `Register` also declares the `scatter-gather` capability (an older hub ignores it) and lists
`@QueryHandler(shared = true)` types in `shared_query_types`. Before marking a type shared, make sure every hub runs P4a
(a P3 hub ignores `shared_query_types`, so the type is simply not registered there) and switch every owner at once:
the hub refuses a mix of exclusive and shared registrants (`ALREADY_EXISTS`). `QueryBus` gained `scatterGather` and
`scatterGatherFlow` with default bodies, so a custom `QueryBus` implementation still compiles (it throws
`UnsupportedOperationException` until it overrides them). `RemoteHandlerException` gained a `code` property (default
`HANDLER_ERROR`).

**To the multi-hub client (P3).** The version stays `0.1.0-SNAPSHOT` (nothing has been published beyond local
`mavenLocal()` installs yet), so re-run `./gradlew publishToMavenLocal` here and rebuild the services. Two beans changed
type:

- The hub transport bean is `HubConnections` (one connection per hub), no longer a single `HubConnection`. Replace
  `context.getBean(HubConnection::class.java)` with `context.getBean(HubConnections::class.java)`; `awaitRegistered`,
  `register`, `connected` and `instanceId` keep their meaning, and `connections()` lists the per-hub `HubConnection`s.
- `krakenHubChannelFactory` is a `HubChannelFactory` (`target -> ManagedChannel`, called with each hub's `host:port`)
  instead of `() -> ManagedChannel`. An override bean of that name must take the target, for example
  `HubChannelFactory { _ -> InProcessChannelBuilder.forName(hub.name).build() }` for one in-process hub.

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
| `hub.enabled` | `true` | Connect to the hub and expose the hub-backed `CommandBus`/`QueryBus` (addition beyond the spec table). |
| `hub.address` | `static://kraken-hub:7000` | The hub or hubs: one gRPC target (`host:port`, optional `static://` prefix), a comma-separated list of them (`hub-1:7000,hub-2:7000`, one connection per hub), or one `dns:///name:port` whose addresses are re-resolved every 30 s. See [Multiple hubs](#multiple-hubs). A malformed value fails the startup. |
| `hub.status-interval` | `5s` | Period of the `InstanceStatus` report (connected hub ids, handlers in flight) sent to every hub whose ack lists `instance-status`; also sent whenever the set of live hubs changes. Must be positive. |
| `hub.request-timeout` | unset | Per-request timeout sent as `timeout_ms`. Unset: a v2 hub applies its default (5 s) or its per-type override (`KRAKEN_HUB_TIMEOUT_OVERRIDES`), a legacy hub gets 5 s. Also bounds the wait for a hub stream (5 s when unset). `0s` counts as unset. |
| `hub.heartbeat-interval` | `10s` | Fallback heartbeat period, used only when the hub does not announce one (legacy hubs); a v2 hub announces 3 s in its `RegisterAck`. |
| `hub.reconnect-backoff.min` | `500ms` | Initial reconnect delay when the stream drops; doubles per failed attempt. Must be positive and not above `max`, or the startup fails naming the property. |
| `hub.reconnect-backoff.max` | `5s` | Maximum reconnect delay. Every wait is jittered by ±20 % and capped at this value (at the cap: 80-100 % of it); the delay resets to `min` once an attempt reached the hub's handshake. Must be positive (a zero would hot-loop handshakes against a refusing hub): checked at binding, a bad value fails the startup naming the property. |
| `hub.max-subscriptions` | `1024` | Subscription queries this instance serves at once, over every hub (at least 1). The next one is refused with `RESOURCE_EXHAUSTED` ("subscription limit (N) reached"), which the subscriber sees as a `HandlerBusyException`. A subscription holds no handler permit, so `max-concurrent-handlers` does not bound it; the default equals the hub's own `KRAKEN_HUB_MAX_SUBSCRIPTIONS_PER_INSTANCE`. |
| `hub.stream-credit-timeout` | `10m` | The longest a streaming handler of this instance waits for credit from the collector before it ends the stream with `TIMEOUT` ("no credit within ...") and frees its handler slot and hub permit (a hub that forgot the stream would otherwise leave them held forever). Must be positive; the default is the hub's default maximum stream duration (`KRAKEN_HUB_MAX_STREAM_DURATION`). |
| `hub.token` | none | Static token sent in `Register.credentials.token` of **every** hub stream (hub access control, [Hub security](#hub-security-token-acl-and-tls)). Optional; an empty or blank value is the same as none. Never logged, redacted in `toString` and in the actuator's `configprops` and `env`. Environment: `KRAKEN_MESSAGING_HUB_TOKEN`, or `KRAKEN_HUB_TOKEN` when this key is unset. |
| `hub.token-file` | none | File holding the token (a container secret such as `/run/secrets/kraken-hub-token`; surrounding whitespace ignored). Read at startup (an unreadable file fails it, naming the file) and **again on every reconnect**, so a rotated secret is picked up without a restart. After a first non-empty read, an unreadable or empty file (a non-atomic rotation caught half way) keeps the last token read and logs one `WARN`; at startup an empty file means no token. **Wins over `hub.token` when both are set.** Environment: `KRAKEN_MESSAGING_HUB_TOKEN_FILE`, or `KRAKEN_HUB_TOKEN_FILE` when this key is unset. |
| `hub.tls.enabled` | `false` | Talk TLS to the hub, for every `hub.address` form (a list of targets and `dns:///`). Off: plain text, as before. The hub must serve TLS on that port (`KRAKEN_HUB_TLS_CERT` / `KRAKEN_HUB_TLS_KEY`). |
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
| `kafka.consumer.max-attempts` | `5` | Handler invocations per record (first try included) before the record is logged and skipped. |
| `outbox.enabled` | `true` | Run the outbox publisher that relays `outbox` documents to Kafka. |
| `outbox.poll-interval` | `200ms` | Outbox poll period. |
| `outbox.batch-size` | `100` | Documents per poll. |
| `outbox.retention` | `7d` | Sent outbox documents older than this are deleted (TTL index). |
| `outbox.max-attempts` | `10` | Send attempts per outbox document before it is marked failed (`failedAt`) and skipped. |
| `idempotency.retention` | `30d` | `processed_events` documents older than this are deleted (TTL index). |
| `security.forward-jwt` | `true` | Put the current request's bearer token in the hub request's `authorization` header. |
| `sagas.enabled` | `true` | Switch for the saga runtime, the dispatch relay and the deadline scheduler. They exist only when the application has a `@Saga` class (found by scanning its packages) anyway. See [Sagas](#sagas). |
| `sagas.max-attempts` | `5` | Attempts per saga event (first try included) for version conflicts, start-key duplicates and handler exceptions before the event is parked in `saga_failures`; also the bound for firing a deadline. Backoff 200 ms doubling to 5 s (not configurable). |
| `sagas.ended-retention` | `30d` | Ended sagas are deleted this long after they ended (TTL index on `sagas.endedAt`). Must be positive. |
| `sagas.dispatch-retention` | `7d` | Dispatched `saga_dispatch` entries are deleted this long after they were sent (TTL index on `dispatchedAt`). Must be positive. |
| `sagas.relay.poll-interval` | `500ms` | How often the dispatch relay looks for entries that saga handlers recorded. Must be positive. |
| `sagas.relay.lease` | `30s` | How long a relay instance holds one saga's dispatch order before another instance may take over (a crashed instance's lease expires); renewed before each entry. Must be positive. |
| `sagas.relay.max-attempts` | `10` | Attempts per command for transient failures (first try included, at least 1) before it becomes `SagaCommandFailed(EXHAUSTED)`. |
| `sagas.relay.initial-backoff` | `1s` | Wait before the second attempt of a command; doubles per attempt up to `max-backoff`. |
| `sagas.relay.max-backoff` | `5m` | Upper bound of one wait between attempts of a command. |
| `deadlines.poll-interval` | `1s` | How often the deadline scheduler looks for due deadlines. Must be positive. |
| `deadlines.lease` | `30s` | How long a claimed deadline stays reserved for the instance that claimed it; an expired claim lets it fire again (at least once). Must be positive. |
| `deadlines.retention` | `7d` | Fired and cancelled deadlines are deleted this long after (TTL index on `deadlines.finishedAt`). Must be positive. |
| `external.calls.enabled` | unset | Whether the idempotency interceptor and the `external_calls` indexes exist. Unset: they exist when a class in the application's packages has an `@IdempotentHandler` method. `true` when the adapter lives outside those packages, `false` to switch it off. |
| `external.calls.lease` | `5m` | How long an `IN_PROGRESS` external call may take before its outcome counts as unknown (a crash in the middle of the call). Must be positive. |
| `external.calls.retention` | `30d` | `external_calls` documents older than this are deleted (TTL index on `updatedAt`). Must be positive. |
| `external.kafka.sources.<name>.topic` | required | A partner's topic, consumed into local kraken events (never republished to `kraken.events`). Each source needs an `ExternalRecordTranslator` bean named `<name>`; a source without one fails the startup naming it. |
| `external.kafka.sources.<name>.bootstrap-servers` | `kafka.bootstrap-servers` | The partner's cluster. |
| `external.kafka.sources.<name>.group-id` | `<service-name>.external.<name>` | The source's consumer group. |
| `external.kafka.sources.<name>.properties.*` | none | Extra consumer properties (for example `security.protocol`); bootstrap servers, group id, deserializers and auto-commit are fixed by the library. `auto.offset.reset` is `earliest` unless set here. Record retries use `kafka.consumer.max-attempts`. |

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
sanitization the application configured. An empty token is no token (a v1-style `Register` without credentials, which
`off` and `permissive` hubs accept). **`token-file` wins when both are set** (it is the source that can rotate); the
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

**Rollout.** Hub first, clients second, `enforce` last (any order is safe because an older client or hub simply ignores
the credentials): (1) run the hub with `KRAKEN_HUB_AUTH_MODE=permissive` and an ACL file; (2) give every service its
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
  stream as `ClientMessage.credentials`, when the stream's ack lists `credentials-refresh` (every `Register` declares it, static
  token sessions too). The hub closes a JWT session at `exp` plus `KRAKEN_HUB_JWT_EXPIRY_GRACE` (30 s) without one. A stream
  whose ack lacks the capability gets nothing: its next reconnect uses the new token.
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
- With a static `hub.token-file` the same capability rotates the token without a reconnect: when the file's content changes the new
  token is sent as `ClientMessage.credentials` on streams that acked the capability (checked once per heartbeat).
- The client secret and the access tokens never appear in logs, exception messages, `toString()` or the actuator's `configprops`
  and `env` (the starter's `SanitizingFunction` masks `hub.oauth2.client-secret` and `KRAKEN_HUB_OAUTH2_CLIENT_SECRET`).

**What you can read.** `HubConnections.authStatus()` returns `kind` (`NONE`, `TOKEN`, `JWT`), the current token's `tokenExpiresAt`, the
`lastTokenError` (null once a fetch worked), `tokenFetches` by result and `credentialsRefreshesSent` (one process-wide counter: it never drops when a hub connection goes away): wire it into your own
health indicator. With Micrometer on the classpath the starter binds `kraken_messaging_hub_token_fetches_total{result}`
(`ok`, `http_error`, `oauth_error`, `io_error`, `bad_response`) and `kraken_messaging_hub_credentials_refreshes_sent_total`.

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
`kraken_messaging_hub_certificate_rotations_total`. Logs name a certificate only by that fingerprint and the service; no key material
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
  gets up to `max-attempts` invocations with a 1 s back-off; a record that still fails is logged and skipped. A payload
  that does not decode into the handler's class is skipped at once. There is no dead-letter topic.
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
- **Handshake.** `Register` announces protocol version 2 (`HubProtocol`), the `flow-control` capability and `client_version`
  `kraken-messaging/<version>`. A v2 hub answers with a `RegisterAck` (hub id, heartbeat interval and timeout, default,
  maximum and per-type request timeouts) before the echo of the first heartbeat; a hub whose echo arrives first is a
  legacy (v1) hub and the connection keeps the pre-v2 behaviour (10 s heartbeats, `timeout_ms` always sent).
  `HubConnection.handshake` tells which (per hub: `HubConnections.connections()`).
- **Timeouts.** Unless `hub.request-timeout` is set, requests go out with `timeout_ms = 0` and the hub decides (default
  or per-type override). The client gives up locally only when the hub never answers: after the hub's deadline bound
  from the ack (`min(timeout, override or max)`, or `override or max` for 0) plus 1 s, with `MessageTimeoutException`.
- **Dead connections.** A hub stream whose heartbeats stay unanswered for three heartbeat intervals (9 s against a v2
  hub) is closed and re-established; HTTP/2 keepalive pings (every 10 s, 5 s timeout, also without calls) run on the
  channel.
- **Lost connections.** A request the stream never accepted (including one still queued on the client when the stream
  broke) fails with `HubUnavailableException` (the handler did not run, retrying is safe; with several hubs it is sent to
  another hub first). A request written to a stream that broke before the answer fails with
  `HubConnectionLostException`: the outcome is unknown, as with a timeout.
- **Flow control.** `Register` declares the `flow-control` capability with `initial_permits = hub.max-concurrent-handlers`.
  A hub that shares it (its ack lists `flow-control`) forwards to this instance only against permits and queues the rest
  per instance, highest `Request.priority` first, each request keeping its own deadline; every completed handler pays
  one permit back on the stream the request arrived on. Permits go back at once while the hub holds at most
  `min(16, initial_permits / 2)` of them for the stream (so a single permit is never held back), otherwise batched (one
  `Permits` message per 16 grants or 50 ms). A slow handler therefore means latency, not timeouts or `handler busy`. Against a hub without the capability (P0 or older)
  nothing changes and no `Permits` are sent.
- **Busy handlers.** A full hub queue or session buffer answers `HandlerBusyException` (`RESOURCE_EXHAUSTED`): the
  request was rejected before any handler ran, so retrying later is safe. Older clients get `RemoteHandlerException`
  "handler busy" instead.
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
  heartbeat, reconnect backoff and legacy detection, and sends the identical `Register` on every one (re-sent on every
  reconnect). One target behaves exactly like the single-hub client.
- **Targets.** A comma-separated list (blank entries and repeats dropped, order kept), or a single `dns:///name:port`
  (port required, not combinable with other entries) resolved at start and every 30 s: a new address gets a connection,
  a vanished one first announces an empty registration, then is closed once idle (at most 30 s later); an address that
  comes back before its connection is closed revives that connection instead of dialing a second one. Only one address
  family is used (rule shared with the Rust library): the IPv4 addresses when the answer has any, else the IPv6 ones,
  repeats dropped; a dual-stack name lists every hub under both families, and two streams to one hub with the same
  instance id would replace each other. A failed or empty resolution keeps the current connections.
- **Hub identity.** A hub is its `RegisterAck.hub_id`; a legacy (v1) hub, or an ack with an empty id, is its target
  id: the bare `host:port` without `static://` (a resolved IPv6 address as `[ip]:port`, without zone; rule shared with
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
  applies as before. With no live hub at all: `HubUnavailableException`, as with one hub.
- **Permits.** A stream announces `ceil(hub.max-concurrent-handlers / N)` initial permits, where N = 1 (the hub being
  registered on) + the other hubs that are live or still on their first connect attempt (a hub down after a failed
  attempt is not counted). Whenever that count changes, every live stream is topped up to its current share:
  `Permits{count = max(0, ceil(max / N) - offered), grant = ceil(max / N)}`, offered = the stream's `initial_permits`
  plus its earlier top-ups; the grant raises the hub's permit cap on the session (otherwise its `initial_permits`) so
  the hub keeps the top-up (a hub that predates `Permits.grant` ignores it and clamps the top-up). So with two hubs the
  survivor of an outage (or of a hub that failed its first attempt) serves at full capacity, flaps never add up, a hub
  that joins or returns gets `ceil(max / N)` on its new stream, and nothing is ever revoked (rule shared with the Rust
  library). Locally at most `hub.max-concurrent-handlers` handlers run in total, whichever hub sent the requests. Permits are paid back per stream as before.
- **Readiness.** `HubConnections.awaitRegistered(timeout)` is true only once **every** kept hub confirmed the latest
  registration; while one configured hub is down it returns false at the timeout, although the instance is already
  served by the others (`connected` / `liveHubIds()` tell that). The Rust library's equivalent may differ; see its
  README.
- **Status.** Every `hub.status-interval`, and whenever the live set changes, each live hub whose ack lists the
  `instance-status` capability gets an `InstanceStatus`: `connected_hub_ids` (the ids of the live protocol v2 hubs) and
  `handlers_in_flight` (over all hubs). A hub without the capability (P2 or older, legacy) never gets one. The hub shows
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
- **Errors.** No owner: `NoHandlerException`; a command type: `InvalidMessageException`; a hub without the
  `scatter-gather` capability (a P3 or older hub, or a legacy v1 hub): `UnsupportedByHubException`, thrown before
  anything is sent. With several hubs a gather goes to one hub that lists the capability (round-robin); only when none
  of the live hubs does is it `UnsupportedByHubException`. If the stream to that hub breaks before the end the call
  fails with `HubConnectionLostException`.
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
  owner's instances on a shared type), chosen like a point query but only among the instances that negotiated
  `streaming`. A `StreamingQuery` is not a `Query`: it cannot be sent with `send` or gathered, and a unary request
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
  `InvalidMessageException`, a hub without `streaming` (a P4a or older hub, or a legacy v1 hub)
  `UnsupportedByHubException` on collection, before anything is sent; with several hubs the stream goes to one hub that
  lists the capability, and only when none of the live hubs does is it unsupported. Owners whose live instances all
  lack `streaming` are `UnsupportedByHubException` too (the hub's message names them). An end with an error fails the
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
  round-robin to another capable one), so no part is ever emitted twice. After the first part nothing is retried: the
  flow throws, a broken hub stream as `HubConnectionLostException`. The budget covers the attempts up to the first part
  (and the wait for a live hub); every attempt sends the chosen `timeout` whole as its idle timeout, never what is
  left of the budget.
- **Compatibility.** Against a P3 or P4a hub `stream` throws `UnsupportedByHubException` while unary queries and
  gathers keep working. Handlers must run this version: the hub streams only to instances that negotiated `streaming`,
  so during a rolling upgrade of an owner the old instances are simply never picked (owners with instances but none
  capable are `UnsupportedByHubException` for the caller).

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
  startup. The hub sends every capable instance of the type a `SUBSCRIPTION` request, and the library registers the
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
- **Targets, and the at-most-once caveat.** Every live instance of every owner that negotiated `subscription`, **as they are
  when the subscription opens**: an instance that registers later is never added (an update emitted on a new instance is
  lost to that subscriber), a target that leaves is not replaced, and the hub never resubscribes. After a rolling
  restart a subscription listens to fewer instances than serve the type, silently until the last one leaves
  (`HandlerDisconnectedException`); a subscriber that needs every instance resubscribes. One of the capable instances,
  chosen like a point query, answers the initial result. A subscription is passive: it holds no handler permit, waits in
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
  `InvalidMessageException`, no live hub (or owner instance) with `subscription` `UnsupportedByHubException("subscription")`
  before anything is sent, the initial request failing `RemoteHandlerException` / `MessageTimeoutException` /
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
  subscription is a new request with a fresh id (with several hubs, round-robin to another capable one), so no update is
  delivered twice. After the initial result nothing is retried; resubscribing is the caller's call (it gets a fresh
  initial result).
- **Compatibility.** Against a P4b hub or older `subscribe` throws `UnsupportedByHubException("subscription")` while
  unary queries, gathers and streams keep working. With several hubs a subscription goes to one hub that lists the
  capability. Handlers must run this version: the hub subscribes only instances that negotiated `subscription`, so during
  a rolling upgrade of an owner the old instances are simply never targets (owners with instances but none capable are
  `UnsupportedByHubException` for the caller).

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
        saga.associateWith("sagaId", saga.sagaId)                   // this saga hears about its failed commands
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
  `ExternalCallUnknownOutcomeException` yourself when you see a connection break after the request went out.
- `@IdempotentHandler(key = "'poll:' + message.jobId + ':' + message.attempt")` takes a SpEL key over `message` and `headers`.
- The handler must not be `suspend`. Use `eventBus.publishBlocking(...)` to publish a result.
- The interceptor and its collection exist when a class in the application's packages has an `@IdempotentHandler` method; set
  `kraken.messaging.external.calls.enabled=true` for an adapter outside them.
- A saga command carries **no user token**: it only reaches handlers without `@RequiresAuthentication` (the same limit as events
  published from handlers, see [Known limitations](#known-limitations)); otherwise the saga hears `SagaCommandFailed(DENIED)`.

### Failed commands: `SagaCommandFailed`

A command that failed for good comes back to the saga as the event `SagaCommandFailed(sagaId, commandType, idempotencyKey, reason,
message)`, delivered locally (never published to `kraken.events`). `reason` is `NO_HANDLER`, `DENIED`, `INVALID`, `FAILED`,
`UNKNOWN_OUTCOME` or `EXHAUSTED` (transient failures that outlasted `sagas.relay.max-attempts`). The saga must **associate itself
with its own id** to receive it, as in the saga above (`saga.associateWith("sagaId", saga.sagaId)` and
`@SagaEventHandler(associationProperty = "sagaId")`); the runtime does not do that for you. Unhandled, it is logged at error and
counted in `kraken.saga.command.failures`. A permanently failed step does not hold back the saga's later steps.

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
exception retries the record up to `kafka.consumer.max-attempts` times, then logs and skips it. `properties` cannot override the
servers, the group id, the deserializers or auto-commit. `kafka-clients` is an `api` dependency of the starter, so the
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
  consumer moves on), counted in `kraken.saga.events{outcome="parked"}` and logged at error. Inject `SagaFailures` and call
  `list(type?)`, `retry(id)`, `retryAll(type)` or `discard(id)` once the cause is fixed. Nothing retries a parked event by itself.
- **Metrics** (Micrometer, with a `MeterRegistry` bean in a service that uses sagas or idempotent handlers): `kraken.sagas.started`,
  `kraken.sagas.ended`, `kraken.sagas.active`, `kraken.saga.events{type,outcome}` (`handled`, `no_match`, `conflict`, `parked`),
  `kraken.saga.dispatch.pending`, `kraken.saga.command.failures`, `kraken.deadlines.fired`, `kraken.deadlines.due`,
  `kraken.external.calls{handler,outcome}`. A `no_match` count that grows means an association that does not line up. Logs carry
  `sagaType`, `sagaId` and `correlationId` in the MDC while a saga handles a message.
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
- **Associate the saga with its own id** (`saga.associateWith("sagaId", saga.sagaId)`) if it handles `SagaCommandFailed`.
- **A saga only sees events consumed after it is deployed.** There is no replay of past events into a new saga type.
- **Ordinary `@EventHandler`s run first, and a failing one does not hide the event from the sagas.** The saga stage runs even
  when an ordinary handler failed, and the failure is then thrown, so the consumer retries and then skips the record as it
  always did; the sagas have applied the event once by then (their own markers make the redeliveries safe).
- **Saga commands have no caller.** A target with `@RequiresAuthentication` refuses them (`DENIED`).
- **Handlers of one event type run in method-name order**, not declaration order: name them so that order is the one you want.
- **Saga state is JSON.** Use `var` properties with defaults (or nullable ones) and types the library's codec can write; the
  startup check round-trips a fresh instance. Do not keep a Spring bean or a client in a saga: take it as a handler parameter.
- **At most 999 messages** can be sent or published by one handling.
- **A saga is for one service.** To talk to another service send it a command or publish an event; a saga never reads another
  service's database.

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
  simulate a hub that stopped answering), for hub-client tests without a container. It speaks protocol v2 (a
  `RegisterAck` with `heartbeatInterval`, `heartbeatTimeout`, `replyTimeout` as default, `maxTimeout`, `typeTimeouts`);
  `legacy = true` emulates a v1 hub, `emitTimeouts = false` a hub that loses requests. Flow control as the real hub:
  permits, a per-instance queue by priority (`queueCapacity`), `RESOURCE_EXHAUSTED` or "handler busy" by sender capability,
  `permitsGrantedBy(...)`, `permitMessagesFrom(...)`, `permitsAvailable(...)`, `queuedFor(...)`, `forwardedOrder()`;
  permits never exceed the session's cap (`initial_permits`, raised by `Permits.grant`); `permitCap = false` drops the
  cap, `permitGrants = false` emulates a hub that predates the grant;
  `flowControl = false` emulates the P0 hub (v2 without flow control). Multi-hub: `instanceStatus = false` emulates the
  P2 hub (no `instance-status`), `lastStatusFrom(...)`, `statusMessagesFrom(...)`, `requestsFrom(...)`, `sessionsOf(...)`,
  and `pauseReading = true` stops reading every stream. Like the real hub it keeps one session per instance id: a new
  stream registering an id replaces the older one (`sessionsReplacedOf(...)`, `streamsOpenedBy(...)`;
  `replaceSessions = false` keeps both), and `ackDelay` delays each `RegisterAck` as network latency would. Query modes
  (P4a): shared query types, scatter-gather with parts, end, `max_results` and the deadline as the real hub;
  `scatterGather = false` emulates the P3 hub (no capability, `shared_query_types` ignored), and `inject(instanceId,
  message)` writes any server message to an instance's stream. Streaming queries (P4b): one capable target, credits
  (`uint32`) and `Flow`, parts and ends relayed, `max_results`, the idle timeout (paused while the credit is 0),
  `maxStreamDuration`, cancels both ways and the gates as the real hub; `streaming = false` emulates the P4a hub,
  `dropParts` drops relayed parts by `seq` (the real hub's full-buffer drop), and `openStreams()`,
  `creditOf(requestId)`, `flowsFrom(instanceId)` and `cancelsFrom(instanceId)` observe it. Subscription queries (P4c):
  every capable instance is a target as they are at the open, the initial request goes out as `<id>#0` through a
  permit and the queue, updates are renumbered behind the initial part under the sender's credit and the buffer
  (`subscriptionBuffer`, 256: one more ends it), `max_results` counts updates, cancels fan out both ways, a target leaving is
  silent unless it was the last (`HANDLER_DISCONNECTED`), `maxSubscriptionsPerInstance` (1024) refuses with
  `RESOURCE_EXHAUSTED`, `maxSubscriptionDuration` (off) ends with `CANCELLED`, and the gates are the real hub's;
  `subscription = false` emulates the P4b hub, and `openSubscriptions()`, `subscriptionTargetsOf(instanceId)`,
  `subscriptionCreditOf(requestId)` and `bufferedUpdatesOf(requestId)` observe it.
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
  signature and `exp` are **not** checked (use the real hub image for that). The ack offers `credentials-refresh` to a registrant
  that declared it (`credentialsRefresh = false` emulates a hub without it); `jwtsSeenFrom(instanceId)` lists the JWTs of the
  `Register`s and `credentialsSeenFrom(instanceId)` the `jwt` or `token` of every `ClientMessage.credentials`, and a refresh is judged
  like a `Register`'s against the session's own service. `KrakenContainers.hubJwt(...)` starts the hub image under test with JWT
  authentication (issuer, JWKS URI, `[[service]]` ACL file, leeway 0 s, expiry grace 2 s).
- `TestCertificates.generate(dir, names)`: a throwaway self-signed certificate made at test time with the JDK's `keytool`
  (PEM certificate, also the CA a client trusts, and PKCS#8 key; valid for `localhost` and `127.0.0.1` by default).
- `KrakenContainers`: unstarted Testcontainers definitions. `kafka()`, `mongo()` (single-node replica set, needed for
  transactions), `hub()` from image `kraken-hub:dev` or the one named by `KRAKEN_HUB_IMAGE`, `hubBaseline()` from the previous hub release
  `kraken-hub:v1-baseline` or `KRAKEN_HUB_BASELINE_IMAGE` and `hubP0()` from the P0 hub `kraken-hub:p0` or
  `KRAKEN_HUB_P0_IMAGE`, `hubP2()` from `kraken-hub:p2` or `KRAKEN_HUB_P2_IMAGE` (compatibility tests), `hubP3()` from
  `kraken-hub:p3-hub` or `KRAKEN_HUB_P3_IMAGE` with ports 7000 and 7001 (two-hub tests), `hubP4a()` from
  `kraken-hub:p4a-hub` or `KRAKEN_HUB_P4A_IMAGE` with ports 7000 and 7001 (scatter-gather tests), `hubP4b()` from
  `kraken-hub:p4b-hub` or `KRAKEN_HUB_P4B_IMAGE` with ports 7000 and 7001 (streaming tests), `hubP4c()` from
  `kraken-hub:p4c-hub` or `KRAKEN_HUB_P4C_IMAGE` with ports 7000 and 7001 (subscription tests), `hubP5a(dir, acl, authMode, tls, env)`
  from `kraken-hub:p5a-hub` or `KRAKEN_HUB_P5A_IMAGE` (access control and TLS: it writes the ACL file, with the SHA-256 of
  each token, into `dir` and copies it and the `TestCertificates` into the container, with `KRAKEN_HUB_AUTH_MODE`,
  `KRAKEN_HUB_ACL_FILE`, `KRAKEN_HUB_TLS_CERT` and `KRAKEN_HUB_TLS_KEY` set), `hub(env)` for the hub
  under test with extra `KRAKEN_HUB_*` settings, and `hubImageAvailable()` / `hubBaselineImageAvailable()` /
  `hubP0ImageAvailable()` / `hubP2ImageAvailable()` / `hubP3ImageAvailable()` / `hubP4aImageAvailable()` /
  `hubP4bImageAvailable()` / `hubP4cImageAvailable()` / `hubP5aImageAvailable()`.
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
- No dead-letter topic: poison records are logged and skipped.
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
`https://gitlab.damoreira.ml/api/v4/groups/kraken-org/-/packages/maven`. No Docker image is built: this
is a library; the example services are built from source by the sandbox compose.

## Contributing

Branches `feat/<topic>`, `fix/<topic>`, `chore/<topic>` off `main`, merged by merge request with a
green pipeline; conventional commit messages. Design changes go to the umbrella repo first.
Conventions: [`../kraken/docs/04-repositories.md`](../kraken/docs/04-repositories.md).

## License

[MIT](LICENSE) © 2026 Guilherme D'Amoreira.
