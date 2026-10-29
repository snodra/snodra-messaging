# kraken-messaging Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the `kraken-messaging` skeleton into the working Spring Boot 4.1 starter from `02-messaging.md`: typed `EventBus`/`CommandBus`/`QueryBus`, annotated handlers discovered by a `BeanPostProcessor`, commands and queries over the kraken hub gRPC stream with JWT forwarding and validation, events on Kafka through a MongoDB outbox with idempotent consumption, plus the `kraken-messaging-test` helpers.

**Architecture:** A `HandlerRegistry` built at startup is the single source of truth for what this service handles; a `HandlerInvoker` turns (type name, JSON bytes, headers) into a handler call and maps exceptions to hub `ErrorCode`s. Three transports plug into it: `HubConnection` (one grpc-kotlin bidirectional stream with pending-reply map, heartbeat and reconnect), `OutboxEventBus` + `OutboxPublisher` (Mongo `outbox` collection to Kafka), and `EventConsumer` (Kafka listener with `processed_events` idempotency inside a Mongo transaction). Auto-configuration wires everything from `kraken.messaging.*` properties. The test module ships an in-process hub and Testcontainers definitions so the starter's own tests run without the Rust binary, and one end-to-end test runs against the real `kraken-hub:dev` image when present.

**Tech Stack:** Kotlin 2.4.21, Spring Boot 4.1.2 (Jackson 3 `tools.jackson`, Spring Kafka 4.1.2, Spring Data MongoDB 5.1.2, Spring Security 7.1.2 oauth2-resource-server), grpc-java 1.85.1 + grpc-kotlin 1.5.0 + protobuf 4.36.2 via `com.google.protobuf` plugin 0.10.0, kotlinx-coroutines 1.10.2, Testcontainers 2.0.5, MockK 1.14.5, Gradle 9.8.2, JDK 25.

**Spec:** `../kraken/docs/02-messaging.md` (API, config keys, error table, Kafka layout, outbox, consumer, hub walkthrough), `../kraken/docs/09-observability-and-testing.md` (test module contents), `../kraken/docs/06-security.md` (JWT rules), `../kraken/protocol/hub.proto`.

## Global Constraints

- Group `io.damoreira.kraken`, artifacts `kraken-messaging-api`, `kraken-messaging`, `kraken-messaging-test`, version `0.1.0-SNAPSHOT`; `kraken-messaging-api` keeps zero third-party dependencies.
- Configuration keys and defaults exactly as the `02-messaging.md` table (`kraken.messaging.service-name` and `type-prefix` required; `hub.address=static://kraken-hub:7000`; `hub.request-timeout=5s`; `hub.heartbeat-interval=10s`; `hub.reconnect-backoff` min `500ms` max `30s`; `kafka.bootstrap-servers=broker:19092`; `kafka.topic=kraken.events`; `kafka.consumer.enabled=true`; `kafka.consumer.max-attempts=5`; `outbox.enabled=true`; `outbox.poll-interval=200ms`; `outbox.batch-size=100`; `outbox.retention=7d`; `idempotency.retention=30d`; `security.forward-jwt=true`; `security.require-jwt=true`).
- Wire type name: `@MessageType.value`, else `"$typePrefix.${simpleName}"`.
- JSON: camelCase, ISO 8601 UTC instants, unknown properties ignored, `Unit` responses serialised as `{}`.
- Kafka: producer `acks=all`, `enable.idempotence=true`; headers `type,id,correlationId,timestamp,source,userId,traceparent`; consumer group = service name, manual ack after the Mongo transaction commits.
- Hub headers: `authorization` (`Bearer <jwt>`), `correlationId`, `traceparent`, `tracestate`. Events never carry the JWT, only `userId`.
- Exceptions: `MessagingException` sealed root with `NoHandlerException`, `MessageTimeoutException`, `HandlerDisconnectedException`, `RemoteHandlerException(message, details: ByteArray)`, `UnauthenticatedException`, `InvalidMessageException`, `HubUnavailableException`, plus startup-time `DuplicateHandlerException` and `HubRegistrationRejectedException`.
- Every Gradle module compiles with `-Xjsr305=strict`; `./gradlew build` must pass with Docker running (Testcontainers).
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Builder call (this plan): the facades keep the skeleton's typed shape `send(command: Command<R>, routingKey: String? = null): R` instead of the spec's `(Any, KClass<R>)`; `R` is read at runtime from the `Command<R>`/`Query<R>` supertype argument of the message class. The spec's shape is recoverable later as an overload.
- Builder call (this plan): the hub channel is built with `ManagedChannelBuilder.forTarget(hostPort).usePlaintext()` after stripping an optional `static://` prefix; the library does not use Spring gRPC's `GrpcChannelFactory`. The hub is plaintext on the private network by design, and this removes a dependency on Spring gRPC's channel configuration model.
- Builder call (this plan): blocking Mongo (`MongoTemplate`, `MongoTransactionManager`). Suspend handlers are invoked with `KFunction.callSuspend` on the calling thread (`runBlocking` where the caller is blocking), so a thread-bound Mongo transaction wraps them correctly. Services run Spring MVC with virtual threads.
- Builder call (this plan): the library validates the forwarded JWT (signature, issuer, expiry) with the application's `JwtDecoder` bean and exposes `MessageContext.roles` from `realm_access.roles`; role authorisation stays in the handler. With `security.require-jwt=true` and no `JwtDecoder` bean the context fails to start.
- Builder call (this plan): `kraken.messaging.hub.enabled` (default `true`) is added beyond the spec table so publish-only tools and unit tests can run without a hub.
- Builder call (this plan): outbox publisher has no leader election (spec); W3C trace headers are injected only when a Micrometer `Tracer`/`Propagator` bean exists (`micrometer-tracing` is `compileOnly`).

## Review Focus

1. A handler method with two parameters where the second is not `MessageContext`, or with zero parameters, must fail startup with a clear message naming the bean and method, not be silently skipped. Test in Task 3.
2. A `Reply` whose payload is valid JSON but not the declared response type (contract drift) must surface as `InvalidMessageException` on the sender, naming the type, not as a bare Jackson exception. Test in Task 6.
3. A Kafka record whose `type` header is missing, or whose payload is not JSON, must be acknowledged and skipped with a warning, never block the partition forever. Test in Task 8.
4. `EventBus.publish` called inside a failing `@Transactional` method must leave no `outbox` document behind. Test in Task 7.
5. `CommandBus.send` while the hub is down must fail with `HubUnavailableException` after `request-timeout`, not hang, and must recover automatically once the hub returns. Test in Task 6.

---

### Task 1: Build wiring: proto codegen, Jackson 3, dependencies

**Files:**
- Modify: `kraken-messaging/build.gradle.kts`, `gradle/libs.versions.toml`, `settings.gradle.kts` (none unless a plugin repo is needed), `kraken-messaging-test/build.gradle.kts`
- Create: `kraken-messaging/src/main/proto/hub.proto` (verbatim copy of `../kraken/protocol/hub.proto`; delete `proto/README.md`'s claim that the proto is "wired later" and point it to this path)
- Test: `kraken-messaging/src/test/kotlin/io/damoreira/kraken/messaging/BuildSmokeTest.kt`

**Interfaces:**
- Produces: generated `io.damoreira.kraken.hub.v1.*` Java messages and `io.damoreira.kraken.hub.v1.HubGrpcKt` (`HubCoroutineStub`, `HubCoroutineImplBase`).
- Dependencies in `libs.versions.toml`: `jackson-module-kotlin = { module = "tools.jackson.module:jackson-module-kotlin" }` (Boot BOM manages the version); add `spring-security-oauth2-jose = { module = "org.springframework.security:spring-security-oauth2-jose" }`, `spring-security-oauth2-resource-server = { module = "org.springframework.security:spring-security-oauth2-resource-server" }`, `micrometer-tracing = { module = "io.micrometer:micrometer-tracing" }`, `grpc-inprocess = { module = "io.grpc:grpc-inprocess", version.ref = "grpc" }`, `grpc-services = { module = "io.grpc:grpc-services", version.ref = "grpc" }`, `nimbus-jose-jwt = { module = "com.nimbusds:nimbus-jose-jwt" }`, `awaitility = { module = "org.awaitility:awaitility-kotlin" }`.
- `kraken-messaging` dependencies: `api(project(":kraken-messaging-api"))`, `api(kotlinx-coroutines-core)`, `implementation`: spring-boot-starter, spring-kafka, spring-boot-starter-data-mongodb, jackson-module-kotlin, kotlin-reflect, grpc-stub, grpc-protobuf, grpc-kotlin-stub, grpc-netty-shaded, protobuf-kotlin, spring-security-oauth2-resource-server, spring-security-oauth2-jose; `compileOnly(micrometer-tracing)`; `kapt`/`annotationProcessor` configuration processor; tests: spring-boot-starter-test, mockk, awaitility, grpc-inprocess, `testImplementation(project(":kraken-messaging-test"))`, spring-boot-testcontainers, testcontainers-kafka, testcontainers-mongodb, nimbus-jose-jwt.
- Protobuf block:
  ```kotlin
  protobuf {
      protoc { artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.get()}" }
      plugins {
          create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}" }
          create("grpckt") { artifact = "io.grpc:protoc-gen-grpc-kotlin:${libs.versions.grpc.kotlin.get()}:jdk8@jar" }
      }
      generateProtoTasks { all().forEach { it.plugins { create("grpc"); create("grpckt") }; it.builtins { create("kotlin") } } }
  }
  ```

- [ ] **Step 1: Write the failing smoke test**

```kotlin
class BuildSmokeTest {
    @Test fun `grpc kotlin stub is generated`() {
        assertNotNull(Class.forName("io.damoreira.kraken.hub.v1.HubGrpcKt"))
    }
    @Test fun `jackson 3 kotlin module is on the classpath`() {
        assertNotNull(Class.forName("tools.jackson.module.kotlin.KotlinModule"))
    }
}
```

- [ ] **Step 2: Run** — `./gradlew :kraken-messaging:test --tests '*BuildSmokeTest*'` Expected: FAIL (compile or class not found).
- [ ] **Step 3: Implement the build changes above; fix any skeleton compile errors**
- [ ] **Step 4: Run** — `./gradlew build -x test && ./gradlew :kraken-messaging:test --tests '*BuildSmokeTest*'` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "build: proto codegen (grpc-java + grpc-kotlin), Jackson 3 kotlin module, deps"`

---

### Task 2: Codec, type names, response types

**Files:**
- Create: `kraken-messaging/src/main/kotlin/io/damoreira/kraken/messaging/codec/MessageTypeResolver.kt`, `.../codec/JsonCodec.kt`, `.../codec/ResponseTypes.kt`
- Test: `kraken-messaging/src/test/kotlin/io/damoreira/kraken/messaging/codec/CodecTest.kt`

**Interfaces:**
- `class MessageTypeResolver(private val typePrefix: String) { fun typeNameOf(klass: KClass<*>): String }`
- `class JsonCodec(val mapper: JsonMapper = defaultMapper()) { fun encode(value: Any): ByteArray; fun <T : Any> decode(bytes: ByteArray, type: KClass<T>): T; fun decodeTree(bytes: ByteArray): JsonNode; companion object { fun defaultMapper(): JsonMapper } }` — `encode(Unit)` returns `{}`; `decode(bytes, Unit::class)` returns `Unit`; decode failures throw `InvalidMessageException("Cannot decode payload as ${type.qualifiedName}", cause)`.
- `object ResponseTypes { fun responseTypeOf(messageClass: KClass<*>): KClass<*> }` — reads the type argument of the `Command<R>` or `Query<R>` supertype; throws `IllegalArgumentException` when the class implements neither or `R` is not a class.
- Test fixtures (reused by later tasks) in `src/test/kotlin/io/damoreira/kraken/messaging/fixtures/Messages.kt`: `@MessageType("test.Ping") data class Ping(val text: String) : Command<Pong>`, `data class Pong(val echoed: String, val at: Instant)`, `@MessageType("test.GetCount") data class GetCount(val key: String) : Query<Count>`, `data class Count(val value: Int)`, `data class NoAnnotation(val x: Int) : Command<Unit>`, `@MessageType("test.Happened") data class Happened(val id: String, val at: Instant) : Event`.

- [ ] **Step 1: Write failing tests**

```kotlin
@Test fun `annotation wins over prefix`() = assertEquals("test.Ping", MessageTypeResolver("svc").typeNameOf(Ping::class))
@Test fun `prefix plus simple name when unannotated`() = assertEquals("svc.NoAnnotation", MessageTypeResolver("svc").typeNameOf(NoAnnotation::class))
@Test fun `instants are iso utc and unknown fields ignored`() {
    val bytes = codec.encode(Pong("hi", Instant.parse("2026-10-28T14:03:22Z")))
    assertEquals("""{"echoed":"hi","at":"2026-10-28T14:03:22Z"}""", String(bytes))
    assertEquals(Pong("hi", Instant.parse("2026-10-28T14:03:22Z")), codec.decode("""{"echoed":"hi","at":"2026-10-28T14:03:22Z","extra":1}""".toByteArray(), Pong::class))
}
@Test fun `unit encodes as empty object and decodes back`()
@Test fun `decode failure is InvalidMessageException`()   // "{}" as Count -> missing value
@Test fun `response type read from Command supertype`() = assertEquals(Pong::class, ResponseTypes.responseTypeOf(Ping::class))
@Test fun `response type of unit command is Unit`()
@Test fun `plain class has no response type`()           // assertThrows<IllegalArgumentException>
```

- [ ] **Step 2: Run** — `./gradlew :kraken-messaging:test --tests '*CodecTest*'` Expected: compile FAIL.
- [ ] **Step 3: Implement** (`defaultMapper()` = `JsonMapper.builder().addModule(KotlinModule.Builder().build()).build()`; Jackson 3 already ignores unknown properties and writes ISO dates by default, assert rather than configure)
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** — `git commit -am "feat(codec): type names, Jackson 3 codec, response type resolution"` (add new files first)

---

### Task 3: Exceptions, MessageContext, HandlerRegistry, BeanPostProcessor

**Files:**
- Modify: `.../messaging/MessagingExceptions.kt` (full set), `.../messaging/handler/Handlers.kt` (keep)
- Create: `.../messaging/MessageContext.kt`, `.../messaging/handler/HandlerMethod.kt`, `.../messaging/handler/HandlerRegistry.kt`, `.../messaging/handler/HandlerBeanPostProcessor.kt`
- Test: `src/test/kotlin/.../handler/HandlerRegistryTest.kt`, `src/test/kotlin/.../handler/HandlerBeanPostProcessorTest.kt`

**Interfaces:**
- `data class MessageContext(val messageId: String, val type: String, val correlationId: String, val userId: String?, val roles: Set<String>, val authorization: String?, val headers: Map<String, String>)`
- `enum class HandlerKind { EVENT, COMMAND, QUERY }`
- `class HandlerMethod(val bean: Any, val function: KFunction<*>, val kind: HandlerKind, val messageClass: KClass<*>, val typeName: String, val wantsContext: Boolean) { suspend fun invoke(message: Any, ctx: MessageContext): Any? }` — `invoke` uses `callSuspend` for suspend functions and `call` otherwise, unwrapping `InvocationTargetException`.
- `class HandlerRegistry(private val typeResolver: MessageTypeResolver) { fun register(bean: Any): List<HandlerMethod>; fun eventHandlers(type: String): List<HandlerMethod>; fun commandHandler(type: String): HandlerMethod?; fun queryHandler(type: String): HandlerMethod?; fun commandTypes(): Set<String>; fun queryTypes(): Set<String>; fun eventTypes(): Set<String>; fun onChange(listener: () -> Unit) }` — scans `bean::class.declaredMemberFunctions` (and superclass functions through `memberFunctions`) for the three annotations; validation errors throw `DuplicateHandlerException(type, existing, duplicate)` or `IllegalStateException("Handler ${bean::class.simpleName}.${fn.name} must take (message) or (message, MessageContext)")`; after a successful `register` with at least one handler, calls every `onChange` listener.
- `class HandlerBeanPostProcessor(private val registry: HandlerRegistry) : BeanPostProcessor` — `postProcessAfterInitialization` calls `registry.register(bean)` on the target class (`AopUtils.getTargetClass`) so proxied `@Transactional` beans are scanned by their real class while the proxy is what gets invoked.

- [ ] **Step 1: Write failing tests**

```kotlin
// HandlerRegistryTest
@Test fun `registers event command and query handlers by parameter type`()      // bean with on(Happened), handle(Ping, ctx): Pong, handle(GetCount): Count
@Test fun `duplicate command handler fails`()                                    // assertThrows<DuplicateHandlerException>
@Test fun `two event handlers for one type are allowed`()
@Test fun `wrong second parameter fails with method name in message`()
@Test fun `zero parameter handler fails`()
@Test fun `invoke passes context when wanted and supports suspend`()             // suspend fun handle(p: Ping, ctx) returns Pong(ctx.userId!!)
@Test fun `invoke unwraps handler exception`()                                   // handler throws IllegalStateException("boom") -> same exception, not InvocationTargetException
@Test fun `onChange fires after register`()
// HandlerBeanPostProcessorTest (AnnotationConfigApplicationContext)
@Test fun `beans with handlers are discovered including proxied ones`()          // bean annotated @Transactional-like via a simple ProxyFactory, registry.commandTypes() contains "test.Ping"
@Test fun `duplicate handlers across beans fail context startup`()               // assertThrows<BeanCreationException> whose root cause is DuplicateHandlerException
```

- [ ] **Step 2: Run** — Expected: compile FAIL.
- [ ] **Step 3: Implement per Interfaces**
- [ ] **Step 4: Run** — `./gradlew :kraken-messaging:test --tests '*handler*'` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(handler): registry, context, bean post processor, exception set"`

---

### Task 4: HandlerInvoker and InMemoryBuses

**Files:**
- Create: `.../messaging/handler/HandlerInvoker.kt`, `.../messaging/hub/HubErrors.kt`
- Create: `kraken-messaging-test/src/main/kotlin/io/damoreira/kraken/messaging/test/InMemoryBuses.kt`
- Test: `src/test/kotlin/.../handler/HandlerInvokerTest.kt`, `kraken-messaging-test/src/test/kotlin/.../test/InMemoryBusesTest.kt`

**Interfaces:**
- `sealed interface InvocationResult { data class Success(val payload: ByteArray) : InvocationResult; data class Failure(val code: ErrorCode, val message: String, val details: ByteArray = ByteArray(0)) : InvocationResult }`
- `class HandlerInvoker(private val registry: HandlerRegistry, private val codec: JsonCodec) { suspend fun invokeCommand(type: String, payload: ByteArray, ctx: MessageContext): InvocationResult; suspend fun invokeQuery(...): InvocationResult; suspend fun invokeEvent(type: String, payload: ByteArray, ctx: MessageContext) /* calls every handler, rethrows first failure */ }` — mapping: no handler => `Failure(NO_HANDLER)`; decode failure => `Failure(INVALID_REQUEST, "Cannot decode payload as <type>")`; `UnauthenticatedException` => `Failure(UNAUTHENTICATED, message)`; `jakarta.validation.ConstraintViolationException` or `IllegalArgumentException` => `Failure(HANDLER_ERROR, message, details = JSON array of violation messages when available)`; any other exception => `Failure(HANDLER_ERROR, "${e::class.simpleName}: ${e.message}")`; never includes stack traces.
- `object HubErrors { fun toException(type: String, code: ErrorCode, message: String, details: ByteArray): MessagingException }` — NO_HANDLER→`NoHandlerException(type)`, TIMEOUT→`MessageTimeoutException(type)`, HANDLER_DISCONNECTED→`HandlerDisconnectedException(type)`, HANDLER_ERROR→`RemoteHandlerException(type, message, details)`, UNAUTHENTICATED→`UnauthenticatedException(message)`, INVALID_REQUEST→`InvalidMessageException(message)`, UNSPECIFIED→`RemoteHandlerException`.
- `class InMemoryBuses(typePrefix: String = "test", userId: String? = "user-1") : EventBus, CommandBus, QueryBus { val registry: HandlerRegistry; fun register(vararg beans: Any): InMemoryBuses; val published: List<PublishedEvent>; data class PublishedEvent(val type: String, val key: String?, val event: Event) }` — `publish` records and dispatches synchronously to local event handlers; `send` encodes, calls `HandlerInvoker`, decodes the response with `ResponseTypes`, converts `Failure` with `HubErrors.toException`.

- [ ] **Step 1: Write failing tests**

```kotlin
// HandlerInvokerTest
@Test fun `command success returns encoded response`()
@Test fun `query without handler is NO_HANDLER`()
@Test fun `bad payload is INVALID_REQUEST with type in message`()
@Test fun `handler exception is HANDLER_ERROR without stack trace`()       // message == "IllegalStateException: boom"
@Test fun `unauthenticated exception maps to UNAUTHENTICATED`()
@Test fun `event invokes all handlers`()
// InMemoryBusesTest
@Test fun `send round trips through local handler with typed response`()  // buses.register(Handlers()); val pong: Pong = buses.send(Ping("x")); assertEquals("x", pong.echoed)
@Test fun `publish dispatches to event handlers and records`()
@Test fun `no handler surfaces NoHandlerException`()
```

- [ ] **Step 2: Run** — Expected: compile FAIL.
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** — `./gradlew test --tests '*HandlerInvoker*' --tests '*InMemoryBuses*'` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat: handler invoker with error mapping; InMemoryBuses for unit tests"`

---

### Task 5: InProcessHub (test module)

**Files:**
- Create: `kraken-messaging-test/src/main/kotlin/io/damoreira/kraken/messaging/test/InProcessHub.kt`
- Modify: `kraken-messaging-test/build.gradle.kts` (add `api(libs.grpc.inprocess)`, `implementation(libs.grpc.kotlin.stub)`, `implementation(libs.grpc.protobuf)`)
- Test: `kraken-messaging-test/src/test/kotlin/.../test/InProcessHubTest.kt`

**Interfaces:**
- `class InProcessHub(val name: String = "kraken-hub-" + UUID.randomUUID()) : AutoCloseable { fun start(): InProcessHub; fun channel(): ManagedChannel /* InProcessChannelBuilder.forName(name) */; fun registeredTypes(): Set<String>; fun connectedInstances(): Set<String>; fun dropInstance(instanceId: String) /* closes that stream, simulating a crash */; fun heartbeatsFrom(instanceId: String): Int; fun lastRequestHeaders(): Map<String, String>?; var replyTimeout: Duration = Duration.ofSeconds(5); override fun close() }` — implements `HubGrpcKt.HubCoroutineImplBase.connect(requests: Flow<ClientMessage>): Flow<ServerMessage>` with the real hub's observable semantics needed by the starter tests: first message must be `Register` (else `FAILED_PRECONDITION`), ownership conflict (`ALREADY_EXISTS`), round-robin forwarding, reply relay by `request_id`, `NO_HANDLER`, `TIMEOUT` after `replyTimeout` or `Request.timeout_ms`, `HANDLER_DISCONNECTED` on stream close, heartbeat echo. No consistent hashing, no metrics.

- [ ] **Step 1: Write failing tests** (two raw `HubCoroutineStub` clients, no starter code)

```kotlin
@Test fun `relays command between two raw clients`()
@Test fun `unknown type is NO_HANDLER`()
@Test fun `dropped handler fails in flight with HANDLER_DISCONNECTED`()
@Test fun `request before register is FAILED_PRECONDITION`()
@Test fun `second owner of a type is ALREADY_EXISTS`()
```

- [ ] **Step 2: Run** — `./gradlew :kraken-messaging-test:test` Expected: compile FAIL.
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** — Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(test): InProcessHub for starter and service unit tests"`

---

### Task 6: HubConnection, HubBus, inbound dispatch, JWT forwarding and validation

**Files:**
- Create: `.../messaging/hub/HubConnection.kt`, `.../messaging/hub/HubBus.kt`, `.../messaging/hub/InboundRequestDispatcher.kt`, `.../messaging/security/CallerContext.kt`, `.../messaging/security/ForwardedJwtAuthenticator.kt`, `.../messaging/tracing/TraceHeaders.kt`
- Modify: `.../messaging/CommandBus.kt`, `.../messaging/QueryBus.kt` (add `sendBlocking`)
- Test: `src/test/kotlin/.../hub/HubConnectionTest.kt`, `src/test/kotlin/.../hub/HubBusTest.kt`, `src/test/kotlin/.../security/ForwardedJwtAuthenticatorTest.kt`, fixture `src/test/kotlin/.../fixtures/TestJwt.kt` (RSA key pair, `fun token(subject: String, roles: List<String>, expiresIn: Duration = 5.minutes, issuer: String = "https://issuer.test/realms/kraken"): String`, `fun decoder(): JwtDecoder` built with `NimbusJwtDecoder.withPublicKey(...)`)

**Interfaces:**
- `class HubConnection(private val channelFactory: () -> ManagedChannel, private val serviceName: String, val instanceId: String, private val settings: HubSettings, private val inbound: suspend (Request) -> Reply, private val scope: CoroutineScope)`; `data class HubSettings(val requestTimeout: Duration, val heartbeatInterval: Duration, val backoffMin: Duration, val backoffMax: Duration)`; methods: `fun start()` (connect loop in `scope`), `suspend fun awaitRegistered(timeout: Duration): Boolean`, `fun register(commandTypes: Set<String>, queryTypes: Set<String>)` (stores and sends if connected; re-sent on every reconnect), `suspend fun send(request: Request): ServerMessage` (returns the `Reply` or `Error` for that id; throws `HubUnavailableException` if not connected within `requestTimeout`, `MessageTimeoutException(type)` if nothing arrives within `timeout_ms`), `suspend fun stop()`, `val connected: StateFlow<Boolean>`, `val lastRejection: StateFlow<Status?>` (set on `ALREADY_EXISTS`; the connect loop keeps retrying at `backoffMax`).
- Inbound requests are dispatched in `scope.launch { }` per request so a slow handler does not block the stream; the `Reply` is written to the outbound channel.
- `class InboundRequestDispatcher(private val invoker: HandlerInvoker, private val authenticator: ForwardedJwtAuthenticator, private val codec: JsonCodec) { suspend fun dispatch(req: Request): Reply }` — builds `MessageContext` from headers (`authenticator.authenticate(headers["authorization"])` gives `userId`/`roles` or throws `UnauthenticatedException`; with `require-jwt=false` and no header the context has `userId=null`), then `invoker.invokeCommand/Query` by `req.kind`.
- `class ForwardedJwtAuthenticator(private val decoder: JwtDecoder?, private val requireJwt: Boolean) { fun authenticate(authorization: String?): Principal; data class Principal(val userId: String?, val roles: Set<String>) }` — strips `Bearer `, `decoder.decode`, roles from claim path `realm_access.roles`; `JwtException` => `UnauthenticatedException(e.message)`.
- `object CallerContext { fun currentBearerToken(): String?; fun currentUserId(): String? }` — reads `SecurityContextHolder.getContext().authentication` (`JwtAuthenticationToken.token.tokenValue`, `BearerTokenAuthentication`, or a `KrakenForwardedAuthentication` set by the dispatcher so nested sends re-forward the same token).
- `class HubBus(private val connection: HubConnection, private val registry: HandlerRegistry, private val invoker: HandlerInvoker, private val codec: JsonCodec, private val typeResolver: MessageTypeResolver, private val forwardJwt: Boolean, private val traceHeaders: TraceHeaders, private val requestTimeout: Duration) : CommandBus, QueryBus` — local short-circuit when `registry.commandHandler(type) != null`; otherwise `Request{id=UUID, kind, type, routingKey ?: "", payload, headers{authorization?, correlationId (MDC "correlationId" or new UUID), traceparent?, tracestate?}, timeoutMs=requestTimeout}`; `Reply.payload` decoded with `ResponseTypes.responseTypeOf(message::class)`, decode failure => `InvalidMessageException`; `Reply.error`/`Error` => `HubErrors.toException`. `sendBlocking` = `runBlocking { send(...) }`.
- `class TraceHeaders(private val propagator: Any? /* io.micrometer.tracing.propagation.Propagator */, private val tracer: Any?) { fun inject(into: MutableMap<String, String>) }` — no-op when either is null.

- [ ] **Step 1: Write failing tests**

```kotlin
// HubConnectionTest (InProcessHub)
@Test fun `registers on connect and hub sees types`()
@Test fun `send gets reply from another connection`()                         // conn B inbound answers Reply{payload="{\"echoed\":\"x\"}"}
@Test fun `send without hub fails with HubUnavailableException within timeout`() // channelFactory to a closed in-process name; requestTimeout 300ms; measure < 2s
@Test fun `reconnects and re-registers after hub restart`()                   // hub.close(); new InProcessHub(sameName).start(); await connected; hub.registeredTypes() contains type
@Test fun `in flight request fails with HandlerDisconnected when handler dropped`()
@Test fun `already exists rejection is exposed`()                             // second connection registering same type: lastRejection.code == ALREADY_EXISTS
@Test fun `heartbeats are sent`()                                             // heartbeatInterval 50ms; hub counts >= 3 within 500ms
// HubBusTest
@Test fun `local handler short circuits without hub`()                        // connection to nothing; registry has Ping handler; send returns Pong
@Test fun `remote command carries bearer token and correlation id`()          // SecurityContextHolder with JwtAuthenticationToken; InProcessHub captures headers
@Test fun `forward-jwt false omits authorization header`()
@Test fun `NO_HANDLER maps to NoHandlerException`()
@Test fun `HANDLER_ERROR reply maps to RemoteHandlerException with details`()
@Test fun `wrong shaped reply is InvalidMessageException`()                   // remote replies {"nope":1} for Count -> InvalidMessageException
@Test fun `sendBlocking works from a plain thread`()
// ForwardedJwtAuthenticatorTest
@Test fun `valid token yields subject and realm roles`()
@Test fun `expired token is UnauthenticatedException`()
@Test fun `missing header with require-jwt is UnauthenticatedException`()
@Test fun `missing header without require-jwt is anonymous`()
@Test fun `dispatcher answers UNAUTHENTICATED reply for bad token`()         // InboundRequestDispatcher end-to-end with invoker
```

- [ ] **Step 2: Run** — Expected: compile FAIL.
- [ ] **Step 3: Implement per Interfaces** (stream: `stub.connect(outbound.receiveAsFlow())` collected in the connect loop; pending map `ConcurrentHashMap<String, CompletableDeferred<ServerMessage>>`; on stream failure complete all pending exceptionally with `HubUnavailableException`)
- [ ] **Step 4: Run** — `./gradlew :kraken-messaging:test --tests '*hub*' --tests '*security*'` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(hub): grpc stream connection, command/query bus, JWT forwarding and validation"`

---

### Task 7: Outbox EventBus and OutboxPublisher (Testcontainers: Mongo replica set, Kafka)

**Files:**
- Create: `.../messaging/kafka/EventHeaders.kt`, `.../messaging/kafka/OutboxDocument.kt`, `.../messaging/kafka/OutboxEventBus.kt`, `.../messaging/kafka/OutboxPublisher.kt`, `.../messaging/kafka/MessagingIndexes.kt`
- Modify: `.../messaging/EventBus.kt` (add `publishBlocking`)
- Create: `kraken-messaging-test/src/main/kotlin/io/damoreira/kraken/messaging/test/KrakenContainers.kt` (replace the stub), `.../test/KrakenTestKafka.kt`
- Test: `src/test/kotlin/.../kafka/OutboxTest.kt`

**Interfaces:**
- `object EventHeaders { const val TYPE="type"; ID="id"; CORRELATION_ID="correlationId"; TIMESTAMP="timestamp"; SOURCE="source"; USER_ID="userId"; TRACEPARENT="traceparent"; TRACESTATE="tracestate" }`
- `@Document("outbox") data class OutboxDocument(@Id val id: String, val type: String, val key: String?, val headers: Map<String, String>, val payload: org.bson.Document, val createdAt: Instant, val sentAt: Instant? = null, val attempts: Int = 0)`
- `class OutboxEventBus(private val mongo: MongoTemplate, private val codec: JsonCodec, private val typeResolver: MessageTypeResolver, private val traceHeaders: TraceHeaders) : EventBus` — `publish` inserts `OutboxDocument(id=UUID, type, key, headers{correlationId, userId=CallerContext.currentUserId() ?: "", traceparent?}, payload=Document.parse(json), createdAt=now)` via `mongo.insert`, joining a thread-bound transaction when present.
- `class OutboxPublisher(private val mongo: MongoTemplate, private val kafka: KafkaTemplate<String, ByteArray>, private val topic: String, private val source: String, private val pollInterval: Duration, private val batchSize: Int) : SmartLifecycle` — loop on a single daemon thread: `find(sentAt == null).sort(createdAt).limit(batchSize)`; for each: `ProducerRecord(topic, null, key, payload.toJson().toByteArray(), headers + type,id,timestamp=createdAt,source)`, `kafka.send(record).get()`, then `updateFirst(id, set sentAt=now)`; on send failure increment `attempts`, log, continue with next poll. Exposes `fun publishPendingNow(): Int` for tests.
- `class MessagingIndexes(mongo: MongoTemplate, outboxRetention: Duration, idempotencyRetention: Duration) : InitializingBean` — ensures `outbox {sentAt:1, createdAt:1}`, TTL `outbox.sentAt` expireAfter `outboxRetention`, TTL `processed_events.processedAt` expireAfter `idempotencyRetention`.
- `KrakenContainers`: `fun kafka(): KafkaContainer` (`org.testcontainers.kafka.KafkaContainer(DockerImageName.parse("apache/kafka:4.4.0"))`), `fun mongo(): MongoDBContainer` (`org.testcontainers.mongodb.MongoDBContainer("mongo:8.3.13")`, which already starts a single-node replica set), `fun hub(): GenericContainer<*>` (image `System.getenv("KRAKEN_HUB_IMAGE") ?: "kraken-hub:dev"`, port 7000, wait for log `kraken-hub starting`), `fun hubImageAvailable(): Boolean` (`docker image inspect` via Testcontainers' `DockerClientFactory`). All with `.withReuse(true)`.
- `class KrakenTestKafka(bootstrap: String, topic: String = "kraken.events") { fun <T : Any> awaitEvent(type: KClass<T>, timeout: Duration = 10.seconds, predicate: (T) -> Boolean = { true }): Pair<T, Map<String, String>>; inline fun <reified T : Any> awaitEvent(...) }` using a fresh consumer group reading from `earliest`.

- [ ] **Step 1: Write failing tests** (Mongo + Kafka containers started once per class)

```kotlin
@Test fun `publish writes outbox document with headers and camelCase payload`()
@Test fun `publish inside failing transaction leaves no document`()         // TransactionTemplate(MongoTransactionManager).execute { bus.publishBlocking(...); throw RuntimeException() } -> count == 0
@Test fun `publisher sends to kafka with all headers and marks sent`()      // KrakenTestKafka.awaitEvent<Happened>(); headers type,id,correlationId,timestamp,source,userId present; sentAt != null
@Test fun `publisher preserves per-key order`()                             // 20 events key "k", consumed order == created order
@Test fun `indexes exist with expected ttl`()                               // listIndexes contains expireAfterSeconds == 7d and 30d
```

- [ ] **Step 2: Run** — Expected: compile FAIL.
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** — `./gradlew :kraken-messaging:test --tests '*OutboxTest*'` Expected: PASS (requires Docker).
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(kafka): outbox event bus, publisher, indexes, Testcontainers helpers"`

---

### Task 8: EventConsumer with idempotency and retries

**Files:**
- Create: `.../messaging/kafka/ProcessedEventDocument.kt`, `.../messaging/kafka/EventConsumer.kt`
- Test: `src/test/kotlin/.../kafka/EventConsumerTest.kt`

**Interfaces:**
- `@Document("processed_events") data class ProcessedEventDocument(@Id val id: String, val type: String, val processedAt: Instant)`
- `class EventConsumer(consumerFactory: ConsumerFactory<String, ByteArray>, topic: String, groupId: String, private val registry: HandlerRegistry, private val invoker: HandlerInvoker, private val tx: TransactionTemplate, private val mongo: MongoTemplate, maxAttempts: Int) : SmartLifecycle` — wraps a `ConcurrentMessageListenerContainer` with `AckMode.MANUAL_IMMEDIATE`, `DefaultErrorHandler(recoverer = log + ack, FixedBackOff(1000, maxAttempts - 1L))`. Per record: no `type` header or no handler → `ack`; else `tx.executeWithoutResult { mongo.insert(ProcessedEventDocument(id, type, now)); runBlocking { invoker.invokeEvent(type, value, ctx) } }`; `DuplicateKeyException` → `ack` and skip; handler exception propagates to the error handler (record redelivered). `MessageContext` from headers: `userId` empty string → `null`, `roles` empty, `authorization` null.

- [ ] **Step 1: Write failing tests** (producer writes raw records; handler bean records invocations into a list)

```kotlin
@Test fun `handled type is invoked once and offset committed`()
@Test fun `unhandled type is skipped`()                                   // then a handled record still arrives
@Test fun `duplicate event id is processed once`()                        // same id twice
@Test fun `handler failure rolls back processed marker and retries up to max attempts`() // handler throws twice then succeeds: invoked 3 times, 1 processed_events doc
@Test fun `poison record is skipped after max attempts`()                 // always throws: invoked exactly 5 times, next record still processed
@Test fun `record without type header is skipped`()
@Test fun `non json payload is skipped with warning`()
```

- [ ] **Step 2: Run** — Expected: compile FAIL.
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** — `./gradlew :kraken-messaging:test --tests '*EventConsumerTest*'` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(kafka): idempotent transactional event consumer with bounded retries"`

---

### Task 9: Properties and auto-configuration

**Files:**
- Modify: `.../messaging/config/KrakenMessagingProperties.kt`, `.../messaging/config/KrakenMessagingAutoConfiguration.kt`, `META-INF/spring/...AutoConfiguration.imports`
- Create: `.../messaging/config/HubAutoConfiguration.kt`, `.../messaging/config/KafkaEventsAutoConfiguration.kt`, `src/main/resources/META-INF/additional-spring-configuration-metadata.json` (only if the processor misses nested defaults)
- Test: `src/test/kotlin/.../config/AutoConfigurationTest.kt` (`ApplicationContextRunner`), `src/test/kotlin/.../config/PropertiesBindingTest.kt`

**Interfaces:**
- `@ConfigurationProperties("kraken.messaging") data class KrakenMessagingProperties(val serviceName: String, val typePrefix: String, val hub: Hub = Hub(), val kafka: Kafka = Kafka(), val outbox: Outbox = Outbox(), val idempotency: Idempotency = Idempotency(), val security: Security = Security())` with nested classes carrying exactly the Global Constraints defaults; `Hub.reconnectBackoff: Backoff(min=500ms, max=30s)` bound from `kraken.messaging.hub.reconnect-backoff.min/max`.
- `KrakenMessagingAutoConfiguration` (core, always): `MessageTypeResolver`, `JsonCodec`, `HandlerRegistry`, `HandlerBeanPostProcessor` (static `@Bean`), `HandlerInvoker`, `TraceHeaders` (optional Micrometer beans), `ForwardedJwtAuthenticator` (`ObjectProvider<JwtDecoder>`; throws `IllegalStateException("kraken.messaging.security.require-jwt=true but no JwtDecoder bean")` when required and absent).
- `HubAutoConfiguration` (`@ConditionalOnProperty("kraken.messaging.hub.enabled", matchIfMissing = true)`): `ManagedChannel` (`@ConditionalOnMissingBean(name = "krakenHubChannel")`), `HubConnection` started via `SmartLifecycle` and registered with `registry.commandTypes()/queryTypes()`, re-registered on `registry.onChange`, `InboundRequestDispatcher`, `HubBus` exposed as both `CommandBus` and `QueryBus` (`@ConditionalOnMissingBean` each).
- `KafkaEventsAutoConfiguration` (`@ConditionalOnClass(KafkaTemplate::class, MongoTemplate::class)`): `ProducerFactory`/`KafkaTemplate<String, ByteArray>` named `krakenKafkaTemplate` (acks all, idempotence, `StringSerializer`/`ByteArraySerializer`), `ConsumerFactory<String, ByteArray>` (group = service name, `enable.auto.commit=false`, `auto.offset.reset=earliest`), `OutboxEventBus` as `EventBus`, `OutboxPublisher` when `outbox.enabled`, `EventConsumer` when `kafka.consumer.enabled`, `MessagingIndexes`, and a `MongoTransactionManager` + `TransactionTemplate` if missing.
- Default property source `META-INF/kraken-messaging-defaults.properties` is not used; defaults live in the properties classes.

- [ ] **Step 1: Write failing tests**

```kotlin
// PropertiesBindingTest
@Test fun `defaults match spec`()                                   // every default from Global Constraints asserted
@Test fun `relaxed env binding KRAKEN_MESSAGING_HUB_ADDRESS`()
@Test fun `missing service-name fails binding`()
// AutoConfigurationTest (ApplicationContextRunner with hub.enabled=false, kafka.consumer.enabled=false, outbox.enabled=false, require-jwt=false for the pure-bean tests)
@Test fun `core beans present`()                                    // HandlerRegistry, HandlerInvoker, JsonCodec, ForwardedJwtAuthenticator
@Test fun `require-jwt without JwtDecoder fails startup with hint`()
@Test fun `user CommandBus bean overrides HubBus`()
@Test fun `hub disabled leaves no HubConnection`()
@Test fun `handlers in user beans are registered`()                 // withUserConfiguration(HandlersConfig::class) -> registry.commandTypes() contains "test.Ping"
```

- [ ] **Step 2: Run** — Expected: FAIL.
- [ ] **Step 3: Implement**
- [ ] **Step 4: Run** — `./gradlew :kraken-messaging:test --tests '*config*'` Expected: PASS.
- [ ] **Step 5: Commit** — `git add -A && git commit -m "feat(config): full property set and conditional auto-configuration"`

---

### Task 10: End-to-end: two Spring Boot apps, real hub image, Kafka, Mongo

**Files:**
- Test: `src/test/kotlin/.../e2e/TwoServicesEndToEndTest.kt`, `src/test/kotlin/.../e2e/ServiceA.kt`, `.../e2e/ServiceB.kt` (minimal `@SpringBootApplication` classes with handler beans, both using `require-jwt=true` with the `TestJwt.decoder()` bean)

**Interfaces:**
- Containers: `KrakenContainers.kafka()`, `.mongo()`, `.hub()`; `Assumptions.assumeTrue(KrakenContainers.hubImageAvailable())` so the test is skipped (and reported as skipped) when `kraken-hub:dev` was not built; databases `e2e_a` and `e2e_b`.
- Two `SpringApplicationBuilder` contexts with distinct `service-name`/`type-prefix`, hub address from the container, Kafka bootstrap and Mongo URI from containers.

- [ ] **Step 1: Write the failing test**

```kotlin
@Test fun `command from A is handled by B through the hub with the caller identity`()
    // A: commandBus.send(Ping("hi")) with SecurityContext token(subject "u-42"); B handler returns Pong(ctx.userId!!); assertEquals("u-42", pong.echoed)
@Test fun `event published by B inside its handler reaches A exactly once`()
    // B's Ping handler publishes Happened; A's @EventHandler records; await size==1 and stays 1 for 2s
@Test fun `query to a type nobody owns is NoHandlerException`()
@Test fun `expired token is UnauthenticatedException`()
```

- [ ] **Step 2: Build the hub image** — `(cd ../kraken-hub && docker build -t kraken-hub:dev .)` Expected: image present.
- [ ] **Step 3: Run** — `./gradlew :kraken-messaging:test --tests '*EndToEnd*'` Expected: PASS (or SKIPPED when the image is missing). Fix whatever integration issue appears; no production code change is planned here beyond bug fixes.
- [ ] **Step 4: Commit** — `git add -A && git commit -m "test: end-to-end two services through the real hub, Kafka and Mongo"`

---

### Task 11: Publish locally, README, CI

**Files:**
- Modify: `README.md`, `.gitlab-ci.yml` (test job needs `docker:28-dind` service and `KRAKEN_HUB_IMAGE` variable), `kraken-messaging-test/src/main/kotlin/.../test/KrakenContainers.kt` KDoc

- [ ] **Step 1: Full build** — `./gradlew clean build` Expected: BUILD SUCCESSFUL, all tests green (E2E may be skipped in CI without the image; locally it runs).
- [ ] **Step 2: Publish** — `./gradlew publishToMavenLocal` Expected: `~/.m2/repository/io/damoreira/kraken/kraken-messaging/0.1.0-SNAPSHOT/` exists with the three artifacts.
- [ ] **Step 3: README**: replace the skeleton status with a usage section (dependency, `application.yaml` keys, handler example from the spec, `InMemoryBuses` and `InProcessHub` test usage, container helpers), list known limitations (no DLT, no leader election, no scatter-gather, metrics not yet exported by the library).
- [ ] **Step 4: Commit** — `git add -A && git commit -m "docs: usage, test helpers, limitations; CI test job with Docker"`
