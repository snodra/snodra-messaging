# Examples Sandbox Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Two Spring Boot example services (A calls, B owns) running through the real `kraken-hub` image and Kafka in Docker Compose, driven by `curl`.

**Architecture:** Three Gradle modules under `examples/` in the `kraken-messaging` repo: `examples-api` (the five wire classes), `service-b` (command + query handlers, in-memory counts, publishes the event), `service-a` (REST controller, event listener, error mapping). One multi-stage Dockerfile builds either service; an umbrella compose override wires both to hub, Kafka and MongoDB from local builds. Unit tests run on `InMemoryBuses`; a `smoke.sh` proves the running stack.

**Tech Stack:** Kotlin 2.4.21, Spring Boot 4.1.2 (webmvc, actuator), kraken-messaging 0.1.0-SNAPSHOT (project dependency), Gradle 9.8.2, JDK 25, Docker Compose, `eclipse-temurin:25-jdk` / `25-jre`.

**Spec:** `docs/superpowers/specs/2026-10-30-examples-sandbox-design.md`

## Global Constraints

- Examples call only the public starter API (`CommandBus`, `QueryBus`, `EventBus`, `MessageContext`, `@CommandHandler`, `@QueryHandler`, `@EventHandler`, `@MessageType`); anything awkward is a starter finding, not an example workaround.
- No domain persistence in the apps; MongoDB is used only by the starter (outbox, processed ids).
- Anonymous only: no `@RequiresAuthentication`, no security configuration.
- Example modules are never published: no `maven-publish` plugin on them.
- Every wire class carries an explicit `@MessageType("examples.<SimpleName>")`.
- Spring Boot module quirk: pin `extra["kotlin.version"] = libs.versions.kotlin.get()` wherever `io.spring.dependency-management` is applied.
- Mongo property key is `spring.mongodb.uri`; hub address accepts `static://host:port`.
- Compose: host ports 8091 (A), 8092 (B), hub gRPC on host 7002, topology HTTP 7001; in-network `hub:7000`, `kafka:19092`, `mongodb:27017`.
- Umbrella repo changes (`infra/compose.local.yaml`, `docs/07-local-development.md`) go on a branch `examples/compose-local` in `../kraken` (worktree under its `.worktrees/`), merged with the user's consent at the end, like earlier features.
- Commit trailer: `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.
- Docker on this machine is Rancher Desktop; `docker compose` and `docker build` work as is.

## Review Focus

1. **B is down when A posts** — the person expects a clear 503 with `NoHandlerException` in the body, not a stack trace or a hang. Pinned by Task 2's "no handler maps to 503" test.
2. **Blank name** — `POST /greetings {"name": ""}` should be a 400, never a command on the wire. Pinned by Task 2's "blank name is 400" test.
3. **Unknown name on the query** — `GET /greetings/Nobody/count` returns `count: 0`, not an error. Pinned by Task 1's "count for unknown name is 0" test.
4. **Wire names independent of `type-prefix`** — A (`type-prefix: service-a`) and B (`service-b`) must agree on `examples.SendGreeting`; a missing `@MessageType` would make every call a `NO_HANDLER`. Pinned by Task 1's "event is published under its explicit wire type" test and the smoke run.
5. **Hub restart while the apps run** — the starter reconnects and the next `curl` succeeds. Not unit-testable here; Task 4's README lists it as a manual experiment (`docker compose restart hub`, wait 10 s, post again).

---

### Task 1: `examples-api` and `service-b`

**Files:**
- Modify: `settings.gradle.kts` (add `include("examples:examples-api")`, `include("examples:service-b")`)
- Create: `examples/examples-api/build.gradle.kts`
- Create: `examples/examples-api/src/main/kotlin/io/damoreira/kraken/examples/api/Greetings.kt`
- Create: `examples/service-b/build.gradle.kts`
- Create: `examples/service-b/src/main/kotlin/io/damoreira/kraken/examples/b/ServiceBApplication.kt`
- Create: `examples/service-b/src/main/kotlin/io/damoreira/kraken/examples/b/GreetingHandlers.kt`
- Create: `examples/service-b/src/main/resources/application.yaml`
- Test: `examples/service-b/src/test/kotlin/io/damoreira/kraken/examples/b/GreetingHandlersTest.kt`

**Interfaces:**
- Consumes: starter `CommandBus`/`QueryBus`/`EventBus`, `MessageContext`, handler annotations; `InMemoryBuses(typePrefix, userId, roles).register(vararg beans): InMemoryBuses`, `InMemoryBuses.published: List<PublishedEvent(type, key, event)>`.
- Produces (package `io.damoreira.kraken.examples.api`, all `data class`, all `@MessageType("examples.<Name>")`):
  - `SendGreeting(val name: String) : Command<GreetingAccepted>`
  - `GreetingAccepted(val id: String, val name: String)`
  - `GreetingDelivered(val id: String, val name: String, val at: Instant) : Event`
  - `CountGreetings(val name: String) : Query<GreetingCount>`
  - `GreetingCount(val name: String, val count: Long)`
  - `class GreetingHandlers(private val events: EventBus)` with `suspend fun handle(cmd: SendGreeting, ctx: MessageContext): GreetingAccepted` (`@CommandHandler`) and `fun handle(q: CountGreetings): GreetingCount` (`@QueryHandler`).

- [ ] **Step 1: Gradle wiring**

`settings.gradle.kts`: add the two `include` lines after the existing three. `examples/examples-api/build.gradle.kts`: plugins `alias(libs.plugins.kotlin.jvm)`, `java-library`; dependencies `api(project(":kraken-messaging-api"))`. `examples/service-b/build.gradle.kts`: plugins `kotlin.jvm`, `kotlin.spring`, `spring.boot`, `spring.dependency.management`; `extra["kotlin.version"]` pin; `dependencyManagement { imports { mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}") } }`; dependencies `implementation(project(":kraken-messaging"))`, `implementation(project(":examples:examples-api"))`, `implementation(libs.spring.boot.starter.webmvc)`, `implementation(libs.spring.boot.starter.actuator)`, `implementation(libs.jackson.module.kotlin)`, `implementation(libs.kotlin.reflect)`; `testImplementation(libs.spring.boot.starter.test)`, `testImplementation(libs.kotlin.test.junit5)`, `testImplementation(project(":kraken-messaging-test"))`, `testRuntimeOnly("org.junit.platform:junit-platform-launcher")`.

Run: `./gradlew :examples:service-b:compileKotlin -q`
Expected: BUILD SUCCESSFUL with no sources yet (or "no sources" NO-SOURCE); the point is the module graph resolves.

- [ ] **Step 2: Write the failing tests**

```kotlin
class GreetingHandlersTest {
    private fun buses() = InMemoryBuses(typePrefix = "service-b")

    @Test fun `SendGreeting is accepted with a fresh id and publishes GreetingDelivered keyed by name`() = runBlocking {
        val buses = buses(); buses.register(GreetingHandlers(buses))
        val accepted = buses.send(SendGreeting("Ada"))
        assertEquals("Ada", accepted.name)
        assertTrue(accepted.id.isNotBlank())
        val published = buses.published.single()
        assertEquals("examples.GreetingDelivered", published.type)   // explicit wire type, not "service-b.GreetingDelivered"
        assertEquals("Ada", published.key)
        val event = assertIs<GreetingDelivered>(published.event)
        assertEquals(accepted.id, event.id); assertEquals("Ada", event.name)
    }

    @Test fun `CountGreetings counts accepted greetings per name`() = runBlocking {
        val buses = buses(); buses.register(GreetingHandlers(buses))
        buses.send(SendGreeting("Ada")); buses.send(SendGreeting("Ada")); buses.send(SendGreeting("Bob"))
        assertEquals(GreetingCount("Ada", 2), buses.send(CountGreetings("Ada")))
        assertEquals(GreetingCount("Bob", 1), buses.send(CountGreetings("Bob")))
    }

    @Test fun `count for unknown name is 0`() = runBlocking {
        val buses = buses(); buses.register(GreetingHandlers(buses))
        assertEquals(GreetingCount("Nobody", 0), buses.send(CountGreetings("Nobody")))
    }
}
```

- [ ] **Step 3: Run to verify they fail**

Run: `./gradlew :examples:service-b:test -q`
Expected: compilation FAILS with unresolved `GreetingHandlers`, `SendGreeting`, …

- [ ] **Step 4: Implement**

`Greetings.kt`: the five classes from Interfaces. `GreetingHandlers.kt`: `@Component class GreetingHandlers(private val events: EventBus)`, counts in a `ConcurrentHashMap<String, AtomicLong>`; command handler generates `UUID.randomUUID().toString()`, increments, `events.publish(GreetingDelivered(id, cmd.name, Instant.now()), key = cmd.name)`, logs `B: handled SendGreeting name={} id={} correlationId={}` at INFO, returns `GreetingAccepted(id, cmd.name)`; query handler logs `B: answered CountGreetings name={} count={}`. `ServiceBApplication.kt`: `@SpringBootApplication class ServiceBApplication` + `fun main(args)`. `application.yaml`:

```yaml
kraken:
  messaging:
    service-name: ${KRAKEN_SERVICE_NAME:service-b}
    type-prefix: ${KRAKEN_SERVICE_NAME:service-b}
    hub:
      address: ${KRAKEN_HUB_ADDRESS:static://localhost:7002}
    kafka:
      bootstrap-servers: ${KRAKEN_KAFKA_BOOTSTRAP:localhost:9092}
spring:
  application.name: ${KRAKEN_SERVICE_NAME:service-b}
  mongodb.uri: ${KRAKEN_MONGODB_URI:mongodb://localhost:27017/service-b?replicaSet=rs0&directConnection=true}
server.port: ${SERVER_PORT:8092}
logging.level.io.damoreira.kraken: INFO
```

- [ ] **Step 5: Run to verify they pass**

Run: `./gradlew :examples:service-b:test -q`
Expected: BUILD SUCCESSFUL, 3 tests.

- [ ] **Step 6: Commit**

```bash
git add settings.gradle.kts examples/examples-api examples/service-b
git commit -m "feat(examples): greetings api and service-b handlers"
```

---

### Task 2: `service-a` (REST caller and event listener)

**Files:**
- Modify: `settings.gradle.kts` (add `include("examples:service-a")`)
- Create: `examples/service-a/build.gradle.kts` (same as service-b's)
- Create: `examples/service-a/src/main/kotlin/io/damoreira/kraken/examples/a/ServiceAApplication.kt`
- Create: `examples/service-a/src/main/kotlin/io/damoreira/kraken/examples/a/GreetingsController.kt`
- Create: `examples/service-a/src/main/kotlin/io/damoreira/kraken/examples/a/GreetingsListener.kt`
- Create: `examples/service-a/src/main/kotlin/io/damoreira/kraken/examples/a/MessagingErrorAdvice.kt`
- Create: `examples/service-a/src/main/resources/application.yaml` (as B's with `service-a`, port default `8091`)
- Test: `examples/service-a/src/test/kotlin/io/damoreira/kraken/examples/a/GreetingsControllerTest.kt`
- Test: `examples/service-a/src/test/kotlin/io/damoreira/kraken/examples/a/GreetingsListenerTest.kt`
- Test: `examples/service-a/src/test/kotlin/io/damoreira/kraken/examples/a/MessagingErrorAdviceTest.kt`

**Interfaces:**
- Consumes: Task 1's api classes; starter exceptions `NoHandlerException(type)`, `HandlerDisconnectedException(type)`, `MessageTimeoutException(type)`, `MessagingException`.
- Produces:
  - `@RestController class GreetingsController(private val commands: CommandBus, private val queries: QueryBus, private val listener: GreetingsListener)`:
    `@PostMapping("/greetings") suspend fun send(@RequestBody body: SendGreetingRequest): GreetingAccepted`,
    `@GetMapping("/greetings/{name}/count") suspend fun count(@PathVariable name: String): GreetingCount`,
    `@GetMapping("/greetings/delivered") fun delivered(): List<GreetingDelivered>`;
    `data class SendGreetingRequest(val name: String)`; blank name → `ResponseStatusException(BAD_REQUEST, "name must not be blank")`.
  - `@Component class GreetingsListener` with `val delivered: List<GreetingDelivered>` (copy of a `CopyOnWriteArrayList`) and `@EventHandler fun on(event: GreetingDelivered, ctx: MessageContext)`.
  - `@RestControllerAdvice class MessagingErrorAdvice` with `fun handle(e: MessagingException): ResponseEntity<ErrorBody>`; `data class ErrorBody(val error: String, val message: String)`; status 503 for `NoHandlerException` and `HandlerDisconnectedException`, 504 for `MessageTimeoutException`, 502 otherwise; `error` = exception simple class name.

- [ ] **Step 1: Gradle wiring**

Add the include; copy service-b's build file. Run: `./gradlew :examples:service-a:compileKotlin -q` → resolves.

- [ ] **Step 2: Write the failing tests**

`GreetingsListenerTest` (no Spring):

```kotlin
@Test fun `records delivered greetings`() = runBlocking {
    val listener = GreetingsListener()
    val buses = InMemoryBuses(typePrefix = "service-a").register(listener)
    val event = GreetingDelivered("id-1", "Ada", Instant.EPOCH)
    buses.publish(event, "Ada")
    assertEquals(listOf(event), listener.delivered)
}
```

`MessagingErrorAdviceTest` (no Spring): `NoHandlerException("examples.SendGreeting")` → 503 and body `ErrorBody("NoHandlerException", "No handler registered for 'examples.SendGreeting'")`; `HandlerDisconnectedException("x")` → 503; `MessageTimeoutException("x")` → 504; `HubUnavailableException("down")` → 502.

`GreetingsControllerTest` with `@WebMvcTest(GreetingsController::class)` and a `@TestConfiguration` that exposes one `InMemoryBuses(typePrefix = "service-a")` bean (it implements `CommandBus`, `QueryBus`, `EventBus`) plus a `GreetingsListener` bean; a test-only owner `class StubOwner { @CommandHandler fun handle(c: SendGreeting) = GreetingAccepted("id-1", c.name); @QueryHandler fun handle(q: CountGreetings) = GreetingCount(q.name, 2) }` registered in the tests that need it. Use `MockMvcTester` (autoconfigured by `@WebMvcTest`, resolves async results from suspend controllers):

- `POST /greetings {"name":"Ada"}` with `StubOwner` registered → 200, JSON `{"id":"id-1","name":"Ada"}`.
- `GET /greetings/Ada/count` → 200, `{"name":"Ada","count":2}`.
- `POST /greetings {"name":"Ada"}` with nothing registered → 503, body `error` = `NoHandlerException`.
- `POST /greetings {"name":"  "}` → 400 and `buses.published.isEmpty()` and no command reached a handler (register a `StubOwner` that records calls; assert none).
- `GET /greetings/delivered` after `listener.on(GreetingDelivered("id-9","Ada",Instant.EPOCH), ctx)` → 200 JSON array with one element whose `id` is `id-9` (build the `MessageContext` with any values).

- [ ] **Step 3: Run to verify they fail**

Run: `./gradlew :examples:service-a:test -q`
Expected: compilation FAILS on unresolved `GreetingsController`, `GreetingsListener`, `MessagingErrorAdvice`.

- [ ] **Step 4: Implement**

Per Interfaces. Controller logs `A: sent SendGreeting name={} -> accepted id={}` and `A: asked CountGreetings name={} -> count={}`; listener logs `A: received GreetingDelivered id={} name={} at={}`. If `MockMvcTester` does not resolve the suspend result, fall back to `MockMvc` with `asyncDispatch` and note it as a ruling; if Spring MVC rejects `suspend` controller methods outright, switch the two methods to `sendBlocking` and record the ruling.

- [ ] **Step 5: Run to verify they pass**

Run: `./gradlew :examples:service-a:test -q`
Expected: BUILD SUCCESSFUL, all controller/listener/advice tests pass.

- [ ] **Step 6: Whole-repo build still green**

Run: `DOCKER_HOST=unix://$HOME/.rd/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock ./gradlew build -q`
Expected: BUILD SUCCESSFUL (starter 131 tests + examples).

- [ ] **Step 7: Commit**

```bash
git add settings.gradle.kts examples/service-a
git commit -m "feat(examples): service-a REST caller and event listener"
```

---

### Task 3: Dockerfile, compose override, smoke run

**Files:**
- Create: `examples/Dockerfile`
- Create: `.dockerignore` (repo root)
- Create: `examples/smoke.sh` (executable)
- Create (umbrella, branch `examples/compose-local`): `infra/compose.local.yaml`

**Interfaces:**
- Consumes: Task 1 and 2 modules (`:examples:service-a:bootJar`, `:examples:service-b:bootJar`), A's endpoints, hub `GET /topology` on 7001, `kraken-hub` Dockerfile at `../../kraken-hub`.
- Produces: images `kraken-examples-service-a:dev`, `kraken-examples-service-b:dev`, `kraken-hub:dev`; the start command
  `docker compose -f compose.yaml -f compose.local.yaml --profile examples up --build -d service-a service-b` run from `kraken/infra`.

- [ ] **Step 1: Write `smoke.sh` first (it is the test)**

Bash, `set -euo pipefail`, curl only, base URLs `A=${A:-http://localhost:8091}`, `HUB=${HUB:-http://localhost:7001}`:
1. wait up to 90 s for `$A/actuator/health` to contain `"UP"`;
2. `POST $A/greetings {"name":"Ada"}` twice, extract both ids (`sed`), fail if empty;
3. poll `$A/greetings/delivered` every second up to 30 s until both ids appear;
4. `GET $A/greetings/Ada/count` must contain `"count":2` (fail otherwise; print body);
5. `GET $HUB/topology` printed, and it must contain both `"service-a"` and `"service-b"`;
6. print `SMOKE OK`.

Run: `bash examples/smoke.sh`
Expected: FAILS at step 1 (nothing listening on 8091).

- [ ] **Step 2: Dockerfile and .dockerignore**

`examples/Dockerfile`:

```dockerfile
# syntax=docker/dockerfile:1
ARG SERVICE
FROM eclipse-temurin:25-jdk AS build
ARG SERVICE
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY kraken-messaging-api ./kraken-messaging-api
COPY kraken-messaging ./kraken-messaging
COPY kraken-messaging-test ./kraken-messaging-test
COPY examples ./examples
RUN --mount=type=cache,target=/root/.gradle ./gradlew :examples:${SERVICE}:bootJar --no-daemon -x test \
    && cp examples/${SERVICE}/build/libs/*.jar /app.jar

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app.jar /app/app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

`.dockerignore`: `**/build/`, `.gradle/`, `.kotlin/`, `.worktrees/`, `.superpowers/`, `.idea/`, `docs/`, `*.iml`.

Run: `docker build -f examples/Dockerfile --build-arg SERVICE=service-b -t kraken-examples-service-b:dev .`
Expected: image built; `docker run --rm kraken-examples-service-b:dev --help` is not meaningful, instead `docker image ls kraken-examples-service-b:dev` shows it.

- [ ] **Step 3: Compose override in the umbrella**

Create the umbrella worktree `.worktrees/compose-local` on branch `examples/compose-local`. `infra/compose.local.yaml`:

```yaml
# Local builds instead of registry images. Use together with compose.yaml:
#   docker compose -f compose.yaml -f compose.local.yaml --profile examples up --build -d service-a service-b
services:
  hub:
    image: kraken-hub:dev
    build: { context: ../../kraken-hub }
    profiles: [examples]            # merged with [apps]: active under either profile
    ports: !override ["7002:7000", "7001:7001"]   # macOS AirPlay owns 7000

  service-a:
    image: kraken-examples-service-a:dev
    build: { context: ../../kraken-messaging, dockerfile: examples/Dockerfile, args: { SERVICE: service-a } }
    container_name: kraken-example-a
    profiles: [examples]
    environment:
      KRAKEN_SERVICE_NAME: service-a
      KRAKEN_HUB_ADDRESS: static://hub:7000
      KRAKEN_KAFKA_BOOTSTRAP: kafka:19092
      KRAKEN_MONGODB_URI: mongodb://mongodb:27017/service-a?replicaSet=rs0
      SERVER_PORT: "8080"
    ports: ["8091:8080"]
    depends_on:
      hub: { condition: service_healthy }
      mongodb: { condition: service_healthy }
      kafka-init: { condition: service_completed_successfully }
    networks: [kraken]

  service-b:   # same shape, SERVICE/service-b, container kraken-example-b, port 8092:8080
```

Run: `cd ../kraken/.worktrees/compose-local/infra && docker compose -f compose.yaml -f compose.local.yaml --profile examples config --quiet`
Expected: exits 0; `... config | grep -A2 'hub:' | grep ports` shows only 7002 and 7001.

- [ ] **Step 4: Bring the stack up and run the smoke**

Run (from the umbrella infra dir): `docker compose -f compose.yaml -f compose.local.yaml --profile examples up --build -d service-a service-b`
Then: `bash /path/to/kraken-messaging/examples/smoke.sh`
Expected: `SMOKE OK`; `docker compose ... logs service-a service-b | grep -E "^.*(A|B): "` shows the four log lines (`B: handled`, `A: sent`, `A: received`, `B: answered`, `A: asked`).

- [ ] **Step 5: NO_HANDLER experiment**

Run: `docker compose ... stop service-b && curl -s -o /dev/null -w '%{http_code}\n' -X POST localhost:8091/greetings -H 'content-type: application/json' -d '{"name":"Ada"}'`
Expected: `503`. Then `docker compose ... start service-b` and the same curl → `200` within 15 s.

- [ ] **Step 6: Commit (both repos)**

```bash
git add examples/Dockerfile .dockerignore examples/smoke.sh && git commit -m "feat(examples): Dockerfile and smoke script"
git -C ../kraken/.worktrees/compose-local add infra/compose.local.yaml && git -C ../kraken/.worktrees/compose-local commit -m "infra: compose.local.yaml with local builds for hub and the example services"
```

---

### Task 4: Documentation

**Files:**
- Create: `examples/README.md`
- Modify: `README.md` (new `## Examples` section between "Test helpers" and "Known limitations"; add the three modules to `## Modules`)
- Modify (umbrella worktree): `docs/07-local-development.md` (new `### Sandbox: example services` under "## 2. Run the services")

- [ ] **Step 1: `examples/README.md`**

Sections: what the sandbox is (one paragraph, the four steps); prerequisites (Docker, the two sibling repos); start command; the four curl calls with expected JSON; the expected log lines per service (`docker compose ... logs -f service-a service-b`); `smoke.sh`; `GET localhost:7001/topology`; experiments: NO_HANDLER (stop B), hub restart (`docker compose ... restart hub`, next post succeeds after reconnect); running A or B from the IDE (`./gradlew :examples:service-a:bootRun`, defaults point at the published ports); teardown (`docker compose ... --profile examples down`, `down -v` to drop the Mongo/Kafka volumes).

- [ ] **Step 2: Starter README and umbrella doc**

`README.md`: module table rows for `examples/examples-api`, `examples/service-a`, `examples/service-b` (not published); `## Examples` with two sentences and a link to `examples/README.md`. Umbrella `07-local-development.md`: the override file's purpose, the start command, link to the examples README; add a row for the hub `GET /topology` check if missing.

- [ ] **Step 3: Verify links and build**

Run: `grep -o '](\.\./[^)]*\|](examples/[^)]*' README.md examples/README.md | sed 's/](//' | while read f; do test -e "$f" || echo "missing: $f"; done`
Expected: no `missing:` lines. Then `./gradlew build -q` (with the Docker env vars) → BUILD SUCCESSFUL.

- [ ] **Step 4: Commit (both repos)**

```bash
git add README.md examples/README.md && git commit -m "docs(examples): how to run the sandbox"
git -C ../kraken/.worktrees/compose-local add docs/07-local-development.md && git -C ../kraken/.worktrees/compose-local commit -m "docs: example services sandbox and compose.local.yaml"
```
