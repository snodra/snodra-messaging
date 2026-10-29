# Opt-in Handler Authentication Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make cross-service commands and queries trusted by default and let handlers opt into authentication with `@RequiresAuthentication(expression)`, evaluated on the receiving side with Spring Security expressions; failures surface at the sender as `UnauthenticatedException` or `AccessDeniedException`.

**Architecture:** An `Identity` value (user id, roles, verified flag, `Authentication`) is produced by `ForwardedIdentityResolver` on the hub path and by `LocalIdentity` from the current `SecurityContext` on the short-circuit path, and passed into `HandlerInvoker`, which evaluates the handler's compiled `@RequiresAuthentication` expression (`MessageSecurityExpressionRoot` over SpEL) before invoking. A new wire code `PERMISSION_DENIED` carries denials through the hub; `HubErrors` maps it to `AccessDeniedException`. `require-jwt` is removed and its presence fails startup with a hint.

**Tech Stack:** Kotlin 2.4.21, Spring Boot 4.1.2, Spring Security 7.1.2 (`spring-security-core` expression classes, `spring-security-oauth2-jose`), SpEL, grpc-kotlin stubs regenerated from the proto, Nimbus `SignedJWT`/`JWTParser` for unverified payload parsing (already on the classpath via oauth2-jose).

**Spec:** `docs/superpowers/specs/2026-10-29-requires-authentication-design.md` (this repo, including "Resolved builder questions"). Protocol source of truth: `/Users/damoreira/.projects/kraken-workspace/kraken/protocol/hub.proto`.

## Global Constraints

- A request is never rejected only because it has no token. A token that fails validation when a `JwtDecoder` exists is rejected with `UNAUTHENTICATED`.
- `@RequiresAuthentication(expression = "isAuthenticated()")` in `io.damoreira.kraken.messaging.handler`, `@Target(FUNCTION)`, runtime retention; allowed only on `@CommandHandler`/`@QueryHandler` methods; a parse error or use on `@EventHandler` fails registration with the handler label.
- Outcomes: no identity → `Failure(UNAUTHENTICATED, "handler <label> requires authentication")`; identity present and expression false → `Failure(PERMISSION_DENIED, "handler <label>: access denied")`.
- Roles → authorities `ROLE_<role>`; `principal` is `MessagePrincipal(userId, roles, verified)`; variables `#message` (typed) and `#ctx` (`MessageContext`).
- Proto: `PERMISSION_DENIED = 7` added additively in the umbrella and copied byte-identically to the hub and the starter; `UNAUTHENTICATED` comment loses "no role".
- `kraken.messaging.security.require-jwt` removed from properties, metadata and README; its presence in the environment fails startup with the hint from the design.
- Never log or forward a token value; log rejections at debug with handler label and code only.
- `./gradlew build` green with Docker (Rancher Desktop env vars `DOCKER_HOST=unix://$HOME/.rd/docker.sock TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`); no new warnings.
- Commit messages end with `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>`.

## Review Focus

1. A handler without `@RequiresAuthentication` must accept a request with no token and a request with an invalid token when no `JwtDecoder` is configured (unverified parse), but reject an invalid token when a decoder is configured. Tests in Task 3.
2. `@RequiresAuthentication` on an `@EventHandler` must fail startup with the handler label, not be silently ignored. Test in Task 4.
3. An expression that throws at evaluation (for example `#message.ownerId` on a message without that property) must produce `PERMISSION_DENIED` with a message naming the handler, never a `HANDLER_ERROR` or a 500-style stack trace. Test in Task 4.
4. A nested `send` denied inside an annotated handler must reach the original sender as `AccessDeniedException` (code `PERMISSION_DENIED`), not as `RemoteHandlerException`. Test in Task 5.
5. Setting `kraken.messaging.security.require-jwt` in any form (yaml, env `KRAKEN_MESSAGING_SECURITY_REQUIRE_JWT`) must fail startup with the removal hint. Test in Task 5.

---

### Task 1: Protocol: `PERMISSION_DENIED`

**Files:**
- Modify: `/Users/damoreira/.projects/kraken-workspace/kraken/protocol/hub.proto` (umbrella; worktree `.worktrees/proto-permission-denied`, branch `proto/permission-denied`), `/Users/damoreira/.projects/kraken-workspace/kraken-hub/proto/hub.proto` (hub `main`, verify with `cargo build` and `cargo test`, commit `chore(proto): PERMISSION_DENIED error code`), `kraken-messaging/src/main/proto/hub.proto` (this repo)

**Interfaces:**
- Produces: `ErrorCode.PERMISSION_DENIED = 7` with comment "The handling service evaluated the handler's @RequiresAuthentication expression as false for an authenticated caller."; `UNAUTHENTICATED` comment becomes "The handling service required an authenticated caller and the request carried no readable identity, or the forwarded JWT failed validation (expired, wrong issuer, bad signature)."

- [ ] **Step 1: Edit the umbrella proto in its worktree and commit; copy to hub and starter; `cmp` all three.**
- [ ] **Step 2: Hub builds and tests pass** — `cargo build && cargo test` in the hub main checkout; commit the copy on hub `main`.
- [ ] **Step 3: Starter regenerates** — `./gradlew :kraken-messaging:compileKotlin -q` succeeds; `ErrorCode.PERMISSION_DENIED` resolves.
- [ ] **Step 4: Commit** (starter) `feat(proto): PERMISSION_DENIED error code`.

---

### Task 2: Identity model, exceptions, error mapping

**Files:**
- Create: `kraken-messaging/src/main/kotlin/io/damoreira/kraken/messaging/security/Identity.kt`
- Modify: `MessagingExceptions.kt`, `hub/HubErrors.kt`, `security/CallerContext.kt` (`KrakenForwardedAuthentication` carries `MessagePrincipal`, authorities `ROLE_<role>`, `isAuthenticated = true`)
- Test: `src/test/kotlin/.../security/IdentityTest.kt`, `hub/HubErrorsTest.kt` (add cases)

**Interfaces:**
- `data class MessagePrincipal(val userId: String?, val roles: Set<String>, val verified: Boolean)`
- `data class Identity(val principal: MessagePrincipal, val authentication: Authentication) { val userId get() = principal.userId; val roles get() = principal.roles }`
- `object LocalIdentity { fun current(): Identity? }` — from `SecurityContextHolder` per resolved question 1; returns null for null/anonymous.
- `class AccessDeniedException(message: String) : MessagingException(message)`
- `HubErrors.toException`: `PERMISSION_DENIED → AccessDeniedException(message)`.
- `KrakenForwardedAuthentication(token: String?, principal: MessagePrincipal)` with `authorities = roles.map { SimpleGrantedAuthority("ROLE_$it") }`; `token` nullable so an unverified/absent-token identity can still be installed (null token → nothing to re-forward).

- [ ] **Step 1: Write failing tests**
```kotlin
@Test fun `local identity from JwtAuthenticationToken maps sub and realm roles`()
@Test fun `local identity from forwarded authentication is passed through`()
@Test fun `local identity is null for anonymous or missing authentication`()
@Test fun `local identity from other authenticated token uses name and ROLE_ authorities`()
@Test fun `permission denied maps to AccessDeniedException`()   // HubErrorsTest
```
- [ ] **Step 2: Run** — `./gradlew :kraken-messaging:test --tests '*IdentityTest*' --tests '*HubErrorsTest*'` Expected: compile FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — PASS; then `./gradlew :kraken-messaging:test` (non-Docker classes still green; adjust compile sites for the renamed principal).
- [ ] **Step 5: Commit** `feat(security): Identity model, AccessDeniedException, PERMISSION_DENIED mapping`.

---

### Task 3: Forwarded identity resolution (replaces `ForwardedJwtAuthenticator`)

**Files:**
- Create: `security/ForwardedIdentityResolver.kt`; Delete: `security/ForwardedJwtAuthenticator.kt`
- Modify: `hub/InboundRequestDispatcher.kt` (use the resolver; install `KrakenForwardedAuthentication` whenever an identity exists, verified or not; `ctx.userId/roles` from it), `config/KrakenMessagingAutoConfiguration.kt` (bean without the `requireJwt` check; keep `ObjectProvider<JwtDecoder>`)
- Test: `security/ForwardedIdentityResolverTest.kt` (replaces the authenticator test), `fixtures/TestJwt.kt` (add `unsignedToken(subject, roles)` producing a `none`-alg or arbitrarily signed token for the unverified case)

**Interfaces:**
- `class ForwardedIdentityResolver(private val decoder: JwtDecoder?) { fun resolve(authorization: String?): Identity? /* null = anonymous */ }` throwing `UnauthenticatedException` for: malformed header, decoder present and token invalid, no decoder and token unreadable. Warns once per process when it first accepts an unverified identity.
- Unverified parse: `com.nimbusds.jwt.JWTParser.parse(token).jwtClaimsSet` → `subject`, `realm_access.roles`.

- [ ] **Step 1: Write failing tests**
```kotlin
@Test fun `no header is anonymous`()                                   // resolve(null) == null
@Test fun `valid token with decoder yields verified identity with roles`()
@Test fun `invalid token with decoder is UnauthenticatedException`()
@Test fun `token without decoder is parsed unverified`()               // verified == false, sub and roles present
@Test fun `unreadable token without decoder is UnauthenticatedException`()
@Test fun `malformed header is UnauthenticatedException`()
@Test fun `missing sub without decoder yields identity with null userId`()
@Test fun `dispatcher installs forwarded authentication for unverified identity`()  // InboundRequestDispatcher + CallerContext.currentUserId() inside a handler
```
- [ ] **Step 2: Run** — compile FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — `./gradlew :kraken-messaging:test --tests '*security*' --tests '*hub*'` PASS.
- [ ] **Step 5: Commit** `feat(security): trusted-by-default identity resolution`.

---

### Task 4: `@RequiresAuthentication`: annotation, compiled expression, evaluation

**Files:**
- Create: `handler/RequiresAuthentication.kt`, `handler/HandlerAuthorization.kt` (`CompiledAuthorization`, `MessageSecurityExpressionRoot`, `AuthorizationDecision`)
- Modify: `handler/HandlerMethod.kt` (`val authorization: CompiledAuthorization?`), `handler/HandlerRegistry.kt` (compile at scan; reject on `@EventHandler`), `handler/HandlerInvoker.kt` (`invokeCommand/invokeQuery(type, payload, ctx, identity: Identity?)`; evaluation before invoke; map thrown `AccessDeniedException` (both kinds) → `PERMISSION_DENIED`)
- Test: `handler/HandlerAuthorizationTest.kt`, `handler/HandlerRegistryTest.kt` (add), `handler/HandlerInvokerTest.kt` (add)

**Interfaces:**
- `annotation class RequiresAuthentication(val expression: String = "isAuthenticated()")`
- `class CompiledAuthorization(val expression: Expression, val source: String) { fun check(identity: Identity?, message: Any, ctx: MessageContext, label: String): AuthorizationDecision }`; `sealed interface AuthorizationDecision { Granted; Unauthenticated(message); Denied(message) }` — an exception during evaluation is `Denied("handler <label>: access denied (<ExceptionSimpleName>)")`.
- `class MessageSecurityExpressionRoot(authentication: Supplier<Authentication>, val principalValue: MessagePrincipal) : SecurityExpressionRoot(...)` (verify the 7.1 constructor in the Gradle cache; use the `Supplier<Authentication>` form); `getPrincipal()` returns `principalValue`; default role prefix `ROLE_`.
- `object HandlerAuthorizations { fun compile(source: String, label: String): CompiledAuthorization /* ParseException → IllegalStateException("Handler <label>: invalid @RequiresAuthentication expression '<src>': <reason>") */ }`
- Registry: `@RequiresAuthentication` on an `@EventHandler` → `IllegalStateException("Handler <label>: @RequiresAuthentication is not supported on @EventHandler")`.
- `HandlerInvoker`: `suspend fun invokeCommand(type, payload, ctx, identity: Identity?)`, same for `invokeQuery`; `invokeEvent` unchanged; `InMemoryBuses` and tests updated to pass `LocalIdentity.current()`.

- [ ] **Step 1: Write failing tests**
```kotlin
// HandlerAuthorizationTest
@Test fun `default expression grants authenticated and rejects anonymous`()
@Test fun `hasRole matches realm role and denies otherwise`()
@Test fun `hasAnyRole and hasAuthority work`()
@Test fun `message and ctx variables are available`()        // "#message.ownerId == #ctx.userId"
@Test fun `evaluation error is Denied with handler label`()  // "#message.missing" on Ping
@Test fun `invalid expression fails compile with label`()
// HandlerRegistryTest
@Test fun `requires authentication on event handler fails registration`()
@Test fun `handler method carries compiled authorization`()
// HandlerInvokerTest
@Test fun `annotated handler without identity is UNAUTHENTICATED`()
@Test fun `annotated handler with wrong role is PERMISSION_DENIED`()
@Test fun `annotated handler with matching role runs`()
@Test fun `unannotated handler accepts anonymous`()
@Test fun `handler throwing AccessDeniedException maps to PERMISSION_DENIED`()   // both our and Spring's class
```
- [ ] **Step 2: Run** — compile FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — `./gradlew :kraken-messaging:test --tests '*handler*'` PASS; `./gradlew build -x test` compiles the test module.
- [ ] **Step 5: Commit** `feat(handler): @RequiresAuthentication with Spring Security expressions`.

---

### Task 5: Wiring, configuration, integration

**Files:**
- Modify: `hub/HubBus.kt` (local path passes `LocalIdentity.current()`), `hub/InboundRequestDispatcher.kt` (passes the resolved identity), `config/KrakenMessagingProperties.kt` (remove `requireJwt`), `config/KrakenMessagingAutoConfiguration.kt` (removed-property check; startup `info` listing annotated handlers), `META-INF/spring-configuration-metadata.json`, `kraken-messaging-test/.../InMemoryBuses.kt` (identity from `LocalIdentity.current()`), `src/test/kotlin/.../e2e/ServiceB.kt`/`TwoServicesEndToEndTest.kt` (drop `require-jwt`, add the event-handler-to-unannotated-command test and an annotated-handler denial test)
- Test: `hub/HubBusTest.kt` (add), `config/AutoConfigurationTest.kt` (add), `e2e/TwoServicesEndToEndTest.kt` (add)

**Interfaces:**
- Auto-config: `check(!environment.containsProperty("kraken.messaging.security.require-jwt")) { "kraken.messaging.security.require-jwt was removed; annotate handlers with @RequiresAuthentication" }` in the core configuration; `ForwardedIdentityResolver` bean from `ObjectProvider<JwtDecoder>`; an `ApplicationRunner`-free `info` log at `HandlerRegistry` registration time listing `label → expression`.

- [ ] **Step 1: Write failing tests**
```kotlin
// HubBusTest (InProcessHub)
@Test fun `remote annotated handler rejects sender without token as UnauthenticatedException`()
@Test fun `remote annotated handler denies wrong role as AccessDeniedException`()
@Test fun `remote annotated handler accepts matching role`()
@Test fun `nested denied send inside annotated handler propagates AccessDeniedException`()
@Test fun `local short-circuit enforces the same rules`()
@Test fun `receiver without JwtDecoder accepts forwarded token unverified`()
// AutoConfigurationTest
@Test fun `starts without JwtDecoder`()
@Test fun `require-jwt property fails startup with removal hint`()   // via property and via env-style source
// E2E
@Test fun `command from event handler reaches unannotated handler without token`()
@Test fun `annotated handler denies user without role end to end`()
```
- [ ] **Step 2: Run** — compile/assert FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** — full `./gradlew build` with the Docker env; E2E twice. Expected: PASS, no new warnings.
- [ ] **Step 5: Commit** `feat(config): trusted-by-default security wiring, require-jwt removed`.

---

### Task 6: Docs

**Files:**
- Modify: `README.md` (security section, configuration table, limitations), umbrella worktree from Task 1: `docs/02-messaging.md` (error table row `PERMISSION_DENIED`, security paragraph, `@RequiresAuthentication` example), `docs/06-security.md` ("JWT forwarding over the hub" and "Service-to-service calls without a user" rewritten for trusted-by-default), `docs/05-services/*.md` (remove `require-jwt` mentions, show an annotated handler)

- [ ] **Step 1: Write the docs; `grep -rn "require-jwt" README.md /Users/damoreira/.projects/kraken-workspace/kraken/.worktrees/proto-permission-denied/docs` returns nothing.**
- [ ] **Step 2: Commit** starter `docs: opt-in handler authentication` and umbrella branch `docs: trusted-by-default hub calls, @RequiresAuthentication`; leave the umbrella branch unmerged.
