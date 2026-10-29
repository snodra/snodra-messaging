# Opt-in authentication for command and query handlers: design

Date: 2026-10-29. Status: builder check done, ready to plan.

## Intent

Commands and queries between kraken services are service-to-service calls on a private network. Today
the receiving library rejects every forwarded request that carries no valid JWT
(`kraken.messaging.security.require-jwt=true`), which breaks the common case where a command is sent
from an event handler or a background job that has no user token. The new model inverts this:
requests are trusted by default, and a handler that needs an authenticated user opts in with an
annotation carrying a Spring-Security-style expression. The receiving library evaluates it before
invoking the handler; a failure travels back to the sender as a typed exception.

## Goals

- `@RequiresAuthentication(expression = "...")` on `@CommandHandler` and `@QueryHandler` methods.
  Default expression `isAuthenticated()`.
- Evaluation on the receiving side, before the handler runs, for both the hub path and the local
  short-circuit path, against the identity forwarded with the request.
- Two outcomes surfaced at the sender: `UnauthenticatedException` when the request carries no
  identity, `AccessDeniedException` when it carries one that fails the expression.
- Forwarded tokens are still read for `MessageContext.userId`/`roles`; a missing token no longer
  rejects the request.
- `kraken.messaging.security.require-jwt` is removed.

## Non-goals

- No change to hub routing. The hub keeps forwarding handler errors unchanged.
- No authentication of the service itself (mTLS, client credentials); the private network stays the
  trust boundary, as decided in `06-security.md`.
- No minting of a `kraken-system` token for background work in this change (`06-security.md` already
  foresees one; a later change can plug it into `forward-jwt`).
- `@RequiresAuthentication` on `@EventHandler` is not supported: events never carry an identity, so the
  annotation there is a startup error.
- No Rust crate changes.

## Anti-goals

- A request must never be rejected only because it has no token.
- The library must never log or forward an identity it could not read; a token that fails validation
  when a `JwtDecoder` exists is still rejected loudly (`UNAUTHENTICATED`), because that is a bug or an
  attack, not a trusted peer.

## The annotation

```kotlin
package io.damoreira.kraken.messaging.handler

@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class RequiresAuthentication(val expression: String = "isAuthenticated()")
```

It lives in the starter, next to `@CommandHandler`/`@QueryHandler`, because it annotates handler
methods in services, which already depend on the starter. The `*-api` modules are untouched.

```kotlin
@CommandHandler
@RequiresAuthentication(expression = "hasRole('kraken-user')")
suspend fun handle(cmd: LogWorkout, ctx: MessageContext): LogWorkoutResult { ... }

@QueryHandler
@RequiresAuthentication   // isAuthenticated()
fun handle(q: GetWorkout, ctx: MessageContext): GetWorkoutResult { ... }

@CommandHandler
fun handle(cmd: RecomputeStats): Unit { ... }   // no annotation: any caller, including event handlers
```

## Expression language

Spring Security's `SecurityExpressionRoot` is the root object, so `isAuthenticated()`,
`hasRole('x')`, `hasAnyRole(...)`, `hasAuthority(...)`, `principal` and `authentication` work as in
`@PreAuthorize`. Roles come from the forwarded JWT claim `realm_access.roles`, exposed as
authorities `ROLE_<role>`, so `hasRole('kraken-admin')` matches the Keycloak realm role
`kraken-admin`. Two variables are added for message-aware rules:

- `#message`: the decoded command or query;
- `#ctx`: the `MessageContext` (`userId`, `roles`, `correlationId`, ...).

Example: `@RequiresAuthentication("hasRole('kraken-user') and #message.ownerId == #ctx.userId")`.

Expressions are parsed once when the handler is registered; a syntax error fails startup with the
bean and method name. Evaluation happens per request.

## Receiving side

```text
Request arrives (hub stream)                 Local short-circuit (CommandBus/QueryBus)
  headers.authorization?                       SecurityContextHolder.authentication?
          │                                              │
          ▼                                              ▼
  ForwardedIdentity.resolve(header)            identity from the current Authentication
          │                                              │
          └──────────────► HandlerInvoker.invoke(type, payload, ctx, identity) ◄──────┘
                                   │
                        handler has @RequiresAuthentication?
                         no  → invoke handler
                         yes → identity absent  → Failure(UNAUTHENTICATED, "handler X requires authentication")
                               expression false → Failure(PERMISSION_DENIED, "handler X: access denied")
                               expression true  → invoke handler
```

### Identity resolution (replaces `ForwardedJwtAuthenticator`)

| Situation | Result |
|---|---|
| no `authorization` header | anonymous: `userId = null`, no roles, request proceeds |
| token present, `JwtDecoder` bean configured, token valid | identity `{sub, realm roles}`, `verified = true` |
| token present, `JwtDecoder` configured, token invalid or expired | `Failure(UNAUTHENTICATED, <reason>)` (unchanged) |
| token present, no `JwtDecoder` | claims parsed without signature verification: identity `{sub, realm roles}`, `verified = false`; one `warn` per process at the first occurrence ("forwarded identities are trusted without verification: no JwtDecoder bean") |
| malformed header | `Failure(UNAUTHENTICATED, "malformed authorization header")` |

**Builder call:** an unverified identity counts as authenticated for `@RequiresAuthentication`. The
peer that forwarded it is trusted by the network model; verification is about catching forged or
expired tokens when the service can, not about distrusting the peer.

The resolved identity is installed as `KrakenForwardedAuthentication` in the `SecurityContext` for
the duration of the handler (authorities `ROLE_<role>`), so nested sends re-forward the same token
and `CallerContext.currentUserId()` works inside handlers, exactly as today.

### Error codes

`ErrorCode` gains `PERMISSION_DENIED = 7` in `kraken/protocol/hub.proto` (additive; copies synced to
the hub and the starter). The hub forwards handler `Reply{error}` unchanged and already records any
handler error as outcome `handler_error`, so it needs no code change beyond regenerating from the
proto. The Kotlin mapping becomes:

| Code | Sender sees |
|---|---|
| `UNAUTHENTICATED` | `UnauthenticatedException` |
| `PERMISSION_DENIED` | `AccessDeniedException` (new, extends `MessagingException`) |

Both are produced by `HandlerInvoker`, so the local short-circuit throws the same exceptions without
a wire round trip.

## Sending side

Unchanged: `forward-jwt` (default `true`) still puts the caller's bearer token in `headers.authorization`
when the current `SecurityContext` has one (`JwtAuthenticationToken`, `BearerTokenAuthentication` or a
`KrakenForwardedAuthentication` from an outer handler). A sender with no token sends none; whether that
is acceptable is now the handler's decision, not the transport's.

## Configuration

| Key | Change |
|---|---|
| `kraken.messaging.security.require-jwt` | removed; binding it fails with a message pointing to `@RequiresAuthentication` |
| `kraken.messaging.security.forward-jwt` | unchanged, default `true` |

The startup check "require-jwt=true but no JwtDecoder" disappears. Kafka-only tools need no security
configuration at all.

## Observability

- One `debug` line per rejected request with handler label, code and (never the token) the reason.
- Startup `info` line listing handlers that carry `@RequiresAuthentication` and their expressions.

## Testing

- Unit (`handler`): expression parse error fails registration naming the method; `@EventHandler` with
  the annotation fails registration; `isAuthenticated()`, `hasRole`, `hasAnyRole`, `#message`/`#ctx`
  rules evaluate correctly against a `KrakenForwardedAuthentication`; anonymous → `UNAUTHENTICATED`;
  wrong role → `PERMISSION_DENIED`; unannotated handler accepts anonymous callers.
- Unit (`security`): identity resolution table above, including unverified parsing and the one-time
  warning.
- Integration (`InProcessHub`): sender without token → `UnauthenticatedException`; sender with a role
  that fails the expression → `AccessDeniedException`; matching role → result; nested send from inside
  an annotated handler re-forwards the token; receiver without `JwtDecoder` accepts a forwarded token
  unverified.
- E2E (real hub): service B drops `require-jwt`; the expired-token test still expects
  `UnauthenticatedException` because B configures a `JwtDecoder`; a new test sends a command from an
  event handler (no token) to an unannotated handler and succeeds.
- Auto-configuration: no `JwtDecoder` bean → context starts; `require-jwt` property present → binding
  failure with the hint.

## Docs

- Starter README: security section rewritten around the annotation and the identity table.
- Umbrella `02-messaging.md` (error table, security paragraph), `06-security.md` (JWT forwarding section
  and the "service to service" paragraph), `05-services/*.md` examples that show `require-jwt`.

## Left to the builder

- Whether to parse unverified claims with Nimbus `SignedJWT.parse` or Spring's `JwtDecoder`-free
  helpers; any correct JSON-payload parse is fine.
- Exact message texts beyond the two quoted above.
- Whether the expression root exposes `#message` as the typed object or as a map (typed preferred).

## Resolved builder questions (all builder calls)

1. **Identity model and the local path.** One type feeds both paths:
   `Identity(userId: String?, roles: Set<String>, verified: Boolean, authentication: Authentication)`.
   Hub path: built by the identity resolver from the forwarded header. Local path: built from the
   current `SecurityContextHolder.authentication` without re-validation (Spring already validated it at
   ingress): `JwtAuthenticationToken` → `sub` and `realm_access.roles`; `BearerTokenAuthentication` →
   principal attributes `sub` and `realm_access.roles`; `KrakenForwardedAuthentication` → as is; any
   other authenticated `Authentication` → `userId = name`, roles from its `ROLE_*` authorities; null or
   `AnonymousAuthenticationToken` → no identity. The local path installs nothing new (the context is
   already there). Both paths produce the same two failures.
2. **Where the annotation is read.** On the concrete method of the bean's target class (same scan as
   `@CommandHandler`); an annotation on an interface or superclass method that the bean overrides is
   not inherited, which is documented. Class-level use is out of scope.
3. **Expression engine.** A hand-built `SpelExpressionParser` with a `StandardEvaluationContext` whose
   root is a `SecurityExpressionRoot` subclass (`MessageSecurityExpressionRoot`); independent of
   `@EnableMethodSecurity`. Anonymous callers never reach evaluation: the annotation means "requires
   authentication", so `permitAll()`/`isAnonymous()` are meaningless there (documented).
   `KrakenForwardedAuthentication.isAuthenticated()` is `true`.
4. **`principal` shape.** `principal` is always `MessagePrincipal(userId: String?, roles: Set<String>,
   verified: Boolean)` built from the `Identity`, for both paths; `#ctx.userId` is the documented way
   to reference the caller. `ForwardedJwtAuthenticator.Principal` is renamed to `MessagePrincipal`
   (`validated` → `verified`).
5. **Wire compatibility.** `PERMISSION_DENIED = 7` is additive. The Rust hub keeps unknown enum values
   as raw integers and forwards handler errors unchanged; it only needs the regenerated proto. Older
   Kotlin clients map an unknown code to `RemoteHandlerException` (degrades, does not break). Copies
   are synced by hand (`cmp` check in the plan); the `UNAUTHENTICATED` comment drops "no role".
6. **Removed property.** The core auto-configuration checks
   `environment.containsProperty("kraken.messaging.security.require-jwt")` and fails with
   "kraken.messaging.security.require-jwt was removed; annotate handlers with @RequiresAuthentication".
7. **Handler-thrown access errors.** `HandlerInvoker` maps a thrown `AccessDeniedException` (ours) or
   `org.springframework.security.access.AccessDeniedException` to `PERMISSION_DENIED`, so a nested
   send that was denied propagates to the original sender as `AccessDeniedException`.
8. **Unverified parsing.** Without a `JwtDecoder` the payload is decoded (base64url JSON) with no
   signature or `exp` check (the peer validated it at ingress; expiry during a chain must not break
   trusted calls); an unparsable token is `Failure(UNAUTHENTICATED, "unreadable token")`; a missing
   `sub` yields an authenticated identity with `userId = null`.
