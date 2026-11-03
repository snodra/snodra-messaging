# Sagas, deadlines and the external-integration toolkit: design

Status: agreed in conversation with the project owner on 2026-10-31; builder calls marked.
**Approved by the owner on 2026-11-03** for implementation: roadmap phase S1 (Kotlin) after P5c,
then S2 (Rust), in `kraken/docs/superpowers/specs/2026-10-31-hub-axon-parity-design.md`.

## Intent

kraken replaced Axon Framework and Axon Server with its own library and hub, and left sagas and
deadlines out of v1 (ADR 0002, 01-architecture). The owner wants Axon's saga feature set in full,
mainly to drive **integrations with external systems**: a long-running process that reacts to
kraken events, asks an outside system to do something, waits for the answer and handles the
answer that never comes. There is no concrete use case yet, so the feature must be general and
complete rather than shaped around one flow.

## Decisions taken with the owner

1. **Full Axon parity**, not a minimal version: `@Saga`, `@StartSaga`, `@SagaEventHandler` with
   association properties, `@EndSaga`, runtime association changes, ending from code, persistent
   saga state, one event at a time per instance, no double application of a redelivered event,
   deadlines that can be scheduled and cancelled.
2. **Sagas live in the library (`kraken-messaging`), inside each service, not in the hub.** As in
   Axon, where sagas run in the framework and Axon Server only routes and stores. The hub stays a
   stateless router (ADR 0004); it only carries commands a saga sends to another service.
3. **Kotlin first.** The Rust crate gets the same model later; the MongoDB collection shapes in
   this document are the contract it will follow.
4. **Sagas only speak kraken messages.** Every external communication starts with a kraken
   `Command` or `Event`. A saga never performs an HTTP call or produces to a foreign broker.
5. **Integration adapters live inside the service that owns the saga.** A saga's outbound command
   is handled by an adapter in the same service (local short-circuit, no hub).
6. **The external system answers through one of three channels**: a record on **its own Kafka
   topic** (not `kraken.events`), a **webhook** call, or **long polling**. All three end as an
   ordinary kraken event routed to the saga by association.

## Goals

- Write a saga as a Kotlin class with annotated handlers, as in Axon.
- Saga state survives restarts and crashes; each event is applied exactly once per saga instance;
  concurrent events for the same instance never interleave.
- A saga's outbound commands never fire for a saga state that did not commit, and fire at least
  once for one that did.
- Deadlines fire at least once after their due time, also after a restart, and never for a
  cancelled deadline or an ended saga.
- Integration adapters: an outbound call with a redelivered command never reaches the external
  system twice for the same idempotency key; inbound answers from any of the three channels are
  deduplicated and become kraken events.
- A test fixture that drives a saga with events and time and asserts commands, deadlines and the
  end of the saga, without Kafka or MongoDB.
- One end-to-end example in the sandbox that exercises all three inbound channels against a fake
  external system.

## Non-goals

- Sagas in the Rust crate (later, same collections).
- **Replaying past events into a newly deployed saga type** (owner: out of scope for now). A new
  saga type reacts only to events consumed after it is deployed. A later replay would need its own
  consumer reading back through `kraken.events`' 30-day retention; nothing here may rule it out.
- Deadlines outside sagas (no aggregate- or bean-scoped deadlines).
- Event sourcing, aggregates, aggregate-scoped deadlines: kraken has no aggregates.
- Sagas spanning several services' databases (a saga belongs to one service; it talks to others
  through commands and events).
- A visual saga monitor. Metrics and logs only (below).
- Generic HTTP or Kafka client libraries: adapters use whatever client the service picks.

## Anti-goals (would make it a failure even if it works)

- A saga handler that calls an external system directly, or a design that makes it easy.
- State in the hub.
- An external system called twice because a command was redelivered or a service restarted.
- A saga that silently stops: an event that matched no instance and should have, or a deadline
  that never fired, must be visible in logs and metrics.

## Programming model

```kotlin
@Saga
class PaymentSaga {
    // State: plain properties, persisted as JSON (Jackson) after each handled message.
    var orderId: String? = null
    var paymentRef: String? = null
    var attempts = 0
    var timeoutDeadline: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: OrderPlaced, saga: SagaLifecycle, commands: SagaCommands, deadlines: DeadlineManager) {
        orderId = e.orderId
        paymentRef = "pay-${e.orderId}"
        saga.associateWith("paymentRef", paymentRef!!)
        commands.send(RequestCharge(paymentRef!!, e.amount))          // dispatched after commit
        timeoutDeadline = deadlines.schedule(Duration.ofMinutes(15), "charge-timeout")
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "paymentRef")
    fun on(e: ChargeSucceeded, deadlines: DeadlineManager) {
        timeoutDeadline?.let(deadlines::cancel)
    }

    @DeadlineHandler("charge-timeout")
    fun timeout(saga: SagaLifecycle, commands: SagaCommands) {
        commands.send(CancelCharge(paymentRef!!))
        saga.end()
    }
}
```

| Element | Meaning |
|---|---|
| `@Saga` | class discovered at startup; a new instance per saga, never a Spring singleton; needs a no-argument constructor |
| `@StartSaga(forceNew = false)` | the handler may create an instance when none is associated; `forceNew = true` always creates a new one |
| `@SagaEventHandler(associationProperty, keyName = associationProperty, associationHeader = "", payloadType = Event::class)` | the event's property value (or, when `associationHeader` is set, that message header's value) is looked up as association `keyName = value` among this type's live instances; every match handles it. `payloadType` narrows a handler declared on a supertype |
| `@EndSaga` | the instance ends after the handler returns; allowed on saga event handlers and deadline handlers |
| `@DeadlineHandler(deadlineName)` | runs when a deadline of that name scheduled by this instance is due |
| `SagaLifecycle` (handler parameter) | `sagaId`, `associateWith(key, value)`, `removeAssociationWith(key, value)`, `end()` |
| `SagaCommands` (handler parameter) | `send(command, routingKey?)` and `publish(event, key?)`: recorded in the saga's transaction, dispatched after commit |
| `DeadlineManager` (handler parameter) | `schedule(Duration \| Instant, name, payload? = null): String` (deadline id), `cancel(id)`, `cancelAll(name)`; saga-scoped |
| Other parameters | the event (first), `MessageContext`, the deadline payload (for `@DeadlineHandler`), and any Spring bean by type |

> **Builder call:** collaborators are handler parameters resolved per call (lifecycle objects from
> the saga runtime, everything else from the Spring context), not injected fields. Saga state then
> contains only data, and a deserialised instance never needs re-injection.

Rules, checked at startup with a clear error naming the class and method:
- every handler's first parameter is an `Event` (saga event handlers) or absent / the payload type
  (deadline handlers); `associationProperty` names a property of that event;
- handlers are non-suspend (they run inside the store's Mongo transaction, which is thread-bound);
- a saga has at least one `@StartSaga` handler; saga classes are serialisable by the library's
  Jackson mapper (round-trip check of a fresh instance at startup).

## Delivery and consistency

Sagas are fed by the service's existing Kafka consumer (`kraken.events`) and by the inbound
channels below.

> **Builder call (isolation):** saga handling runs in **its own transaction**, separate from the
> transaction of the service's ordinary `@EventHandler`s, with its own idempotency marker
> (`processed_events` id `saga:<eventId>`). A failing ordinary handler never rolls back a saga,
> and the reverse.

For each event, in the saga transaction:

1. resolve matching instances per saga type (association lookup), plus a new instance when a
   `@StartSaga` handler applies and none matched (or `forceNew`);
2. for each instance: load, invoke, apply lifecycle changes, write state with an **optimistic
   version check**; record `SagaCommands` in `saga_dispatch` and deadline changes in `deadlines`;
3. insert the saga marker and commit. A version conflict aborts the transaction and the event is
   retried (bounded, with backoff), so two events for one instance never interleave.

Exactly-once per instance follows from the saga marker being in the same transaction as the state
change. A redelivered event finds its marker and is skipped.

> **Builder call (one instance per start key):** when `forceNew = false`, at most one live
> instance of a saga type exists per **start association** (the association the `@StartSaga`
> handler resolved on). `sagas` stores it as `startKey = "<type>|<key>=<value>"` with a unique
> index partial on `ended: false`. Two concurrent start events for the same key: one inserts, the
> other hits the duplicate key, aborts and is retried, and then finds the instance and is handled
> by it. `forceNew = true` instances carry no `startKey`.

> **Builder call (exhausted retries):** a saga event that still fails after the retries (handler
> exception, endless conflicts) is never skipped silently. It is written to `saga_failures`
> (`_id` = `<sagaType>:<eventId>`, the event, the error, `attempts`, `firstFailedAt`,
> `lastFailedAt`) together with the saga marker, so the consumer moves on; it is counted in
> `kraken_saga_events_total{outcome="parked"}` and logged at error. An admin API on the library
> (`SagaFailures.retry(id)`, `retryAll(type)`, `discard(id)`) re-delivers a parked event into the
> saga runtime once the cause is fixed. Ordinary `@EventHandler`s keep today's behaviour.

> **Builder call (deferred dispatch):** commands and events a saga emits are **deferred until commit** through a
> `saga_dispatch` collection (a saga-side outbox), unlike Axon, which sends from the handler.
> Integration adapters call external systems, so a command must never run for a saga state that
> rolled back; the local short-circuit would otherwise run the adapter inside the saga's
> transaction. A relay dispatches each entry once after commit (commands through the normal
> `CommandBus`, events through the normal outbox), retries with backoff on failure, and marks
> it done; each dispatched command carries `sagaId:stepNo` as its idempotency key header.

> **Builder call (order):** entries of one saga are dispatched **in the order they were
> recorded**: `saga_dispatch` carries a per-saga `seq` (the saga's `version` at recording plus
> the entry's index), and the relay claims a saga (lease) and dispatches its entries one after the
> other; entries of different sagas run in parallel. A step blocked in retry holds back that
> saga's later steps only. The idempotency key is `"<sagaId>:<seq>"`.

Command replies: a saga does not wait for replies. Failures come back as a library event
`SagaCommandFailed(sagaId, commandType, idempotencyKey, reason, message)` that a saga may handle
with `@SagaEventHandler(associationProperty = "sagaId")`; unhandled, it is logged at error and
counted.

> **Builder call (retry taxonomy):** the relay retries only **transient** failures, with
> exponential backoff up to a configured limit: `HubUnavailable`, `MessageTimeout`,
> `HandlerDisconnected`, and adapter failures marked retryable. **Permanent** failures become
> `SagaCommandFailed` at once: `NoHandler` (`reason = NO_HANDLER`), `Unauthenticated` /
> `AccessDenied` (`DENIED`), `InvalidMessage` (`INVALID`), any other handler error (`FAILED`),
> an unknown outcome (`UNKNOWN_OUTCOME`, below), and an exhausted transient retry (`EXHAUSTED`).
> Adapters do not retry internally: a retryable external failure is thrown as
> `RetryableExternalException` and the relay owns the backoff, so retries never stack and no relay
> thread blocks on an adapter's sleep.

> **Builder call (identity):** saga commands carry no user token: the triggering event's user is
> not a caller. They reach handlers without `@RequiresAuthentication` (adapters, in-service
> handlers). Commanding a handler that requires authentication fails with `DENIED` until the
> `kraken-system` service identity (06-security, planned) is forwarded; that is documented, not
> built here.

## Deadlines

- Stored in `deadlines`: `_id` (deadline id), `sagaType`, `sagaId`, `name`, `payload` (JSON, type
  name), `dueAt`, `status` (`SCHEDULED | FIRED | CANCELLED`), `claimedBy`, `claimedUntil`.
- A scheduler in every instance polls due deadlines, claims one with a lease
  (`findAndModify`, `claimedUntil = now + lease`), and fires it in a transaction that loads the
  saga, runs the `@DeadlineHandler`, writes state, and sets `FIRED`. Several service instances
  can run the scheduler; the lease makes each deadline fire on one of them, and an expired lease
  (crashed instance) makes it fire again: **at least once**.
- Cancelling sets `CANCELLED` in the caller's transaction. Ending a saga cancels its scheduled
  deadlines. A deadline whose saga is gone or ended is marked `CANCELLED` when found due.
- Time comes from an injectable `Clock`; the test fixture advances it.

## Integration toolkit

All three inbound channels and the outbound side share one rule: **the edge of the service turns
the outside world into kraken messages, deduplicated.**

**Outbound: idempotent adapter handlers.** An adapter is an ordinary `@CommandHandler` (or
`@EventHandler`) in the same service. The library adds `@IdempotentHandler(key = "<SpEL>")`,
default the `idempotency-key` header set by `SagaCommands`:
- `external_calls` collection: `_id` = key, `status` (`IN_PROGRESS | SUCCEEDED | FAILED`),
  `reply` (JSON), `attempts`, `updatedAt`;
- a call whose key is `SUCCEEDED` returns the stored reply without running the handler; `FAILED`
  re-throws the stored error (permanent);
- `RetryableExternalException` thrown by the body resets the record to "not started" and is
  retried by the saga relay (above), not inside the handler;
- **crash in the middle of a call**: an `IN_PROGRESS` record older than its lease means the
  outcome is unknown. What happens is the adapter's declaration:
  `@IdempotentHandler(key = ..., externalIdempotent = false)` (default) moves it to `UNKNOWN`, never
  calls again automatically, and reports `SagaCommandFailed(reason = UNKNOWN_OUTCOME)` so the saga
  decides (poll the partner, compensate, or ask a human); `externalIdempotent = true` (the partner
  de-duplicates by the key the adapter passes it) retries.
The handler body passes the key to the external system when it supports one.

**Inbound 1: external Kafka topics.** Configuration
`kraken.messaging.external.kafka.sources.<name>: { bootstrap-servers, topic, group-id?, properties }`
and a bean `ExternalRecordTranslator` named `<name>` with
`fun translate(record): TranslatedEvent?` (`TranslatedEvent(event, dedupeKey)`; `null` = ignore).
The library runs one consumer per source; each record is translated and dispatched **locally**
(sagas and `@EventHandler`s of this service) in a transaction with a `processed_events` marker
keyed by the dedupe key (default `"<name>:<partition>:<offset>"`). Translated events are not
republished to `kraken.events`.

> **Builder call:** local dispatch keeps a partner's events private to the integrating service;
> a service that wants to share them publishes its own kraken event from a handler.

**Inbound 2: webhooks.** `ExternalEventIngress.accept(source, dedupeKey, event)` runs the same
local dispatch synchronously in one transaction and returns `ACCEPTED | DUPLICATE`; services
expose their own endpoint and call it after verifying the request with the library's
`WebhookSignatureVerifier` (shared secret header or HMAC-SHA256 of the body, current and previous
secret, constant time, the same rules as the Keycloak webhook). A ready `@RestController` is not
provided; the README shows the five-line controller.

**Inbound 3: long polling.** No new machinery: a saga schedules a polling deadline, the deadline
handler sends a poll command to its adapter, the adapter asks the external system and publishes
a result event when there is one (deduplicated by `@IdempotentHandler` on a key that includes the
poll attempt), and the saga reschedules or ends. The README documents the pattern, and the
example uses it.

## Storage (MongoDB, per service database)

| Collection | Fields | Indexes |
|---|---|---|
| `sagas` | `_id` (saga id, UUID), `type` (class wire name, `@Saga(name = …)` or FQCN), `state` (JSON), `associations` [`{k, v}`], `startKey?`, `ended`, `endedAt?`, `version`, `createdAt`, `updatedAt` | `(type, associations.k, associations.v)` partial on `ended: false`; unique `startKey` partial on `ended: false` and `startKey` present; TTL on `endedAt` (configurable, default 30 days) |
| `saga_dispatch` | `_id`, `sagaId`, `seq`, `kind` (`COMMAND \| EVENT`), `type`, `payload`, `headers`, `routingKey?`, `createdAt`, `dispatchedAt?`, `attempts`, `nextAttemptAt?`, `failedAt?` | `(sagaId, seq)`; `(dispatchedAt, nextAttemptAt)`; TTL on `dispatchedAt` |
| `deadlines` | as above | `(status, dueAt)`; `(sagaId, status)`; TTL on fired/cancelled |
| `external_calls` | as above, `status` also `UNKNOWN` | TTL on `updatedAt` (default 30 days) |
| `saga_failures` | as above | `(sagaType, lastFailedAt)` |

These shapes are the contract the Rust crate will follow.

## Observability

- Logs carry `sagaType`, `sagaId`, `correlationId` in the MDC while a saga handles a message.
- Micrometer metrics (the library has none yet; these are the first): `kraken_sagas_active{type}`,
  `kraken_sagas_started_total{type}`, `kraken_sagas_ended_total{type}`,
  `kraken_saga_events_total{type,outcome}` (`handled | no_match | conflict | parked`),
  `kraken_deadlines_due{name}` (gauge of overdue), `kraken_deadlines_fired_total{name}`,
  `kraken_saga_dispatch_pending`, `kraken_external_calls_total{handler,outcome}`.
- An event that matched no saga instance and started none is counted as `no_match` (debug log),
  so a broken association shows up in metrics.

## Testing support (`kraken-messaging-test`)

`SagaTestFixture(PaymentSaga::class)` in the style of Axon's:
`givenEvents(...)`, `givenCurrentTime(...)`, `whenEvent(e)`, `whenTimeElapses(duration)`,
`expectActiveSagas(n)`, `expectAssociation(key, value)`, `expectDispatchedCommands(...)`,
`expectPublishedEvents(...)`, `expectScheduledDeadline(name, at)`, `expectNoScheduledDeadlines()`,
`expectSagaEnded()`. In memory, no Kafka or MongoDB, using the real handler discovery and
serialisation (a state round-trip on every step catches non-serialisable state).

## Example (sandbox)

A fourth example in `kraken-messaging/examples`: service-b gains a `GreetingDeliverySaga` for an
external "postcard printer":
- on `GreetingDelivered` it sends `PrintPostcard` (handled by an adapter in service-b that calls
  a fake printer over HTTP with an idempotency key) and schedules a 30 s timeout;
- the fake printer (a small container in `compose.local.yaml`) answers one of three ways chosen by
  the greeting name: posts a webhook to service-b, produces to its own topic `printer.events`, or
  makes service-b poll `GET /jobs/{id}`; a fourth name makes it never answer (timeout path), and
  a fifth makes it accept the request and crash before replying (unknown-outcome path);
- the saga ends on `PostcardPrinted`, or on the timeout with `PostcardFailed` published;
- `smoke.sh` covers all three channels and the timeout.

## Constraints

- Spring Boot 4.1, Kotlin 2.4, MongoDB transactions are thread-bound: saga, deadline and adapter
  handlers that rely on the transaction are non-suspend.
- No new infrastructure: MongoDB and Kafka only.
- Configuration under `kraken.messaging.sagas.*`, `kraken.messaging.deadlines.*`,
  `kraken.messaging.external.*`; the saga runtime, relay and deadline scheduler start only when a
  `@Saga` exists, `external_calls` only with an `@IdempotentHandler`, the external consumers only
  with a configured source.
- Public API documented in `kraken/docs/02-messaging.md` (new section) and an ADR
  (`0013-sagas-in-the-library`) recording decision 2 and the deferred-dispatch builder call;
  01-architecture's Sagas/deadlines row changes from "not replaced".

## Left to the builder

Package layout, default intervals and leases, retry defaults, how the consumer exposes its
transaction to the saga runtime, metric implementation details, the fake printer's
implementation language, and the README wording.
