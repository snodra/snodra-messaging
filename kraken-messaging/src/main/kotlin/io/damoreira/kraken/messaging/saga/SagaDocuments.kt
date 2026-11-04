package io.damoreira.kraken.messaging.saga

import org.springframework.data.annotation.Id
import org.springframework.data.mongodb.core.mapping.Document
import java.time.Instant

/**
 * One saga instance in the `sagas` collection. [state] is the saga object as JSON **text** (Jackson, the library's codec: its
 * exact output, so a `BigDecimal` keeps its scale and a map key may hold a dot or start with `$`),
 * [version] the optimistic version every write checks. While [ended] is false the instance is live: its [associations]
 * route events to it and its [startKey] (`"<type>|<key>=<value>"`, absent for `forceNew` instances) is unique. Ended
 * instances expire through the TTL index on [endedAt].
 */
@Document("sagas")
data class SagaDocument(
    @Id val id: String,
    val type: String,
    val state: String,
    val associations: List<Association> = emptyList(),
    val startKey: String? = null,
    val ended: Boolean = false,
    val endedAt: Instant? = null,
    val version: Long = 0,
    val createdAt: Instant,
    val updatedAt: Instant,
)

/** An association `k = v` of a saga instance: events whose association value is [v] for key [k] are routed to it. */
data class Association(val k: String, val v: String)

/** Whether a [SagaDispatchDocument] is a command (through the `CommandBus`) or an event (through the outbox). */
enum class DispatchKind { COMMAND, EVENT }

/**
 * One command or event a saga recorded in its transaction, waiting in the saga-side outbox `saga_dispatch`. [seq] orders
 * the entries of one saga (`version * 1000 + index`); [routingKey] is the command's routing key or the event's key.
 * [dispatchedAt] is set once sent, [failedAt] when given up on.
 *
 * A command given up on carries its outcome with it: [failureReason] and [failureMessage] (what the `SagaCommandFailed`
 * says) are written in the same update as [failedAt], and [noticePending] is true until that event has been delivered
 * locally (review amendment, ruling 28). A pending notice is retried by the relay with its own bounded backoff
 * ([noticeAttempts], [noticeNextAt]; the next try is not before [noticeNextAt], which also reserves the delivery for the
 * instance that claimed it) and never makes the relay send the command again. All of these are absent while unused.
 */
@Document("saga_dispatch")
data class SagaDispatchDocument(
    @Id val id: String,
    val sagaId: String,
    val seq: Long,
    val kind: DispatchKind,
    val type: String,
    val payload: String,
    val headers: Map<String, String> = emptyMap(),
    val routingKey: String? = null,
    val createdAt: Instant,
    val dispatchedAt: Instant? = null,
    val attempts: Int = 0,
    val nextAttemptAt: Instant? = null,
    val failedAt: Instant? = null,
    val failureReason: SagaCommandFailed.Reason? = null,
    val failureMessage: String? = null,
    val noticePending: Boolean? = null,
    val noticeAttempts: Int? = null,
    val noticeNextAt: Instant? = null,
)

/**
 * The lease of one saga's dispatch order (`saga_dispatch_leases`, added by the S1 plan): the relay instance that holds it
 * dispatches that saga's entries one after the other. An expired lease is as good as none; the TTL index only tidies up.
 */
@Document("saga_dispatch_leases")
data class SagaDispatchLeaseDocument(
    @Id val sagaId: String,
    val owner: String,
    val until: Instant,
)

/** Lifecycle of a [DeadlineDocument]. */
enum class DeadlineStatus { SCHEDULED, FIRED, CANCELLED }

/**
 * A deadline of a saga instance in `deadlines`. [payload] is the JSON text of the payload (any JSON value, not only an
 * object) and [payloadType] its type name; both absent when there is none. A scheduler instance claims a due deadline
 * by setting [claimedBy] and [claimedUntil] (a lease: an expired one lets it fire again). [finishedAt] (added by the S1
 * plan: a TTL index needs a date) is set when the deadline is FIRED or CANCELLED. [correlationId] (ruling 37) is the
 * correlation id of the message that scheduled it: the deadline handler's `MessageContext.correlationId` and the commands
 * it sends carry it, so a trace follows a saga through its timeouts.
 */
@Document("deadlines")
data class DeadlineDocument(
    @Id val id: String,
    val sagaType: String,
    val sagaId: String,
    val name: String,
    val payload: String? = null,
    val payloadType: String? = null,
    val dueAt: Instant,
    val correlationId: String,
    val status: DeadlineStatus = DeadlineStatus.SCHEDULED,
    val claimedBy: String? = null,
    val claimedUntil: Instant? = null,
    val finishedAt: Instant? = null,
)

/**
 * A saga event that failed all its attempts and was parked in `saga_failures` (`_id` = `<sagaType>:<eventId>`): the event
 * as received ([eventType], [payload], [headers], enough to rebuild its `MessageContext`), the last [error], how many
 * [attempts] were made. `SagaFailures` re-delivers or discards it.
 */
@Document("saga_failures")
data class SagaFailureDocument(
    @Id val id: String,
    val sagaType: String,
    val eventId: String,
    val eventType: String,
    val payload: String,
    val headers: Map<String, String> = emptyMap(),
    val error: String,
    val attempts: Int,
    val firstFailedAt: Instant,
    val lastFailedAt: Instant,
)
