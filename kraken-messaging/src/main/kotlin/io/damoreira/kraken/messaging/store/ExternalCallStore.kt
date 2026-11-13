package io.damoreira.kraken.messaging.store

import io.damoreira.kraken.messaging.external.ExternalCallStatus
import java.time.Instant

/**
 * The record of one idempotent external call (`@IdempotentHandler`, `external_calls`), keyed by its idempotency key [id].
 * [reply] is the JSON of a `SUCCEEDED` call's reply (any JSON value), [error] the JSON of a `FAILED` call's error
 * (`{"code", "message"}`). An `IN_PROGRESS` record is owned by the call that wrote the random [owner] token, until
 * [leaseUntil]; both are null once the call is settled. [updatedAt] starts the retention.
 */
class ExternalCallRecord(
    val id: String,
    val status: ExternalCallStatus,
    val reply: ByteArray? = null,
    val error: ByteArray? = null,
    val attempts: Int = 0,
    val updatedAt: Instant,
    val leaseUntil: Instant? = null,
    val owner: String? = null,
) {
    fun copy(
        id: String = this.id, status: ExternalCallStatus = this.status, reply: ByteArray? = this.reply, error: ByteArray? = this.error,
        attempts: Int = this.attempts, updatedAt: Instant = this.updatedAt, leaseUntil: Instant? = this.leaseUntil, owner: String? = this.owner,
    ) = ExternalCallRecord(id, status, reply, error, attempts, updatedAt, leaseUntil, owner)

    override fun equals(other: Any?): Boolean = other is ExternalCallRecord && id == other.id && status == other.status &&
        same(reply, other.reply) && same(error, other.error) && attempts == other.attempts && updatedAt == other.updatedAt &&
        leaseUntil == other.leaseUntil && owner == other.owner

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String =
        "ExternalCallRecord(id=$id, status=$status, reply=${reply?.size} bytes, error=${error?.size} bytes, attempts=$attempts, " +
            "updatedAt=$updatedAt, leaseUntil=$leaseUntil, owner=$owner)"

    private fun same(a: ByteArray?, b: ByteArray?) = if (a == null) b == null else b != null && a.contentEquals(b)
}

/**
 * The external calls (`external_calls`): what makes an idempotent handler run its body once per key. Every write commits
 * on its own (the core runs each in a short transaction of its own, apart from the caller's), and every write after
 * [insert] is conditional, so a call whose lease ran out never overwrites the record of the call that took over.
 *
 * The *owned* writes ([succeed], [fail], [markUnknown], [reset]) match `{id, status IN_PROGRESS, owner}`; the writes on a
 * record read earlier ([takeOver], [giveUp]) match `{id, status IN_PROGRESS, owner, leaseUntil}` as read. Each returns
 * whether it matched; a write that matched nothing changed nothing.
 */
interface ExternalCallStore {
    /**
     * Inserts [call]; false when a record with its id exists. Inside a transaction the caller then rolls it back (a database
     * may have aborted it on the duplicate), as after [ProcessedEventStore.record].
     */
    suspend fun insert(call: ExternalCallRecord): Boolean

    suspend fun find(id: String): ExternalCallRecord?

    /**
     * Takes over the record [id] read as owned by [expectedOwner] until [expectedLeaseUntil]: [owner] and [leaseUntil] are
     * the new ones, `attempts + 1`, `updatedAt` [now]. The updated record, or null when it changed since it was read.
     */
    suspend fun takeOver(id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, owner: String, leaseUntil: Instant, now: Instant): ExternalCallRecord?

    /** Gives the record [id] read as owned by [expectedOwner] until [expectedLeaseUntil] up as `UNKNOWN` (no owner, no lease). */
    suspend fun giveUp(id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, now: Instant): Boolean

    /** [owner]'s call returned [reply]: `SUCCEEDED` with it, no owner, no lease, no error. */
    suspend fun succeed(id: String, owner: String, reply: ByteArray, now: Instant): Boolean

    /** [owner]'s call failed for good: `FAILED` with [error], no owner, no lease. */
    suspend fun fail(id: String, owner: String, error: ByteArray, now: Instant): Boolean

    /** [owner]'s call has an unknown outcome: `UNKNOWN`, no owner, no lease. */
    suspend fun markUnknown(id: String, owner: String, now: Instant): Boolean

    /** [owner]'s call failed in a retryable way: the record is deleted, so the next call of the key starts afresh. */
    suspend fun reset(id: String, owner: String): Boolean
}
