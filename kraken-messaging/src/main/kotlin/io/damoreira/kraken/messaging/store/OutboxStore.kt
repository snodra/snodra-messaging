package io.damoreira.kraken.messaging.store

import java.time.Duration
import java.time.Instant

/**
 * One event in the outbox (ADR 0005): written in the service's transaction, relayed to the event store afterwards.
 * [payload] is the event as a JSON object (UTF-8): a store rejects any other JSON value, or bytes that are not JSON, with an
 * [IllegalArgumentException] and appends nothing. [createdAt] and [seq] order the outbox ([seq] breaks ties of one millisecond within one
 * publishing instance); [attempts] counts the relay's failed sends.
 */
class OutboxEntry(
    val id: String,
    val type: String,
    val key: String?,
    val headers: Map<String, String>,
    val payload: ByteArray,
    val createdAt: Instant,
    val seq: Long,
    val attempts: Int = 0,
) {
    override fun equals(other: Any?): Boolean = other is OutboxEntry && id == other.id && type == other.type && key == other.key &&
        headers == other.headers && payload.contentEquals(other.payload) && createdAt == other.createdAt && seq == other.seq &&
        attempts == other.attempts

    override fun hashCode(): Int = id.hashCode()

    override fun toString(): String =
        "OutboxEntry(id=$id, type=$type, key=$key, headers=$headers, payload=${payload.size} bytes, createdAt=$createdAt, seq=$seq, attempts=$attempts)"
}

/**
 * The outbox (`outbox`; `kraken_outbox` in SQL). Entries are appended in the caller's transaction and relayed by any number
 * of instances at once: a relay [claim]s a batch under a lease, sends it, then marks each entry sent or failed. While a lease
 * holds no other relay gets the entry; once it passed (the claimer crashed) the next claim takes it over.
 */
interface OutboxStore {
    /** Appends [entry], joining the transaction active on the calling thread (see [MessageStore]). */
    suspend fun append(entry: OutboxEntry)

    /**
     * Up to [limit] entries not sent and not failed, in `(createdAt, seq)` order, that are free, already [owner]'s, or whose
     * lease passed; each is [owner]'s until [lease] from now. Two concurrent claims never return the same entry while its
     * lease holds. `(createdAt, seq)` is the only order: an entry [owner] still holds comes back in its place, so an entry whose
     * send failed is retried before the later ones (order per key).
     */
    suspend fun claim(owner: String, lease: Duration, limit: Int): List<OutboxEntry>

    /** Marks [ids] sent (now): never claimed again, purged after the retention. */
    suspend fun markSent(ids: Collection<String>)

    /**
     * A send of [id] failed and will be retried: `attempts + 1`; the entry stays its claimer's until the lease passes.
     * [error] is for diagnosis; a store may keep it or not.
     */
    suspend fun markAttemptFailed(id: String, error: String)

    /**
     * [id] is given up on (a failure that retrying cannot fix, or the last attempt): `attempts + 1`, and it is never claimed
     * again; it stays in the outbox for inspection. [error] is for diagnosis; a store may keep it or not.
     */
    suspend fun markFailed(id: String, error: String)

    /** How many entries are neither sent nor failed (the `outbox.pending` gauge). */
    suspend fun pending(): Long

    /** Deletes the entries sent before [sentBefore]; returns how many. Unsent and failed entries are never purged. */
    suspend fun purge(sentBefore: Instant): Long
}
