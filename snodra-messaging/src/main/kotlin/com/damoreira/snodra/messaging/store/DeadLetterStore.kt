package com.damoreira.snodra.messaging.store

import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.deadletter.DeadLetterError
import com.damoreira.snodra.messaging.deadletter.DeadLetterReason
import java.time.Instant

/**
 * The dead letters (`dead_letters`): records that exhausted their attempts, sequenced per key. A letter's id is
 * `<processor>:<source>:<segment>:<position>` (on Kafka `<processor>:<topic>:<partition>:<offset>`, as always). The **blocking**
 * letters of a key are those whose reason is not INVALID; the key's head is the blocking letter at the earliest position (ties
 * by id), and later records of the key wait behind it. The letters of one key come from one segment; positions are ordered as
 * the store orders them, decimal ones (every first-wave event store's) by value.
 *
 * Every write commits on its own, outside any transaction of the caller, except [deleteInTransaction]. A letter expires at
 * its [DeadLetter.expireAt] (which also unblocks its key).
 */
interface DeadLetterStore {
    /** What `retryAll` needs of a letter. */
    data class Summary(val id: String, val key: String?, val position: String, val reason: DeadLetterReason, val parkedAt: Instant)

    /** Stores [letter] as it is; true when parked now, false when a letter with its id was parked already. */
    suspend fun park(letter: DeadLetter): Boolean

    /** The id of the head letter of ([processor], [key]): the earliest position that is not INVALID (ties by id), or null. */
    suspend fun head(processor: String, key: String): String?

    /** The id of the head among the blocking letters of ([processor], [key]) at a position before [position], or null. */
    suspend fun earlier(processor: String, key: String, position: String): String?

    /** Whether a letter with [id] exists. */
    suspend fun exists(id: String): Boolean

    /** The keys of [processor]'s blocking letters with a key on [segments] of [source], by segment (none: no entry). */
    suspend fun keys(processor: String, source: String, segments: Collection<String>): Map<String, Set<String>>

    /** How many letters [processor] has (every processor's when null). */
    suspend fun count(processor: String?): Long

    /** Up to [limit] letters of [processor] (every processor's when null), by `parkedAt`, then id. */
    suspend fun list(processor: String?, limit: Int): List<DeadLetter>

    /** Every letter of [processor], summarised, for `retryAll`: by position as [head] orders them, then id. */
    suspend fun summaries(processor: String): List<Summary>

    suspend fun get(id: String): DeadLetter?

    /** Deletes the letter outside any transaction; true when it existed. */
    suspend fun delete(id: String): Boolean

    /** Deletes the letter joining the transaction active on the calling thread (a retry's); true when it existed. */
    suspend fun deleteInTransaction(id: String): Boolean

    /**
     * A retry that failed again: `attempts + 1`, [error], `lastAttemptAt` now, reason HANDLER_FAILED, no `blockedBy`; false
     * when the letter is gone.
     */
    suspend fun recordFailure(id: String, error: DeadLetterError): Boolean

    companion object {
        /** A letter's id: `<processor>:<source>:<segment>:<position>` (on Kafka `<processor>:<topic>:<partition>:<offset>`). */
        fun id(processor: String, source: String, segment: String, position: String) = "$processor:$source:$segment:$position"
    }
}
