package io.damoreira.kraken.messaging.store

import io.damoreira.kraken.messaging.eventstore.ReplayRange
import java.time.Duration
import java.time.Instant

/**
 * The processors' control documents (`processor_control`, P6b, ADR 0023): one per event processor, keyed by its name, the
 * source of truth of its pause, of the reset operation holding its lock and of its open replay ranges. Every instance
 * reads it at start and on every poll. A processor without a document is not paused, has no operation and no replay.
 * Every write commits on its own, outside any transaction of the caller.
 */
interface ProcessorControlStore {
    /** The reset operation holding a processor's lock until [leaseUntil]. */
    data class Operation(val id: String, val kind: String, val leaseUntil: Instant, val preparedAt: Instant) {
        fun expired(now: Instant): Boolean = !leaseUntil.isAfter(now)
    }

    /** One processor's document. */
    data class Control(
        val processor: String,
        val paused: Boolean = false,
        val pausedAt: Instant? = null,
        val pausedBy: String? = null,
        val operation: Operation? = null,
        val replay: List<ReplayRange> = emptyList(),
        val updatedAt: Instant? = null,
    )

    /** What [lock] did. */
    sealed interface Lock {
        /** The lock is the operation's now (it was free, expired, or already the operation's). */
        data object Taken : Lock

        /** Another operation holds it. */
        data class Held(val by: Operation?) : Lock
    }

    /** The documents of [processors] that exist, by name. */
    suspend fun read(processors: Collection<String>): Map<String, Control>

    /** The document of [processor], or null. */
    suspend fun get(processor: String): Control?

    /** Pauses or resumes [processor] ([by]: the control id, or `local`); idempotent. */
    suspend fun setPaused(processor: String, paused: Boolean, by: String)

    /**
     * Takes [processor]'s lock for [op] (kind [KIND_RESET]) until [lease] from now, when no operation holds it or its lease
     * passed; a lock [op] already holds is renewed. Atomic: two operations never both get [Lock.Taken] while a lease holds.
     */
    suspend fun lock(processor: String, op: String, lease: Duration): Lock

    /** Moves the lease of [op] to [lease] from now; false when [op] does not hold [processor]'s lock. */
    suspend fun renew(processor: String, op: String, lease: Duration): Boolean

    /** Clears [op]'s lock ([expiredOnly]: only when its lease passed); false when [op] does not hold it (then nothing changes). */
    suspend fun release(processor: String, op: String, expiredOnly: Boolean = false): Boolean

    /**
     * Replaces the open replay ranges of [processor] (it exists: its lock is held). A store that keeps a range's segment and
     * positions as numbers (the stored layout before neutral positions) refuses one whose segment or positions are not decimal
     * with an [IllegalArgumentException].
     */
    suspend fun setReplay(processor: String, ranges: List<ReplayRange>)

    /** Removes the open range [range] (exactly that item) of [processor]; false when it was not there any more. */
    suspend fun removeReplay(processor: String, range: ReplayRange): Boolean

    /**
     * Deletes [processor]'s stage-1 markers ([ProcessedMarker] with that processor) in [ranges]: topic = the range's source,
     * partition = its segment, offset in `[from, until)`. Returns how many were deleted. A store that keeps the marker's partition
     * and offset as numbers refuses a range whose segment or positions are not decimal with an [IllegalArgumentException].
     */
    suspend fun deleteMarkers(processor: String, ranges: List<ReplayRange>): Long

    companion object {
        /** `operation.kind` of a reset, the only kind so far. */
        const val KIND_RESET = "reset"
    }
}
