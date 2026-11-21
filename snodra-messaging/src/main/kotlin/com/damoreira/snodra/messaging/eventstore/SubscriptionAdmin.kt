package com.damoreira.snodra.messaging.eventstore

/** Where a reset moves a subscription (capability [Capability.REPLAY]). */
sealed interface ResetTarget {
    /** Every segment's first position: everything the event store still has is a replay. */
    data object Earliest : ResetTarget

    /** Every segment's head: nothing is replayed. */
    data object Latest : ResetTarget

    /** The first position at or after [timestampMs] (epoch milliseconds, the event store's append time) per segment, else its head. */
    data class Timestamp(val timestampMs: Long) : ResetTarget

    /** These segments to these positions (segment to position); a segment not listed keeps its committed position. */
    data class Positions(val positions: Map<String, String>) : ResetTarget
}

/**
 * Events of [segment] of [source] at a position `from <= position < until` are a replay: handled again without sagas or
 * `@DisallowReplay` handlers.
 *
 * What [SubscriptionAdmin.plan] gives is one per segment a reset moves: [from] is the position it moves to, [until] the
 * subscription's committed position before (empty when it had none). Only a range with [until] set and [from] before it (by
 * [EventStore.compare]) replays anything; the others are moves without a replay (`latest`, a segment never committed). The
 * processor control store keeps the open ones.
 *
 * [segment], [from] and [until] are opaque strings of at most [MAX_SEGMENT_BYTES] and [MAX_POSITION_BYTES] bytes that only the
 * event store reads; [EventStore.compare] is the only way the core orders two positions of one segment. [source] is the source
 * of [segment].
 */
data class ReplayRange(val source: String, val segment: String, val from: String, val until: String)

/** The reset side of one subscription ([EventStore.admin]). */
interface SubscriptionAdmin {
    /** The members reading the subscription now (the event store's ids for them); empty when none does. */
    suspend fun members(): List<String>

    /**
     * Where [target] moves each segment, from the event store's current positions; nothing moves yet. In segment order ([SegmentOrder]). A
     * target position the segment does not have (a segment the source lacks, a position past the head) throws
     * [EventStoreException.Invalid].
     */
    suspend fun plan(target: ResetTarget): List<ReplayRange>

    /**
     * Moves the committed position of each range's segment to its [ReplayRange.from] ([ReplayRange.until] is not read), all or
     * nothing; refused with [EventStoreException.SubscribersActive] while an instance reads the subscription.
     */
    suspend fun apply(ranges: List<ReplayRange>)
}
