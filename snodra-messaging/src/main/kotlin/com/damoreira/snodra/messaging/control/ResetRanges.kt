package com.damoreira.snodra.messaging.control

import com.damoreira.snodra.messaging.eventstore.ReplayRange
import com.damoreira.snodra.messaging.eventstore.ResetTarget
import com.damoreira.snodra.messaging.eventstore.SegmentOrder

/**
 * The replay ranges of a reset (ADR 0023), from what the event store plans ([com.damoreira.snodra.messaging.eventstore.SubscriptionAdmin.plan]);
 * positions are ordered by the event store's `compare` only.
 */
internal object ResetRanges {
    /**
     * The replay ranges of a reset whose plan is [moves] (one per segment it moves) over [open] (the ranges still open): only the
     * moves that replay, `[from, until)` with `until` set and `from` before it, never for `latest`; a segment that still has an
     * open range gets `[from, max(until, open until))` (that range's records past `until` were not replayed yet and their
     * markers are gone). In segment order.
     */
    fun replaying(moves: List<ReplayRange>, target: ResetTarget, open: List<ReplayRange>, compare: Comparator<String>): List<ReplayRange> {
        if (target == ResetTarget.Latest) return emptyList()
        return moves.filter { it.until.isNotEmpty() && compare.compare(it.from, it.until) < 0 }.map { move ->
            val until = open.filter { it.source == move.source && it.segment == move.segment }.map { it.until }
                .fold(move.until) { a, b -> if (compare.compare(a, b) < 0) b else a }
            move.copy(until = until)
        }.sortedWith(compareBy(SegmentOrder) { it.segment })
    }

    /**
     * The replay ranges after the reset: [open] without the ranges of the segments that got a new one, then the [new] ones;
     * sorted by source, segment (in segment order) and `from`.
     */
    fun merged(open: List<ReplayRange>, new: List<ReplayRange>, compare: Comparator<String>): List<ReplayRange> =
        (open.filter { o -> new.none { it.source == o.source && it.segment == o.segment } } + new).sortedWith(
            compareBy<ReplayRange> { it.source }.thenBy(SegmentOrder) { it.segment }.thenComparator { a, b -> compare.compare(a.from, b.from) },
        )
}
