package com.damoreira.snodra.messaging.rabbitmq

import com.damoreira.snodra.messaging.eventstore.EventStoreException
import com.damoreira.snodra.messaging.eventstore.ReplayRange
import com.damoreira.snodra.messaging.eventstore.ResetTarget
import com.damoreira.snodra.messaging.eventstore.SegmentOrder
import com.damoreira.snodra.messaging.eventstore.SubscriptionAdmin
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withTimeout
import java.time.Duration

/**
 * The reset side of a subscription (capability `replay`): the offsets stored under its name, moved while no consumer of that
 * name reads the source. Who reads it comes from the management API (the stream protocol lists no consumers) plus this process's
 * own open subscriptions; the API failing to answer, or answering what cannot be trusted, fails the call, never "nobody reads".
 * Each call opens connections of its own and closes them.
 */
internal class RabbitSubscriptionAdmin(
    private val spec: SubscriptionSpec,
    private val connector: RabbitConnector,
    private val management: RabbitManagement,
    private val live: RabbitLive,
) : SubscriptionAdmin {
    /** One partition with its offsets as they are now. */
    private class Now(val part: Part, val stored: Long?, val bounds: Bounds)

    override suspend fun members(): List<String> = call { reads -> membersOf(reads) }

    private fun membersOf(reads: RabbitReads): List<String> {
        val streams = layout(reads).map { it.stream }
        return (management.members(spec.name, streams) + live.members(spec)).toSortedSet().toList()
    }

    override suspend fun plan(target: ResetTarget): List<ReplayRange> = call { reads ->
        val now = now(reads)
        fun range(n: Now, from: Long) = ReplayRange(spec.source, n.part.segment, from.toString(), n.stored?.toString().orEmpty())
        when (target) {
            ResetTarget.Earliest -> now.map { range(it, it.bounds.first) }
            ResetTarget.Latest -> now.map { range(it, it.bounds.head) }
            is ResetTarget.Timestamp -> now.map { range(it, reads.offsetAt(it.part.stream, target.timestampMs, it.bounds, READ_WAIT)) }
            is ResetTarget.Positions -> {
                if (target.positions.isEmpty()) throw EventStoreException.Invalid("the target lists no segment")
                target.positions.map { (segment, position) ->
                    val n = now.firstOrNull { it.part.segment == segment } ?: throw EventStoreException.Invalid("${spec.source} has no segment $segment")
                    range(n, checked(n, position))
                }.sortedWith(compareBy(SegmentOrder) { it.segment })
            }
        }
    }

    override suspend fun apply(ranges: List<ReplayRange>): Unit = call { reads ->
        if (ranges.isEmpty()) return@call
        val members = membersOf(reads)
        if (members.isNotEmpty()) throw EventStoreException.SubscribersActive(members.joinToString())
        val now = now(reads)
        val moves = LinkedHashMap<String, Long>()
        for (r in ranges) {
            if (r.source != spec.source) throw EventStoreException.Invalid("segment ${r.segment} is of ${r.source}, not of ${spec.source}")
            val n = now.firstOrNull { it.part.segment == r.segment } ?: throw EventStoreException.Invalid("${spec.source} has no segment ${r.segment}")
            if (moves.put(n.part.stream, checked(n, r.from)) != null) throw EventStoreException.Invalid("segment ${r.segment} is moved twice")
        }
        for ((stream, from) in moves) reads.storeAndConfirm(null, spec.name, stream, from, STORE_WAIT)
        // Read back once all are stored: an instance that joined meanwhile and stored its own position undoes the reset, which
        // then fails.
        for ((stream, from) in moves) {
            val stored = reads.storedOffset(spec.name, stream)
            if (stored != from) {
                throw EventStoreException.SubscribersActive(
                    "the offset of ${spec.name} on $stream is $stored after the reset stored $from: an instance read and stored meanwhile",
                )
            }
        }
    }

    private fun layout(reads: RabbitReads): List<Part> =
        (reads.layout(spec.source) ?: throw EventStoreException.Invalid("the source ${spec.source} does not exist"))
            .sortedWith(compareBy(SegmentOrder) { it.segment })

    private fun now(reads: RabbitReads): List<Now> = layout(reads).map { part ->
        val stored = reads.storedOffset(spec.name, part.stream)
        val bounds = reads.bounds(part.stream)
        if (stored != null && stored > bounds.head) {
            // The stored offset past the end read: one of the two is wrong, so nothing is planned from them.
            throw EventStoreException.Failed("rabbitmq: the offset stored for ${spec.name} on ${part.stream} ($stored) is past its end (${bounds.head})")
        }
        Now(part, stored, bounds)
    }

    /** [position] of [n]'s segment as an offset the segment has (not past its end). */
    private fun checked(n: Now, position: String): Long {
        val offset = position.toLongOrNull()?.takeIf { it >= 0 }
            ?: throw EventStoreException.Invalid("position $position of segment ${n.part.segment} is not an offset")
        if (offset > n.bounds.head) throw EventStoreException.Invalid("position $offset of segment ${n.part.segment} is past its head ${n.bounds.head}")
        return offset
    }

    /** Runs [block] off the caller's thread with connections of its own (to each stream's leader), closed after, within [CALL_BUDGET]. */
    private suspend fun <T> call(block: (RabbitReads) -> T): T = try {
        withTimeout(CALL_BUDGET.toMillis()) {
            runInterruptible(Dispatchers.IO) { RabbitReads(connector).use(block) }
        }
    } catch (e: TimeoutCancellationException) {
        throw EventStoreException.Failed("rabbitmq: the reset side of ${spec.name} took over $CALL_BUDGET")
    }

    private companion object {
        /** The longest one admin call may take. */
        val CALL_BUDGET: Duration = Duration.ofSeconds(20)

        /** How long one chunk of a read by time may take to come. */
        val READ_WAIT: Duration = Duration.ofSeconds(5)

        /** How long a moved offset may take to be read back. */
        val STORE_WAIT: Duration = Duration.ofSeconds(5)
    }
}

/**
 * The subscriptions of this process that are open now, per (source, name), by member id (the `snodra-member` property): a reset
 * must not move a subscription this very process reads, whatever the management API says.
 */
internal class RabbitLive {
    private val open = HashMap<Pair<String, String>, MutableSet<String>>()

    @Synchronized
    fun enter(spec: SubscriptionSpec, member: String): () -> Unit {
        open.getOrPut(spec.source to spec.name) { HashSet() } += member
        return { leave(spec, member) }
    }

    @Synchronized
    private fun leave(spec: SubscriptionSpec, member: String) {
        val key = spec.source to spec.name
        open[key]?.let { members ->
            members -= member
            if (members.isEmpty()) open.remove(key)
        }
    }

    @Synchronized
    fun members(spec: SubscriptionSpec): Set<String> = open[spec.source to spec.name]?.toSet().orEmpty()
}
