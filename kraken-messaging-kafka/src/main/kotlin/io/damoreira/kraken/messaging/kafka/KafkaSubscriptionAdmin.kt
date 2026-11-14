package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.eventstore.SubscriptionAdmin
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.GroupIdNotFoundException
import org.apache.kafka.common.errors.GroupNotEmptyException
import org.apache.kafka.common.errors.RebalanceInProgressException
import org.apache.kafka.common.errors.UnknownMemberIdException
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import java.util.concurrent.ExecutionException
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException

/**
 * The reset side of a consumer group (capability `replay`): a plan from the topic's earliest, latest and per-time offsets and
 * the group's committed ones, applied by moving the group's offsets, which Kafka allows only while the group has no member.
 * One short-lived `Admin` client per call.
 */
internal class KafkaSubscriptionAdmin(
    private val spec: SubscriptionSpec,
    private val settings: KafkaEventStoreSettings,
) : SubscriptionAdmin {
    private val topic = spec.source
    private val group = spec.name
    private val timeout = settings.adminTimeout.toMillis()

    override suspend fun members(): List<String> = admin { a -> membersOf(a) }

    override suspend fun plan(target: ResetTarget): List<ReplayRange> = admin { a ->
        val partitions = partitionsOf(a)
        fun offsets(spec: OffsetSpec): Map<TopicPartition, Long> =
            a.listOffsets(partitions.associateWith { spec }).all().get(timeout, TimeUnit.MILLISECONDS).mapValues { it.value.offset() }

        val targets: Map<TopicPartition, Long> = when (target) {
            ResetTarget.Earliest -> offsets(OffsetSpec.earliest())
            ResetTarget.Latest -> offsets(OffsetSpec.latest())
            is ResetTarget.Timestamp -> {
                val high = offsets(OffsetSpec.latest())
                val found = offsets(OffsetSpec.forTimestamp(target.timestampMs))
                partitions.associateWith { tp -> found[tp]?.takeIf { it >= 0 } ?: high.getValue(tp) } // no record that late: the end
            }
            is ResetTarget.Positions -> {
                if (target.positions.isEmpty()) throw EventStoreException.Invalid("the target lists no segment")
                val high = offsets(OffsetSpec.latest())
                val known = partitions.associateBy { it.partition() }
                target.positions.entries.associate { (segment, position) ->
                    val tp = segment.toIntOrNull()?.let(known::get) ?: throw EventStoreException.Invalid("$topic has no segment $segment")
                    val offset = position.toLongOrNull() ?: throw EventStoreException.Invalid("position $position of segment $segment is not an offset")
                    if (offset < 0) throw EventStoreException.Invalid("position $offset of segment $segment is negative")
                    if (offset > high.getValue(tp)) throw EventStoreException.Invalid("position $offset of segment $segment is past its head ${high.getValue(tp)}")
                    tp to offset
                }
            }
        }
        val before = committedOf(a)
        targets.entries.sortedBy { it.key.partition().toString() }.map { (tp, to) ->
            ReplayRange(topic, tp.partition().toString(), to.toString(), before[tp]?.offset()?.toString().orEmpty())
        }
    }

    override suspend fun apply(ranges: List<ReplayRange>): Unit = admin { a ->
        val partitions = partitionsOf(a).associateBy { it.partition() }
        val moves = LinkedHashMap<TopicPartition, OffsetAndMetadata>()
        for (range in ranges) {
            if (range.source != topic) throw EventStoreException.Invalid("segment ${range.segment} is of ${range.source}, not of $topic")
            val tp = range.segment.toIntOrNull()?.let(partitions::get) ?: throw EventStoreException.Invalid("$topic has no segment ${range.segment}")
            val offset = range.from.toLongOrNull()?.takeIf { it >= 0 } ?: throw EventStoreException.Invalid("position ${range.from} of segment ${range.segment} is not an offset")
            if (moves.put(tp, OffsetAndMetadata(offset)) != null) throw EventStoreException.Invalid("segment ${range.segment} is moved twice")
        }
        val members = membersOf(a)
        if (members.isNotEmpty()) throw EventStoreException.SubscribersActive(members.joinToString())
        try {
            a.alterConsumerGroupOffsets(group, moves).all().get(timeout, TimeUnit.MILLISECONDS)
            Unit
        } catch (e: ExecutionException) {
            val cause = e.cause
            if (cause is UnknownMemberIdException || cause is GroupNotEmptyException || cause is RebalanceInProgressException) {
                throw EventStoreException.SubscribersActive("Kafka refused to move the offsets of group $group: ${cause.message}")
            }
            throw e
        }
    }

    private fun membersOf(a: Admin): List<String> = orNoGroup(emptyList()) {
        a.describeConsumerGroups(listOf(group)).all().get(timeout, TimeUnit.MILLISECONDS)[group]?.members().orEmpty().map { it.clientId() }
    }

    private fun committedOf(a: Admin): Map<TopicPartition, OffsetAndMetadata?> = orNoGroup(emptyMap()) {
        a.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata().get(timeout, TimeUnit.MILLISECONDS)
    }

    private fun partitionsOf(a: Admin): List<TopicPartition> {
        val described = try {
            a.describeTopics(listOf(topic)).allTopicNames().get(timeout, TimeUnit.MILLISECONDS)
        } catch (e: ExecutionException) {
            if (e.cause is UnknownTopicOrPartitionException) throw EventStoreException.Invalid("$topic does not exist")
            throw e
        }
        return described.getValue(topic).partitions().map { TopicPartition(topic, it.partition()) }.sortedBy { it.partition() }
    }

    /** A group that never existed has no offsets and no members. */
    private fun <T> orNoGroup(none: T, call: () -> T): T = try {
        call()
    } catch (e: ExecutionException) {
        if (e.cause is GroupIdNotFoundException) none else throw e
    }

    /** One call's `Admin` client, run off the caller's thread; the client's failures are the event store's. */
    private suspend fun <T> admin(call: (Admin) -> T): T = withContext(Dispatchers.IO) {
        try {
            Admin.create(settings.adminProperties()).use(call)
        } catch (e: EventStoreException) {
            throw e
        } catch (e: ExecutionException) {
            throw EventStoreException.Failed("the reset side of $group: ${e.cause?.message ?: e.message}", e)
        } catch (e: TimeoutException) {
            throw EventStoreException.Failed("the reset side of $group did not answer within ${timeout} ms", e)
        } catch (e: KafkaException) {
            throw EventStoreException.Failed("the reset side of $group: ${e.message}", e)
        }
    }
}
