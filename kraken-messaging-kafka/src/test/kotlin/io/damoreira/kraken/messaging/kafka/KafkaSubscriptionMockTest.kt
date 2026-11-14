package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger

/** The subscription over a `MockConsumer`: the rebalance and seek paths, deterministic and without a broker. */
class KafkaSubscriptionMockTest {
    private val topic = "mock.events"
    private val tp0 = TopicPartition(topic, 0)
    private val tp1 = TopicPartition(topic, 1)
    private val spec = SubscriptionSpec("svc.default", topic)
    private val settings = KafkaEventStoreSettings("unused:1", statusRefresh = Duration.ofMillis(50))

    /** A consumer whose first [timeouts] reads of the committed offsets time out; the group is at offset 3; commits are recorded. */
    private class Mock(timeouts: Int) : MockConsumer<String, ByteArray>("earliest") {
        val failures = AtomicInteger(timeouts)
        val commits = CopyOnWriteArrayList<Map<TopicPartition, Long>>()

        override fun committed(partitions: Set<TopicPartition>, timeout: Duration): Map<TopicPartition, OffsetAndMetadata> {
            if (failures.getAndDecrement() > 0) throw TimeoutException("the broker did not answer")
            return partitions.associateWith { OffsetAndMetadata(3) }
        }

        override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration) {
            commits += offsets.mapValues { it.value.offset() }
            super.commitSync(offsets, timeout)
        }
    }

    private fun record(tp: TopicPartition, offset: Long) = ConsumerRecord(tp.topic(), tp.partition(), offset, "k$offset", """{"n":$offset}""".toByteArray())

    /** Joins both partitions and adds [offsets] records to partition 0 at the next poll. */
    private fun assignWithRecords(mock: Mock, offsets: LongRange) {
        mock.schedulePollTask {
            mock.updateBeginningOffsets(mapOf(tp0 to 0L, tp1 to 0L))
            mock.updateEndOffsets(mapOf(tp0 to 10L, tp1 to 10L))
            mock.rebalance(listOf(tp0, tp1))
            mock.seek(tp0, 0)
            mock.seek(tp1, 0)
            offsets.forEach { mock.addRecord(record(tp0, it)) }
        }
    }

    private suspend fun until(what: String, done: () -> Boolean) {
        withTimeout(10_000) { while (!done()) kotlinx.coroutines.delay(20) }
    }

    @Test
    fun `a broker timeout while the committed offsets are read does not fail the subscription, which assigns and delivers, then learns the offset`() = runBlocking {
        val mock = Mock(timeouts = 1)
        assignWithRecords(mock, 0L..2L)
        val subscription = KafkaSubscription.start(spec, settings, mock)

        val first = withTimeout(10_000) { subscription.next() }
        assertEquals(SubscriptionEvent.Assigned(listOf("0", "1")), first)
        val delivered = (0 until 3).map { withTimeout(10_000) { subscription.next() } as Delivery }
        assertEquals(listOf("0", "1", "2"), delivered.map { it.position })

        // The committed offset is not known yet: an acknowledgement waits rather than commit below what the group has.
        subscription.ack("0", "0")
        until("the committed offset learned") { subscription.segments().first().committed == "3" }
        assertEquals(listOf("0", "1"), subscription.segments().map { it.segment })
        assertTrue(mock.commits.isEmpty(), "committed nothing below the group's offset: ${mock.commits}")
        subscription.ack("0", "5")
        until("the commit") { mock.commits.isNotEmpty() }
        assertEquals(mapOf(tp0 to 6L), mock.commits.first())
        subscription.close()
    }

    @Test
    fun `the first position is the first queued record, not the consumer's position past them`() = runBlocking {
        val mock = Mock(timeouts = 0)
        assignWithRecords(mock, 0L..4L)
        val subscription = KafkaSubscription.start(spec, settings, mock)
        assertEquals(SubscriptionEvent.Assigned(listOf("0", "1")), withTimeout(10_000) { subscription.next() })
        until("the first status refresh") { subscription.segments().first { it.segment == "0" }.position != null }
        assertEquals("0", subscription.segments().first { it.segment == "0" }.position)
        subscription.close()
    }

    @Test
    fun `redeliver seeks the consumer back and drops what was queued for the segment`() = runBlocking {
        val mock = Mock(timeouts = 0)
        assignWithRecords(mock, 0L..4L)
        val subscription = KafkaSubscription.start(spec, settings, mock)
        assertEquals(SubscriptionEvent.Assigned(listOf("0", "1")), withTimeout(10_000) { subscription.next() })
        repeat(2) { withTimeout(10_000) { subscription.next() } }

        subscription.redeliver("0", "1")
        until("the seek applied") { mock.position(tp0) == 1L }
        assertNull(withTimeoutOrNull(300) { subscription.next() }, "the records queued behind the redelivered one are dropped")
        subscription.close()
    }
}
