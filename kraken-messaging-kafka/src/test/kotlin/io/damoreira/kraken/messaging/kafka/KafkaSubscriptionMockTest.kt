package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
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

        /** While set, every read of the committed offsets times out. */
        val lookupFails = AtomicBoolean(false)
        val commits = CopyOnWriteArrayList<Map<TopicPartition, Long>>()

        override fun committed(partitions: Set<TopicPartition>, timeout: Duration): Map<TopicPartition, OffsetAndMetadata> {
            if (lookupFails.get() || failures.getAndDecrement() > 0) throw TimeoutException("the broker did not answer")
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

        until("the committed offset learned") { subscription.segments().first().committed == "3" }
        assertEquals(listOf("0", "1"), subscription.segments().map { it.segment })
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

        // The broker gives the segment again from the seek on: that position and the later ones, each once.
        mock.schedulePollTask { (1L..4L).forEach { mock.addRecord(record(tp0, it)) } }
        val again = (0 until 4).map { (withTimeout(10_000) { subscription.next() } as Delivery).position }
        assertEquals(listOf("1", "2", "3", "4"), again)
        assertNull(withTimeoutOrNull(300) { subscription.next() }, "no stale duplicate behind them")
        subscription.close()
    }

    @Test
    fun `an ack is committed while the committed offsets cannot be read`() = runBlocking {
        val mock = Mock(timeouts = 0)
        mock.lookupFails.set(true)
        assignWithRecords(mock, 0L..2L)
        val subscription = KafkaSubscription.start(spec, settings, mock)
        assertEquals(SubscriptionEvent.Assigned(listOf("0", "1")), withTimeout(10_000) { subscription.next() })
        repeat(3) { withTimeout(10_000) { subscription.next() } }

        subscription.ack("0", "1")
        until("the commit") { mock.commits.isNotEmpty() }
        assertEquals(mapOf(tp0 to 2L), mock.commits.first())
        until("the committed offset reported") { subscription.segments().first { it.segment == "0" }.committed != null }
        assertEquals("2", subscription.segments().first { it.segment == "0" }.committed, "the offset it committed is the one it reports")
        subscription.close()
    }

    @Test
    fun `close flushes a pending ack, and so does a revoke, while the committed offsets cannot be read`() = runBlocking {
        // The poll thread is held inside a poll, so the ack is still pending when the close (or the revoke) comes.
        val mock = Mock(timeouts = 0)
        mock.lookupFails.set(true)
        assignWithRecords(mock, 0L..2L)
        val subscription = KafkaSubscription.start(spec, settings, mock)
        assertEquals(SubscriptionEvent.Assigned(listOf("0", "1")), withTimeout(10_000) { subscription.next() })
        repeat(3) { withTimeout(10_000) { subscription.next() } }

        val inPoll = CountDownLatch(1)
        val release = CountDownLatch(1)
        // One poll that revokes everything (a rebalance giving only partition 1 back) once released.
        mock.schedulePollTask {
            inPoll.countDown()
            release.await()
            mock.rebalance(listOf(tp1))
        }
        withTimeout(10_000) { while (inPoll.count > 0) kotlinx.coroutines.delay(10) }
        subscription.ack("0", "2")
        release.countDown()
        until("the revoke committed the pending ack") { mock.commits.any { it == mapOf(tp0 to 3L) } }

        // Close: an ack given just before it is committed by the time close() returns.
        subscription.ack("1", "7") // not a partition it has delivered from, but it holds it
        subscription.close()
        assertTrue(mock.commits.any { it == mapOf(tp1 to 8L) }, "close flushed the pending ack: ${mock.commits}")
    }

    @Test
    fun `a failed commit is reported once through takeError, and the subscription goes on`() = runBlocking {
        val mock = object : MockConsumer<String, ByteArray>("earliest") {
            val fail = AtomicBoolean(true)
            override fun commitSync(offsets: Map<TopicPartition, OffsetAndMetadata>, timeout: Duration) {
                if (fail.getAndSet(false)) throw TimeoutException("the coordinator did not answer")
                super.commitSync(offsets, timeout)
            }
        }
        mock.schedulePollTask {
            mock.updateBeginningOffsets(mapOf(tp0 to 0L))
            mock.updateEndOffsets(mapOf(tp0 to 10L))
            mock.rebalance(listOf(tp0))
            mock.seek(tp0, 0)
            mock.addRecord(record(tp0, 0))
        }
        val subscription = KafkaSubscription.start(spec, settings, mock)
        assertEquals(SubscriptionEvent.Assigned(listOf("0")), withTimeout(10_000) { subscription.next() })
        withTimeout(10_000) { subscription.next() }
        assertNull(subscription.takeError(), "nothing went wrong yet")
        subscription.ack("0", "0")
        var error: String? = null
        until("the failed commit reported") { subscription.takeError()?.also { error = it } != null }
        assertTrue(error!!.startsWith("kafka commit: "), error)
        assertNull(subscription.takeError(), "reported once")
        subscription.ack("0", "0")
        until("the commit went through on the next try") { mock.committed(setOf(tp0))[tp0]?.offset() == 1L }
        subscription.close()
    }

    @Test
    fun `a fatal poll error is never lost to a cancelled next, every later call throws it`() = runBlocking {
        val mock = Mock(timeouts = 0)
        val subscription = KafkaSubscription.start(spec, settings, mock)
        // A next() racing with the caller's timers, cancelled before or after the failure arrived.
        assertNull(withTimeoutOrNull(50) { subscription.next() })
        mock.setPollException(KafkaException("fatal: the consumer cannot go on"))
        assertNull(withTimeoutOrNull(1) { runCatching { subscription.next() }.getOrNull() })
        repeat(2) {
            val thrown = runCatching { withTimeout(10_000) { subscription.next() } }.exceptionOrNull()
            assertTrue(thrown is EventStoreException.Failed, "call ${it + 1}: $thrown")
        }
        subscription.close()
    }

    @Test
    fun `an assignment of no partition is announced, the instance is an idle member`() = runBlocking {
        val mock = Mock(timeouts = 0)
        mock.schedulePollTask { mock.rebalance(emptyList()) }
        val subscription = KafkaSubscription.start(spec, settings, mock)
        assertEquals(SubscriptionEvent.Assigned(emptyList()), withTimeout(10_000) { subscription.next() })
        assertTrue(subscription.segments().isEmpty())
        subscription.close()
    }
}
