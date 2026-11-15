package io.damoreira.kraken.messaging.rabbitmq

import com.rabbitmq.stream.OffsetSpecification
import com.rabbitmq.stream.impl.Client
import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.Subscription
import io.damoreira.kraken.messaging.eventstore.SubscriptionEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.TimeUnit

/** When a subscription stores its position, on a real broker ([RabbitBroker]): every stored position is an entry in the stream. */
class RabbitSubscriptionBrokerTest {
    private fun fresh(what: String) = "subscription.$what." + UUID.randomUUID().toString().replace("-", "").take(10)

    private fun event(source: String, key: String) = UUID.randomUUID().toString().let { id ->
        OutgoingEvent(source, id, "t", key, mapOf("id" to id, "type" to "t"), "{}".toByteArray())
    }

    /** The offset the next event appended to [stream] gets (read by a plain client): the events and the tracking entries before it. */
    private suspend fun nextOffset(store: RabbitEventStore, stream: String): Long {
        store.publish(event(stream, "fence"))
        val offsets = LinkedBlockingQueue<Long>()
        val client = RabbitBroker.client(Client.ClientParameters().messageListener { _, offset, _, _, _, _ -> offsets.add(offset) })
        try {
            client.subscribe(0, stream, OffsetSpecification.last(), 1)
            var last = -1L
            while (true) last = offsets.poll(2, TimeUnit.SECONDS) ?: return last
        } finally {
            client.close()
        }
    }

    private suspend fun deliveries(subscription: Subscription, n: Int, ack: Boolean = true): List<Delivery> {
        val got = mutableListOf<Delivery>()
        while (got.size < n) {
            val e = withTimeoutOrNull(30_000) { subscription.next() } ?: error("only ${got.size} of $n deliveries")
            if (e is Delivery) {
                got += e
                if (ack) subscription.ack(e.segment, e.position)
            }
        }
        return got
    }

    @Test
    fun `acknowledgements are stored every so many and at close, not one by one`() = runBlocking {
        val stream = fresh("periodic")
        RabbitBroker.stream(stream)
        RabbitEventStore(RabbitBroker.settings(management = false, offsetStoreMessages = 50, offsetStoreInterval = Duration.ofHours(1))).use { store ->
            repeat(200) { store.publish(event(stream, "k")) }
            val sub = store.subscribe(SubscriptionSpec("svc.periodic", stream))
            deliveries(sub, 200)
            sub.close()
            assertEquals(200L, RabbitBroker.client().use { it.queryOffset("svc.periodic", stream).offset })
            // 200 events, then one tracking entry per 50 acknowledgements (the close finds 200 stored already): the next event is 204.
            assertEquals(204L, nextOffset(store, stream), "4 tracking entries for 200 acknowledgements")
        }
    }

    @Test
    fun `a partition moving away stores its position first`() = runBlocking {
        val source = fresh("moving")
        RabbitBroker.superStream(source, 2)
        // Only a deactivation (or a close) can store under this policy.
        val settings = RabbitBroker.settings(management = false, offsetStoreMessages = 1_000_000, offsetStoreInterval = Duration.ofHours(1))
        RabbitEventStore(settings).use { first ->
            RabbitEventStore(settings).use { second ->
                // Keys on both partitions.
                val keys = (0 until 40).map { "key-$it" }
                for (k in keys) first.publish(event(source, k))
                val spec = SubscriptionSpec("svc.moving", source)
                val a = first.subscribe(spec)
                val seen = deliveries(a, keys.size)
                assertTrue(seen.map { it.segment }.toSet() == setOf("0", "1"), "both partitions read")
                val b = second.subscribe(spec)
                // a loses one partition: Revoked, then Assigned([]); b gets it from after what a acknowledged.
                val changes = mutableListOf<SubscriptionEvent>()
                while (changes.none { it is SubscriptionEvent.Revoked }) {
                    changes += withTimeoutOrNull(30_000) { a.next() } ?: error("no revocation: $changes")
                }
                val revoked = (changes.last() as SubscriptionEvent.Revoked).segments.single()
                assertEquals(SubscriptionEvent.Assigned(emptyList()), withTimeoutOrNull(10_000) { a.next() })
                val moved = seen.filter { it.segment == revoked }
                // Events of the moved partition after the move (keys routed to it).
                val late = generateSequence(0) { it + 1 }.map { "late-$it" }.filter { RabbitWire.partitionIndex(it, 2).toString() == revoked }.take(3)
                for (k in late) first.publish(event(source, k))
                var firstOfB: Delivery? = null
                while (firstOfB == null) {
                    val e = withTimeoutOrNull(30_000) { b.next() } ?: error("b delivers nothing")
                    if (e is Delivery && e.segment == revoked) firstOfB = e
                }
                assertTrue(
                    firstOfB.position.toLong() > moved.maxOf { it.position.toLong() },
                    "b starts after what a acknowledged (${moved.maxOf { it.position }}), not at ${firstOfB.position}",
                )
                a.close()
                b.close()
            }
        }
    }
}
