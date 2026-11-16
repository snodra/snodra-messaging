package io.damoreira.kraken.messaging.rabbitmq

import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.OutgoingEvent
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Publish and consume rates on a real broker, with the Rust add-on's floors (publish 500/s one confirmed event at a time, as the
 * outbox relay publishes; consume 3 000/s acknowledging each). Tagged `throughput`: not part of `test` (a shared Docker VM makes
 * rates meaningless), run alone with `./gradlew throughputTest`.
 */
@Tag("throughput")
class RabbitThroughputTest {
    @Test
    fun `ten thousand events are published and consumed above the floors`() = runBlocking {
        val source = "throughput." + UUID.randomUUID().toString().replace("-", "").take(10)
        RabbitBroker.superStream(source, 4)
        val total = 10_000
        RabbitEventStore(RabbitBroker.settings(management = false)).use { store ->
            val started = System.nanoTime()
            repeat(total) { n ->
                store.publish(OutgoingEvent(source, "e-$n", "t", "k-${n % 100}", mapOf("id" to "e-$n", "type" to "t"), """{"n":$n}""".toByteArray()))
            }
            val publish = total / ((System.nanoTime() - started) / 1e9)
            val sub = store.subscribe(SubscriptionSpec("svc.throughput", source))
            var got = 0
            var first = 0L
            while (got < total) {
                val e = withTimeoutOrNull(30_000) { sub.next() } ?: error("only $got of $total within 30 s of the last")
                if (e is Delivery) {
                    if (got == 0) first = System.nanoTime()
                    got++
                    sub.ack(e.segment, e.position)
                }
            }
            val consume = (total - 1) / ((System.nanoTime() - first) / 1e9)
            sub.close()
            LoggerFactory.getLogger(RabbitThroughputTest::class.java).info("THROUGHPUT rabbitmq: publish {} events/s, consume {} events/s", publish.toLong(), consume.toLong())
            println("THROUGHPUT rabbitmq: publish ${publish.toLong()} events/s, consume ${consume.toLong()} events/s")
            assertTrue(publish > 500, "publish at least 500 events/s, was ${publish.toLong()}")
            assertTrue(consume > 3_000, "consume at least 3 000 events/s, was ${consume.toLong()}")
        }
    }
}
