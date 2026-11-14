package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.Delivery
import io.damoreira.kraken.messaging.eventstore.EventHeaders
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.test.KrakenContainers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.TimeUnit

/** The Kafka event store on a real broker, beyond what the conformance kit asks: throughput and the admin's edges. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class KafkaEventStoreBrokerTest {
    private val kafka = KrakenContainers.kafka()
    private val store by lazy { KafkaEventStore(KafkaEventStoreSettings(bootstrapServers = kafka.bootstrapServers)) }

    @BeforeAll
    fun start() = kafka.start()

    @AfterAll
    fun stop() {
        store.close()
        kafka.stop()
    }

    private fun topic(partitions: Int): String {
        val name = "kraken.broker." + UUID.randomUUID().toString().replace("-", "")
        Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers)).use {
            it.createTopics(listOf(NewTopic(name, partitions, 1.toShort()))).all().get(30, TimeUnit.SECONDS)
        }
        return name
    }

    /**
     * A reader that acknowledges at once must not wait on the queue: 10 000 events through a subscription well above the
     * 1 000 events a second that holding every partition until the queue was empty (a 100 ms poll per batch of 100) allowed.
     */
    @Test
    fun `a subscription that acknowledges at once reads thousands of events a second`() = runBlocking {
        val source = topic(2)
        val total = 10_000
        KafkaProducer(
            mapOf<String, Any>("bootstrap.servers" to kafka.bootstrapServers, "linger.ms" to 5), StringSerializer(), ByteArraySerializer(),
        ).use { producer ->
            repeat(total) { n ->
                val record = ProducerRecord<String, ByteArray>(source, "k${n % 50}", """{"n":$n}""".toByteArray())
                record.headers().add(RecordHeader(EventHeaders.ID, "e$n".toByteArray()))
                producer.send(record)
            }
            producer.flush()
        }
        val subscription = store.subscribe(SubscriptionSpec("throughput.${UUID.randomUUID()}", source))
        var received = 0
        var startedAt = 0L
        withTimeout(120_000) {
            while (received < total) {
                val event = subscription.next()
                if (event is Delivery) {
                    if (received == 0) startedAt = System.nanoTime()
                    subscription.ack(event.segment, event.position)
                    received++
                }
            }
        }
        val seconds = (System.nanoTime() - startedAt) / 1e9
        val rate = total / seconds
        println("KafkaEventStore throughput: $total events in ${"%.2f".format(seconds)} s = ${rate.toLong()} events/s")
        subscription.close()
        assertTrue(rate > 3_000, "at least 3 000 events/s, was ${rate.toLong()}")
    }

    @Test
    fun `a partition named twice under two spellings is invalid, an empty apply does nothing, and next after close says closed`() = runBlocking {
        val source = topic(2)
        val spec = SubscriptionSpec("admin.${UUID.randomUUID()}", source)
        val admin = store.admin(spec)
        val twice = runCatching { admin.plan(ResetTarget.Positions(mapOf("1" to "0", "01" to "0"))) }.exceptionOrNull()
        assertTrue(twice is EventStoreException.Invalid, "1 and 01 name one partition: $twice")
        admin.apply(emptyList())
        assertEquals(listOf("0", "1"), admin.plan(ResetTarget.Earliest).map { it.segment })

        val subscription = store.subscribe(spec)
        subscription.close()
        val error = assertThrows(EventStoreException.Failed::class.java) { runBlocking { subscription.next() } }
        assertTrue(error.message!!.contains("closed"), error.message)
    }
}
