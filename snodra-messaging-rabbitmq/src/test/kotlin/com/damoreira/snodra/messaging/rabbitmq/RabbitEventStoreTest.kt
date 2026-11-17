package com.damoreira.snodra.messaging.rabbitmq

import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.EventStoreException
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
import com.damoreira.snodra.messaging.kafka.KafkaEventStore
import com.damoreira.snodra.messaging.kafka.KafkaEventStoreAutoConfiguration
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner

class RabbitEventStoreTest {
    private val store = RabbitEventStore(RabbitEventStoreSettings(hosts = listOf("127.0.0.1:1")))

    @Test
    fun `positions are offsets ordered numerically`() {
        assertEquals("rabbitmq-streams", store.name)
        assertTrue(store.compare("9", "10") < 0)
        assertEquals(0, store.compare("7", "7"))
        assertThrows(EventStoreException.Invalid::class.java) { store.compare("x", "1") }
    }

    @Test
    fun `an event over the frame is rejected before any connection, an unreachable broker is a failure`() = runBlocking {
        val big = OutgoingEvent("s", "e-1", "t", null, emptyMap(), ByteArray(RabbitPublisher.MAX_FRAME))
        assertThrows(EventStoreException.Rejected::class.java) { runBlocking { store.publish(big) } }
        val small = OutgoingEvent("s", "e-2", "t", "k", emptyMap(), "{}".toByteArray())
        assertThrows(EventStoreException.Failed::class.java) { runBlocking { store.publish(small) } }
        assertThrows(EventStoreException.Failed::class.java) {
            runBlocking { store.subscribe(com.damoreira.snodra.messaging.eventstore.SubscriptionSpec("svc.p", "s")) }
        }
    }

    @Test
    fun `the store's text carries no password`() {
        val s = RabbitEventStore(RabbitEventStoreSettings(hosts = listOf("h"), password = "stream-secret"))
        assertFalse("stream-secret" in s.toString(), s.toString())
    }

    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SnodraMessagingAutoConfiguration::class.java, RabbitEventStoreAutoConfiguration::class.java))
        .withPropertyValues(
            "snodra.messaging.service-name=svc", "snodra.messaging.type-prefix=test", "snodra.messaging.hub.enabled=false",
            "snodra.messaging.rabbitmq.hosts=a:5552,b",
        )

    @Test
    fun `the auto-configuration provides the event store from snodra messaging rabbitmq, and yields to an application's own`() {
        runner.withPropertyValues("snodra.messaging.rabbitmq.password=secret", "snodra.messaging.rabbitmq.offset-store-interval=2s").run { ctx ->
            assertNull(ctx.startupFailure)
            val store = ctx.getBean(EventStore::class.java)
            assertEquals("rabbitmq-streams", store.name)
            val properties = ctx.getBean(RabbitProperties::class.java).toSettings()
            assertEquals(listOf("a" to 5552, "b" to 5552), properties.nodes())
            assertEquals(java.time.Duration.ofSeconds(2), properties.offsetStoreInterval)
            assertEquals("secret", properties.password)
        }
        val own = RabbitEventStore(RabbitEventStoreSettings(hosts = listOf("own")))
        runner.withBean("ownEventStore", EventStore::class.java, { own }).run { ctx ->
            assertEquals(listOf("ownEventStore"), ctx.getBeanNamesForType(EventStore::class.java).toList())
        }
        runner.withPropertyValues("snodra.messaging.event-store=kafka").run { ctx ->
            // Another event store named: this one steps back, and the guard names what is missing.
            val failure = ctx.startupFailure!!.stackTraceToString()
            assertTrue("snodra.messaging.event-store is 'kafka', but no event store is configured" in failure, failure)
        }
    }

    /** The Kafka and the RabbitMQ Streams add-ons on one classpath: without the setting the startup fails naming both; each value runs that one. */
    @Test
    fun `two event-store add-ons need the setting`() {
        val both = runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("snodra.messaging.kafka.bootstrap-servers=localhost:9")
        both.run { ctx ->
            val failure = ctx.startupFailure!!.stackTraceToString()
            assertTrue("2 event store add-ons are configured" in failure, failure)
            assertTrue("KafkaEventStore: kafka" in failure && "RabbitEventStore: rabbitmq-streams" in failure, failure)
        }
        both.withPropertyValues("snodra.messaging.event-store=rabbitmq-streams").run { ctx ->
            assertNull(ctx.startupFailure)
            assertTrue(ctx.getBean(EventStore::class.java) is RabbitEventStore)
        }
        both.withPropertyValues("snodra.messaging.event-store=kafka").run { ctx ->
            assertNull(ctx.startupFailure)
            assertTrue(ctx.getBean(EventStore::class.java) is KafkaEventStore)
        }
    }
}
