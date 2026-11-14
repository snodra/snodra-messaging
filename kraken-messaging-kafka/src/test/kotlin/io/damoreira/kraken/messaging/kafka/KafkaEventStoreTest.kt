package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStoreException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration

class KafkaEventStoreTest {
    private val store = KafkaEventStore(KafkaEventStoreSettings(bootstrapServers = "localhost:1"))

    @Test
    fun `reports its name and all four capabilities`() {
        assertEquals("kafka", store.name)
        assertEquals(setOf(Capability.REPLAY, Capability.BROWSE, Capability.LAG, Capability.NATIVE_DEAD_LETTER), store.capabilities)
        assertEquals(setOf("replay", "browse", "lag", "native-dead-letter"), store.capabilities.map { it.wireName }.toSet())
    }

    @Test
    fun `positions are offsets ordered numerically, not as text`() {
        assertTrue(store.compare("9", "10") < 0)
        assertEquals(0, store.compare("7", "7"))
        assertTrue(store.compare("100", "99") > 0)
        assertThrows(EventStoreException.Invalid::class.java) { store.compare("a", "1") }
    }

    @Test
    fun `settings refuse a blank server and keep the consumer group and the commits out of the client properties`() {
        assertThrows(IllegalArgumentException::class.java) { KafkaEventStoreSettings(bootstrapServers = " ") }
        val settings = KafkaEventStoreSettings("b:9092", clientProperties = mapOf("group.id" to "other", "enable.auto.commit" to true, "client.id" to "x"))
        val consumer = settings.consumerProperties("svc.default")
        assertEquals("svc.default", consumer["group.id"])
        assertEquals(false, consumer["enable.auto.commit"])
        assertEquals("x", consumer["client.id"])
        assertEquals("all", settings.producerProperties()["acks"])
        assertEquals("b:9092", settings.adminProperties()["bootstrap.servers"])
    }

    @Test
    fun `the auto-configuration provides the event store from the Kafka bootstrap servers, and yields to an application's own`() {
        val runner = ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KrakenMessagingAutoConfiguration::class.java, KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.service-name=svc", "kraken.messaging.type-prefix=test", "kraken.messaging.hub.enabled=false", "kraken.messaging.kafka.bootstrap-servers=broker:9")
        runner.run { ctx -> assertEquals("kafka", ctx.getBean(EventStore::class.java).name) }
        val own = KafkaEventStore(KafkaEventStoreSettings(bootstrapServers = "own:1"))
        runner.withBean("ownEventStore", EventStore::class.java, { own }).run { ctx ->
            assertEquals(listOf("ownEventStore"), ctx.getBeanNamesForType(EventStore::class.java).toList())
        }
    }
}
