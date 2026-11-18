package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.EventBus
import com.damoreira.snodra.messaging.config.sagascan.SagaScanConfig
import com.damoreira.snodra.messaging.eventstore.Capability
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
import com.damoreira.snodra.messaging.eventstore.Subscription
import com.damoreira.snodra.messaging.eventstore.SubscriptionAdmin
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import com.damoreira.snodra.messaging.kafka.KafkaEventStore
import com.damoreira.snodra.messaging.kafka.KafkaEventStoreAutoConfiguration
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * No event store (ADR 0027): commands and queries run without one; an event handler, a saga or a publisher fails the startup
 * with a message naming what needs it and the add-on that provides one. Two event stores need `snodra.messaging.event-store`.
 */
class EventStoreRequiredTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                SnodraMessagingAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                EventsAutoConfiguration::class.java,
                SagaAutoConfiguration::class.java,
                ExternalAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
            ),
        )
        .withUserConfiguration(MessageStoreMockConfig::class.java)
        .withPropertyValues(
            "snodra.messaging.service-name=orders",
            "snodra.messaging.type-prefix=test",
            "snodra.messaging.hub.enabled=false",
            "snodra.messaging.events.consumer.enabled=false",
            "snodra.messaging.outbox.enabled=false",
            "snodra.messaging.sagas.relay.poll-interval=1h",
            "snodra.messaging.deadlines.poll-interval=1h",
        )

    private fun store(name: String): EventStore = mockk(relaxed = true) {
        every { this@mockk.name } returns name
        every { capabilities } returns Capability.entries.toSet()
    }

    @Test
    fun eventHandlerWithoutEventStoreFailsAtStartup() {
        runner.withUserConfiguration(EventHandlersConfig::class.java).run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("@EventHandler OrderEvents.on needs an event store" in message, message)
            assertTrue("snodra-messaging-kafka" in message, message)
        }
    }

    @Test
    fun sagaWithoutEventStoreFailsAtStartup() {
        runner.withUserConfiguration(SagaScanConfig::class.java).run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("@Saga scan.Order needs an event store" in message, message)
            assertTrue("snodra-messaging-kafka" in message, message)
        }
    }

    @Test
    fun publishingWithoutEventStoreFailsAtStartupNamingTheAddOn() {
        runner.withUserConfiguration(PublisherConfig::class.java).run { context ->
            val failure = assertNotNull(context.startupFailure, "no event store, no event bus")
            val analysis = assertNotNull(MissingMessageStoreFailureAnalyzer().analyze(failure), "the analyzer explains a missing EventBus")
            assertTrue("event store" in analysis.description, analysis.description)
            assertTrue("snodra-messaging-kafka" in analysis.action.orEmpty(), analysis.action)
        }
    }

    @Test
    fun commandsAndQueriesNeedNoEventStore() {
        runner.withUserConfiguration(HandlersConfig::class.java).run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBeansOfType(EventStore::class.java).isEmpty())
            assertTrue(context.getBeansOfType(EventBus::class.java).isEmpty())
        }
    }

    @Test
    fun theKafkaAddOnIsTheEventStoreWhenItIsTheOnlyOne() {
        runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java))
            .withUserConfiguration(EventHandlersConfig::class.java)
            .withPropertyValues("snodra.messaging.kafka.bootstrap-servers=localhost:9")
            .run { context ->
                assertNull(context.startupFailure)
                assertIs<KafkaEventStore>(context.getBean(EventStore::class.java))
            }
        // Named by the setting: the same. Another name: the add-on steps back, and the guard names what is missing.
        runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("snodra.messaging.kafka.bootstrap-servers=localhost:9", "snodra.messaging.event-store=kafka")
            .run { context -> assertNull(context.startupFailure) }
        runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("snodra.messaging.kafka.bootstrap-servers=localhost:9", "snodra.messaging.event-store=rabbitmq-streams")
            .run { context ->
                val message = failureOf(context.startupFailure)
                assertTrue("snodra.messaging.event-store is 'rabbitmq-streams', but no event store is configured" in message, message)
            }
    }

    @Test
    fun twoEventStoresNeedTheSetting() {
        runner.withBean("first", EventStore::class.java, { store("kafka") })
            .withBean("second", EventStore::class.java, { store("rabbitmq-streams") })
            .run { context ->
                val message = failureOf(context.startupFailure)
                assertTrue("2 event stores are configured (kafka, rabbitmq-streams)" in message, message)
                assertTrue("snodra.messaging.event-store" in message, message)
            }
    }

    /**
     * Two event-store add-ons on one classpath (the Kafka add-on and another following the convention): without the setting
     * the startup fails naming both, never one winning silently; each value runs that one; the application's own wins over both.
     */
    @Test
    fun twoEventStoreAddOnsNeedTheSetting() {
        val both = runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java, OtherEventStoreAddOn::class.java))
            .withPropertyValues("snodra.messaging.kafka.bootstrap-servers=localhost:9")
        both.run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("2 event store add-ons are configured" in message, message)
            assertTrue("KafkaEventStore: kafka" in message && "OtherEventStore: other" in message, message)
            assertTrue("set snodra.messaging.event-store to the one this service uses (kafka, other)" in message || "set snodra.messaging.event-store to the one this service uses (other, kafka)" in message, message)
        }
        both.withPropertyValues("snodra.messaging.event-store=kafka").run { context ->
            assertNull(context.startupFailure)
            assertIs<KafkaEventStore>(context.getBean(EventStore::class.java))
        }
        both.withPropertyValues("snodra.messaging.event-store=other").run { context ->
            assertNull(context.startupFailure)
            assertIs<OtherEventStore>(context.getBean(EventStore::class.java))
        }
        val mine = store("mine")
        both.withBean("myEventStore", EventStore::class.java, { mine }).run { context ->
            assertNull(context.startupFailure)
            assertEquals(listOf("myEventStore"), context.getBeanNamesForType(EventStore::class.java).toList())
        }
    }

    private fun failureOf(failure: Throwable?): String {
        assertNotNull(failure, "expected a startup failure")
        return generateSequence(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
    }
}

/** A second event-store add-on following the convention ([ConditionalOnNoApplicationEventStore], its name as the setting). */
@AutoConfiguration(after = [SnodraMessagingAutoConfiguration::class], before = [EventsAutoConfiguration::class])
class OtherEventStoreAddOn {
    @Bean
    @ConditionalOnNoApplicationEventStore
    @ConditionalOnProperty(name = ["snodra.messaging.event-store"], havingValue = OtherEventStore.NAME, matchIfMissing = true)
    fun otherEventStore(): OtherEventStore = OtherEventStore()
}

class OtherEventStore : EventStore {
    override val name: String = NAME
    override val capabilities: Set<Capability> = emptySet()
    override suspend fun publish(event: OutgoingEvent) = Unit
    override suspend fun subscribe(spec: SubscriptionSpec): Subscription = error("not in this test")
    override fun admin(spec: SubscriptionSpec): SubscriptionAdmin = error("not in this test")
    override fun compare(a: String, b: String): Int = a.compareTo(b)

    companion object {
        const val NAME = "other"
    }
}
