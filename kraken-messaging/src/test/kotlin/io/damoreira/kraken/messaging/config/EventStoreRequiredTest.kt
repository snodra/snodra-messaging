package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.config.sagascan.SagaScanConfig
import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.kafka.KafkaEventStore
import io.damoreira.kraken.messaging.kafka.KafkaEventStoreAutoConfiguration
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * No event store (ADR 0027): commands and queries run without one; an event handler, a saga or a publisher fails the startup
 * with a message naming what needs it and the add-on that provides one. Two event stores need `kraken.messaging.event-store`.
 */
class EventStoreRequiredTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                EventsAutoConfiguration::class.java,
                SagaAutoConfiguration::class.java,
                ExternalAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
            ),
        )
        .withUserConfiguration(MessageStoreMockConfig::class.java)
        .withPropertyValues(
            "kraken.messaging.service-name=orders",
            "kraken.messaging.type-prefix=test",
            "kraken.messaging.hub.enabled=false",
            "kraken.messaging.events.consumer.enabled=false",
            "kraken.messaging.outbox.enabled=false",
            "kraken.messaging.sagas.relay.poll-interval=1h",
            "kraken.messaging.deadlines.poll-interval=1h",
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
            assertTrue("kraken-messaging-kafka" in message, message)
        }
    }

    @Test
    fun sagaWithoutEventStoreFailsAtStartup() {
        runner.withUserConfiguration(SagaScanConfig::class.java).run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("@Saga scan.Order needs an event store" in message, message)
            assertTrue("kraken-messaging-kafka" in message, message)
        }
    }

    @Test
    fun publishingWithoutEventStoreFailsAtStartupNamingTheAddOn() {
        runner.withUserConfiguration(PublisherConfig::class.java).run { context ->
            val failure = assertNotNull(context.startupFailure, "no event store, no event bus")
            val analysis = assertNotNull(MissingMessageStoreFailureAnalyzer().analyze(failure), "the analyzer explains a missing EventBus")
            assertTrue("event store" in analysis.description, analysis.description)
            assertTrue("kraken-messaging-kafka" in analysis.action.orEmpty(), analysis.action)
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
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9")
            .run { context ->
                assertNull(context.startupFailure)
                assertIs<KafkaEventStore>(context.getBean(EventStore::class.java))
            }
        // Named by the setting: the same. Another name: the add-on steps back, and the guard names what is missing.
        runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9", "kraken.messaging.event-store=kafka")
            .run { context -> assertNull(context.startupFailure) }
        runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9", "kraken.messaging.event-store=rabbitmq-streams")
            .run { context ->
                val message = failureOf(context.startupFailure)
                assertTrue("kraken.messaging.event-store is 'rabbitmq-streams', but no event store is configured" in message, message)
            }
    }

    @Test
    fun twoEventStoresNeedTheSetting() {
        runner.withBean("first", EventStore::class.java, { store("kafka") })
            .withBean("second", EventStore::class.java, { store("rabbitmq-streams") })
            .run { context ->
                val message = failureOf(context.startupFailure)
                assertTrue("2 event stores are configured (kafka, rabbitmq-streams)" in message, message)
                assertTrue("kraken.messaging.event-store" in message, message)
            }
    }

    private fun failureOf(failure: Throwable?): String {
        assertNotNull(failure, "expected a startup failure")
        return generateSequence(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
    }
}
