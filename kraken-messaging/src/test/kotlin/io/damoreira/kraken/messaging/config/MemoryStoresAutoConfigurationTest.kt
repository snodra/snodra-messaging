package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.kafka.KafkaEventStore
import io.damoreira.kraken.messaging.kafka.KafkaEventStoreAutoConfiguration
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.memory.MemoryMessageStore
import io.mockk.mockk
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.annotation.Transactional
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** A handler recording what it handled, and a publisher whose `@Transactional` method publishes. */
class RecordingOrderEvents {
    val handled = CopyOnWriteArrayList<String>()

    @EventHandler fun on(event: Happened) {
        handled += event.id
    }
}

open class TransactionalPublisher(private val events: EventBus) {
    @Transactional
    open fun publish(id: String, fail: Boolean = false) {
        events.publishBlocking(Happened(id, Instant.EPOCH), key = id)
        check(!fail) { "the business write failed" }
    }
}

@Configuration(proxyBeanMethods = false)
class InMemoryApplicationConfig {
    @Bean fun recordingOrderEvents() = RecordingOrderEvents()

    @Bean fun transactionalPublisher(events: EventBus) = TransactionalPublisher(events)
}

/**
 * The in-memory stores (ADR 0027, ADR 0028): chosen only by `kraken.messaging.event-store=memory` and
 * `kraken.messaging.store=memory`, never because nothing else is there, never behind an add-on's back; each warns that it is not
 * durable, naming the profile; a setting naming a store the application lacks fails the startup.
 */
@ExtendWith(OutputCaptureExtension::class)
class MemoryStoresAutoConfigurationTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                MemoryStoresAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                EventsAutoConfiguration::class.java,
                SagaAutoConfiguration::class.java,
                ExternalAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues(
            "kraken.messaging.service-name=orders",
            "kraken.messaging.type-prefix=test",
            "kraken.messaging.hub.enabled=false",
        )

    private val inMemory = arrayOf("kraken.messaging.store=memory", "kraken.messaging.event-store=memory")

    @Test
    fun theSettingsRunTheInMemoryStoresAndEachWarnsItIsNotDurable(output: CapturedOutput) {
        runner.withPropertyValues(*inMemory, "kraken.messaging.memory.segments=2").run { context ->
            assertNull(context.startupFailure)
            val store = assertIs<MemoryMessageStore>(context.getBean(MessageStore::class.java))
            val events = assertIs<MemoryEventStore>(context.getBean(EventStore::class.java))
            assertEquals(2, events.settings.segments)
            assertSame(store.transactions, context.getBean(PlatformTransactionManager::class.java), "@Transactional runs on the store's transactions")
        }
        assertTrue("in-memory message store: not durable (profile default)" in output.out, output.out)
        assertTrue("in-memory event store: not durable (profile default)" in output.out, output.out)
        assertTrue(output.out.lines().filter { "not durable" in it }.all { " WARN " in it }, "logged at WARN")
    }

    @Test
    fun theWarningNamesTheActiveProfiles(output: CapturedOutput) {
        runner.withPropertyValues(*inMemory).withInitializer { it.environment.setActiveProfiles("local", "demo") }.run { context ->
            assertNull(context.startupFailure)
        }
        assertTrue("in-memory message store: not durable (profile local,demo)" in output.out, output.out)
        assertTrue("in-memory event store: not durable (profile local,demo)" in output.out, output.out)
    }

    @Test
    fun withoutTheSettingsThereIsNoInMemoryStore(output: CapturedOutput) {
        runner.run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBeansOfType(MessageStore::class.java).isEmpty())
            assertTrue(context.getBeansOfType(EventStore::class.java).isEmpty())
        }
        // An event handler without either store still fails, naming the add-ons and the in-memory settings.
        runner.withUserConfiguration(EventHandlersConfig::class.java).run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("@EventHandler OrderEvents.on needs a message store" in message, message)
            assertTrue("kraken.messaging.store=memory" in message, message)
        }
        assertTrue("not durable (profile" !in output.out, "no in-memory store ran: ${output.out}")
        runner.withUserConfiguration(EventHandlersConfig::class.java).withPropertyValues("kraken.messaging.store=memory").run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("@EventHandler OrderEvents.on needs an event store" in message, message)
            assertTrue("kraken.messaging.event-store=memory" in message, message)
        }
    }

    @Test
    fun anEventStoreAddOnStaysTheStoreUnlessTheSettingNamesMemory() {
        val withKafka = runner.withConfiguration(AutoConfigurations.of(KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9", "kraken.messaging.store=memory")
        withKafka.run { context ->
            assertNull(context.startupFailure)
            assertIs<KafkaEventStore>(context.getBean(EventStore::class.java), "never the in-memory one silently")
        }
        withKafka.withPropertyValues("kraken.messaging.event-store=memory").run { context ->
            assertNull(context.startupFailure)
            assertIs<MemoryEventStore>(context.getBean(EventStore::class.java), "the add-on steps back when the setting names memory")
            assertEquals(1, context.getBeansOfType(EventStore::class.java).size)
        }
    }

    @Test
    fun aStoreSettingNamingAStoreTheApplicationLacksFailsTheStartup() {
        runner.withPropertyValues("kraken.messaging.store=jdbc").run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("kraken.messaging.store is 'jdbc', but no message store is configured" in message, message)
        }
        runner.withPropertyValues("kraken.messaging.store=jdbc").withUserConfiguration(MessageStoreMockConfig::class.java).run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("kraken.messaging.store is 'jdbc', but the message store is ''" in message, message)
        }
        runner.withBean("first", MessageStore::class.java, { MemoryMessageStore() }).withBean("second", MessageStore::class.java, { MemoryMessageStore() })
            .run { context ->
                val message = failureOf(context.startupFailure)
                assertTrue("2 message stores are configured (memory, memory): set kraken.messaging.store" in message, message)
            }
    }

    @Test
    fun theApplicationsOwnTransactionManagerIsKept() {
        val own = mockk<PlatformTransactionManager>(relaxed = true)
        runner.withPropertyValues(*inMemory).withBean("transactionManager", PlatformTransactionManager::class.java, { own }).run { context ->
            assertNull(context.startupFailure)
            assertSame(own, context.getBean(PlatformTransactionManager::class.java))
            assertIs<MemoryMessageStore>(context.getBean(MessageStore::class.java))
        }
    }

    @Test
    fun anEventPublishedInATransactionIsHandledOnceItCommittedAndNeverWhenItRolledBack() {
        runner.withPropertyValues(*inMemory, "kraken.messaging.outbox.poll-interval=1h")
            .withUserConfiguration(InMemoryApplicationConfig::class.java)
            .run { context ->
                assertNull(context.startupFailure)
                val publisher = context.getBean(TransactionalPublisher::class.java)
                val handler = context.getBean(RecordingOrderEvents::class.java)
                runCatching { publisher.publish("rolled-back", fail = true) }
                publisher.publish("committed")
                // The relay is woken by the commit (its poll is an hour away), the processor by the event store.
                await().atMost(Duration.ofSeconds(10)).until { handler.handled.toList() == listOf("committed") }
                val events = context.getBean(MemoryEventStore::class.java)
                assertEquals(listOf("committed"), events.events("kraken.events").map { it.key })
            }
    }

    private fun failureOf(failure: Throwable?): String {
        assertNotNull(failure, "expected a startup failure")
        return generateSequence(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
    }
}
