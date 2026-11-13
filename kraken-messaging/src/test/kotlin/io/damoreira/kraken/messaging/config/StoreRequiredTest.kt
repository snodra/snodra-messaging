package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.config.sagascan.SagaScanConfig
import io.damoreira.kraken.messaging.fixtures.Happened
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.test.InProcessHub
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.autoconfigure.ImportAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.test.system.CapturedOutput
import org.springframework.boot.test.system.OutputCaptureExtension
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertFails
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class OrderEvents {
    @EventHandler fun on(event: Happened) = Unit
}

@Configuration(proxyBeanMethods = false)
class EventHandlersConfig {
    @Bean fun orderEvents() = OrderEvents()
}

/** Application code that publishes events: it injects the [EventBus]. */
class Publisher(val events: EventBus)

@Configuration(proxyBeanMethods = false)
class PublisherConfig {
    @Bean fun publisher(events: EventBus) = Publisher(events)
}

/** A Spring Boot application that publishes events and has no message store: only the starter's own configurations. */
@Configuration(proxyBeanMethods = false)
@ImportAutoConfiguration(KrakenMessagingAutoConfiguration::class, HubAutoConfiguration::class, KafkaEventsAutoConfiguration::class)
@Import(PublisherConfig::class)
class PublisherApp

/**
 * No message store (ADR 0028): commands and queries run without one; an event handler, a saga or a publisher fails the startup
 * with a message naming what needs the store and the add-on that provides one.
 */
@ExtendWith(OutputCaptureExtension::class)
class StoreRequiredTest {
    private val hub = InProcessHub().start()

    @AfterEach fun tearDown() = hub.close()

    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                KafkaEventsAutoConfiguration::class.java,
                SagaAutoConfiguration::class.java,
                ExternalAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues(
            "kraken.messaging.service-name=orders",
            "kraken.messaging.type-prefix=test",
            "kraken.messaging.hub.enabled=false",
        )

    @Test
    fun eventHandlerWithoutStoreFailsAtStartup() {
        runner.withUserConfiguration(EventHandlersConfig::class.java).run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("@EventHandler OrderEvents.on needs a message store" in message, message)
            assertTrue("kraken-messaging-mongo" in message, message)
        }
    }

    @Test
    fun sagaWithoutStoreFailsAtStartup() {
        runner.withUserConfiguration(SagaScanConfig::class.java).run { context ->
            val message = failureOf(context.startupFailure)
            assertTrue("@Saga scan.Order needs a message store" in message, message)
            assertTrue("kraken-messaging-mongo" in message, message)
        }
    }

    @Test
    fun commandsAndQueriesNeedNoStore() {
        runner
            .withPropertyValues("kraken.messaging.hub.enabled=true")
            .withBean("krakenHubChannelFactory", HubChannelFactory::class.java, { HubChannelFactory { hub.channel() } })
            .withUserConfiguration(HandlersConfig::class.java)
            .run { context ->
                assertNull(context.startupFailure)
                assertTrue(context.getBeansOfType(MessageStore::class.java).isEmpty())
                assertTrue(context.getBeansOfType(EventBus::class.java).isEmpty(), "no store, no event bus")
                val connections = context.getBean(HubConnections::class.java)
                await atMost Duration.ofSeconds(5) until { connections.connected.value && "test.Ping" in hub.registeredTypes() }
                // The service serves a command through the hub, with no database at all.
                val client = HubConnectionsClient(hub)
                try {
                    assertEquals(Pong("hello", Instant.EPOCH), runBlocking { client.bus.send(Ping("hello")) })
                } finally {
                    client.close()
                }
                assertNotNull(context.getBean(CommandBus::class.java))
            }
    }

    @Test
    fun eventBusWithoutStoreNamesTheAddOn() {
        runner.withUserConfiguration(PublisherConfig::class.java).run { context ->
            val missing = generateSequence(context.startupFailure) { it.cause }.filterIsInstance<NoSuchBeanDefinitionException>().first()
            val analysis = assertNotNull(MissingMessageStoreFailureAnalyzer().analyze(context.startupFailure!!), "the analyzer explains a missing EventBus")
            assertEquals(EventBus::class.java, missing.beanType)
            assertTrue("EventBus" in analysis.description && "needs a message store" in analysis.description, analysis.description)
            assertTrue("kraken-messaging-mongo" in analysis.action.orEmpty(), analysis.action)
        }
    }

    @Test
    fun theStartupFailureReportNamesTheAddOn(output: CapturedOutput) {
        val app = SpringApplication(PublisherApp::class.java).apply { setWebApplicationType(WebApplicationType.NONE) }
        assertFails { app.run("--kraken.messaging.service-name=orders", "--kraken.messaging.type-prefix=test", "--kraken.messaging.hub.enabled=false") }
        // Spring Boot's failure report, as a service prints it: our analysis, not the generic "consider defining a bean".
        assertTrue("APPLICATION FAILED TO START" in output.all, output.all)
        assertTrue("the EventBus needs a message store" in output.all, output.all)
        assertTrue("kraken-messaging-mongo" in output.all, output.all)
    }

    private fun failureOf(failure: Throwable?): String {
        assertNotNull(failure, "expected a startup failure")
        return generateSequence(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
    }
}
