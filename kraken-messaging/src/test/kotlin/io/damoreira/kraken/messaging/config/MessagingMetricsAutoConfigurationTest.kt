package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.damoreira.kraken.messaging.events.OutboxPublisher
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.kafka.KafkaEventStoreAutoConfiguration
import io.damoreira.kraken.messaging.metrics.EventProcessorMetrics
import io.damoreira.kraken.messaging.metrics.MessagingMetrics
import io.damoreira.kraken.messaging.metrics.MicrometerMessagingMetrics
import io.damoreira.kraken.messaging.metrics.NoopMessagingMetrics
import io.damoreira.kraken.messaging.test.InProcessHub
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertSame
import kotlin.test.assertTrue

/** The messaging meters are registered when a `MeterRegistry` bean exists, and the components report to them (P9, ADR 0026). */
class MessagingMetricsAutoConfigurationTest {
    private val hub = InProcessHub().start()

    @AfterEach fun tearDown() = hub.close()

    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                EventsAutoConfiguration::class.java,
                MessagingMetricsAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
                KafkaEventStoreAutoConfiguration::class.java,
            ),
        )
        .withUserConfiguration(MessageStoreMockConfig::class.java)
        .withBean("krakenHubChannelFactory", HubChannelFactory::class.java, { HubChannelFactory { hub.channel() } })
        .withPropertyValues(
            "kraken.messaging.service-name=orders",
            "kraken.messaging.type-prefix=test",
            "kraken.messaging.kafka.bootstrap-servers=localhost:9",
            "kraken.messaging.outbox.enabled=false",
        )

    private fun field(target: Any, name: String): Any? =
        target.javaClass.getDeclaredField(name).apply { isAccessible = true }.get(target)

    @Test fun `with a registry the components report to the Micrometer meters and the processor gauges are sampled`() {
        runner.withBean(SimpleMeterRegistry::class.java, { SimpleMeterRegistry() }).run { context ->
            assertEquals(null, context.startupFailure)
            val metrics = context.getBean(MessagingMetrics::class.java)
            assertIs<MicrometerMessagingMetrics>(metrics)
            assertSame(metrics, field(context.getBean(LocalEventDispatcher::class.java), "metrics"))
            assertSame(metrics, field(context.getBean(HubBus::class.java), "metrics"))
            assertTrue(context.getBean(EventProcessorMetrics::class.java).isRunning)
        }
    }

    @Test fun `the relay reports to them too`() {
        runner.withBean(SimpleMeterRegistry::class.java, { SimpleMeterRegistry() })
            .withPropertyValues("kraken.messaging.outbox.enabled=true", "kraken.messaging.events.consumer.enabled=false")
            .run { context ->
                assertEquals(null, context.startupFailure)
                assertSame(context.getBean(MessagingMetrics::class.java), field(context.getBean(OutboxPublisher::class.java), "metrics"))
                assertTrue(context.getBeansOfType(EventProcessorMetrics::class.java).isEmpty(), "no event processors, no processor gauges")
            }
    }

    @Test fun `without a registry nothing is registered and the components report nowhere`() {
        runner.run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(context.getBeansOfType(MessagingMetrics::class.java).isEmpty())
            assertTrue(context.getBeansOfType(EventProcessorMetrics::class.java).isEmpty())
            assertSame(NoopMessagingMetrics, field(context.getBean(LocalEventDispatcher::class.java), "metrics"))
            assertSame(NoopMessagingMetrics, field(context.getBean(HubBus::class.java), "metrics"))
        }
    }

    @Test fun `the hub sees the configured instance id, the one the metrics resource carries`() {
        runner.withPropertyValues("kraken.messaging.instance-id=pod-7").run { context ->
            assertEquals(null, context.startupFailure)
            assertEquals("pod-7", context.getBean(HubConnections::class.java).instanceId)
        }
        runner.run { context ->
            val id = context.getBean(KrakenMessagingProperties::class.java).instanceId
            java.util.UUID.fromString(id) // a fresh one when unset
            assertEquals(id, context.getBean(HubConnections::class.java).instanceId)
        }
    }

    @Test fun `an application's own MessagingMetrics bean replaces the Micrometer one`() {
        val own = object : MessagingMetrics {}
        runner.withBean(SimpleMeterRegistry::class.java, { SimpleMeterRegistry() })
            .withBean(MessagingMetrics::class.java, { own })
            .run { context ->
                assertEquals(null, context.startupFailure)
                assertSame(own, context.getBean(MessagingMetrics::class.java))
                assertSame(own, field(context.getBean(HubBus::class.java), "metrics"))
            }
    }
}
