package io.damoreira.kraken.messaging.config

import org.junit.jupiter.api.Test
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.env.SystemEnvironmentPropertySource
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PropertiesBindingTest {
    @Configuration(proxyBeanMethods = false)
    @EnableConfigurationProperties(KrakenMessagingProperties::class)
    class PropertiesOnly

    private val runner = ApplicationContextRunner().withUserConfiguration(PropertiesOnly::class.java)
    private val required = arrayOf("kraken.messaging.service-name=orders", "kraken.messaging.type-prefix=kraken.orders")

    @Test fun `defaults match spec`() {
        runner.withPropertyValues(*required).run { context ->
            val p = context.getBean(KrakenMessagingProperties::class.java)
            assertEquals("orders", p.serviceName)
            assertEquals("kraken.orders", p.typePrefix)
            assertEquals(true, p.hub.enabled)
            assertEquals("static://kraken-hub:7000", p.hub.address)
            assertEquals(null, p.hub.requestTimeout) // unset: the hub's default or per-type override applies
            assertEquals(Duration.ofSeconds(10), p.hub.heartbeatInterval)
            assertEquals(256, p.hub.maxConcurrentHandlers)
            assertEquals(Duration.ofMillis(500), p.hub.reconnectBackoff.min)
            assertEquals(Duration.ofSeconds(5), p.hub.reconnectBackoff.max)
            assertEquals("broker:19092", p.kafka.bootstrapServers)
            assertEquals("kraken.events", p.kafka.topic)
            assertEquals(true, p.kafka.consumer.enabled)
            assertEquals(5, p.kafka.consumer.maxAttempts)
            assertEquals(true, p.outbox.enabled)
            assertEquals(Duration.ofMillis(200), p.outbox.pollInterval)
            assertEquals(100, p.outbox.batchSize)
            assertEquals(10, p.outbox.maxAttempts)
            assertEquals(Duration.ofDays(7), p.outbox.retention)
            assertEquals(Duration.ofDays(30), p.idempotency.retention)
            assertEquals(true, p.security.forwardJwt)
        }
    }

    @Test fun `relaxed env binding KRAKEN_MESSAGING_HUB_ADDRESS`() {
        val env = mapOf<String, Any>(
            "KRAKEN_MESSAGING_SERVICE_NAME" to "billing",
            "KRAKEN_MESSAGING_TYPE_PREFIX" to "kraken.billing",
            "KRAKEN_MESSAGING_HUB_ADDRESS" to "static://hub.internal:7443",
            "KRAKEN_MESSAGING_HUB_RECONNECTBACKOFF_MIN" to "1s",
            "KRAKEN_MESSAGING_OUTBOX_RETENTION" to "3d",
        )
        runner
            .withInitializer { context ->
                // Named like the real environment source so Boot applies the environment-variable name mapping.
                val name = "test-" + StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME
                context.environment.propertySources.addFirst(SystemEnvironmentPropertySource(name, env))
            }
            .run { context ->
                val p = context.getBean(KrakenMessagingProperties::class.java)
                assertEquals("billing", p.serviceName)
                assertEquals("static://hub.internal:7443", p.hub.address)
                assertEquals(Duration.ofSeconds(1), p.hub.reconnectBackoff.min)
                assertEquals(Duration.ofSeconds(5), p.hub.reconnectBackoff.max)
                assertEquals(Duration.ofDays(3), p.outbox.retention)
            }
    }

    @Test fun `request-timeout binds when set`() {
        runner.withPropertyValues(*required, "kraken.messaging.hub.request-timeout=12s").run { context ->
            assertEquals(Duration.ofSeconds(12), context.getBean(KrakenMessagingProperties::class.java).hub.requestTimeout)
        }
    }

    @Test fun `missing service-name fails binding`() {
        runner.withPropertyValues("kraken.messaging.type-prefix=kraken.orders").run { context ->
            val failure = assertNotNull(context.startupFailure)
            val messages = generateSequence<Throwable>(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
            assertTrue("kraken.messaging" in messages, messages)
            assertTrue("parameter serviceName" in messages, messages)
        }
    }
}
