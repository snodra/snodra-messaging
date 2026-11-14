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
            assertEquals(256, p.hub.maxConcurrentHandlers)
            assertEquals(Duration.ofMillis(500), p.hub.reconnectBackoff.min)
            assertEquals(Duration.ofSeconds(5), p.hub.reconnectBackoff.max)
            assertEquals(Duration.ofMinutes(10), p.hub.streamCreditTimeout) // the hub's default max stream duration
            assertEquals(1024, p.hub.maxSubscriptions) // the hub's default per-instance subscription limit
            assertEquals(null, p.eventStore)
            assertEquals("kraken.events", p.events.source)
            assertEquals(true, p.events.consumer.enabled)
            assertEquals(5, p.events.consumer.maxAttempts)
            assertEquals(true, p.outbox.enabled)
            assertEquals(Duration.ofMillis(200), p.outbox.pollInterval)
            assertEquals(100, p.outbox.batchSize)
            assertEquals(10, p.outbox.maxAttempts)
            assertEquals(Duration.ofDays(7), p.outbox.retention)
            assertEquals(Duration.ofMinutes(2), p.outbox.lease)
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

    @Test fun `P1 hub defaults`() {
        runner.withPropertyValues(*required).run { context ->
            val hub = context.getBean(KrakenMessagingProperties::class.java).hub
            assertEquals(100, hub.loadFactor)
            assertEquals(emptyMap(), hub.tags)
            assertEquals(emptyMap(), hub.priorities)
            assertEquals(false, hub.retry.enabled)
            assertEquals(3, hub.retry.maxAttempts)
            assertEquals(java.time.Duration.ofMillis(100), hub.retry.initialBackoff)
            assertEquals(2.0, hub.retry.multiplier)
            assertEquals(java.time.Duration.ofSeconds(2), hub.retry.maxBackoff)
            assertEquals(null, hub.retry.policy())
        }
    }

    @Test fun `load factor, tags, per-type priorities and retry bind`() {
        runner.withPropertyValues(
            *required,
            "kraken.messaging.hub.load-factor=250",
            "kraken.messaging.hub.tags.zone=eu-1",
            "kraken.messaging.hub.priorities.orders.PlaceOrder=5",
            "kraken.messaging.hub.priorities[orders.CancelOrder]=-1",
            "kraken.messaging.hub.retry.enabled=true",
            "kraken.messaging.hub.retry.max-attempts=4",
            "kraken.messaging.hub.retry.initial-backoff=50ms",
        ).run { context ->
            val hub = context.getBean(KrakenMessagingProperties::class.java).hub
            assertEquals(250, hub.loadFactor)
            assertEquals(mapOf("zone" to "eu-1"), hub.tags)
            assertEquals(mapOf("orders.PlaceOrder" to 5, "orders.CancelOrder" to -1), hub.priorities)
            assertEquals(io.damoreira.kraken.messaging.RetryPolicy(maxAttempts = 4, initialBackoff = java.time.Duration.ofMillis(50)), hub.retry.policy())
        }
    }

    @Test fun `request-timeout binds when set`() {
        runner.withPropertyValues(*required, "kraken.messaging.hub.request-timeout=12s").run { context ->
            assertEquals(Duration.ofSeconds(12), context.getBean(KrakenMessagingProperties::class.java).hub.requestTimeout)
        }
    }

    @Test fun `stream-credit-timeout binds and must be positive`() {
        runner.withPropertyValues(*required, "kraken.messaging.hub.stream-credit-timeout=30s").run { context ->
            assertEquals(Duration.ofSeconds(30), context.getBean(KrakenMessagingProperties::class.java).hub.streamCreditTimeout)
        }
        for (value in listOf("0s", "-1s")) {
            val messages = startupFailure("kraken.messaging.hub.stream-credit-timeout=$value")
            assertTrue("kraken.messaging.hub.stream-credit-timeout must be positive" in messages, messages)
        }
    }

    @Test fun `max-subscriptions binds and must be at least 1`() {
        runner.withPropertyValues(*required, "kraken.messaging.hub.max-subscriptions=7").run { context ->
            assertEquals(7, context.getBean(KrakenMessagingProperties::class.java).hub.maxSubscriptions)
        }
        for (value in listOf("0", "-3")) {
            val messages = startupFailure("kraken.messaging.hub.max-subscriptions=$value")
            assertTrue("kraken.messaging.hub.max-subscriptions must be >= 1" in messages, messages)
        }
    }

    @Test fun `reconnect-backoff binds and max must be positive with min not above it`() {
        runner.withPropertyValues(*required, "kraken.messaging.hub.reconnect-backoff.min=100ms", "kraken.messaging.hub.reconnect-backoff.max=2s").run { context ->
            val backoff = context.getBean(KrakenMessagingProperties::class.java).hub.reconnectBackoff
            assertEquals(Duration.ofMillis(100), backoff.min)
            assertEquals(Duration.ofSeconds(2), backoff.max)
        }
        for (value in listOf("0s", "-1s")) {
            val messages = startupFailure("kraken.messaging.hub.reconnect-backoff.max=$value")
            assertTrue("kraken.messaging.hub.reconnect-backoff.max must be positive" in messages, messages)
        }
        val above = startupFailure("kraken.messaging.hub.reconnect-backoff.min=10s", "kraken.messaging.hub.reconnect-backoff.max=2s")
        assertTrue("kraken.messaging.hub.reconnect-backoff.min" in above && "must not be above" in above && "reconnect-backoff.max" in above, above)
        for (value in listOf("0s", "-1s")) {
            val messages = startupFailure("kraken.messaging.hub.reconnect-backoff.min=$value")
            assertTrue("kraken.messaging.hub.reconnect-backoff.min must be positive" in messages, messages)
        }
    }

    /** The messages of the startup failure (and its causes) with [properties] set; fails if the context started. */
    private fun startupFailure(vararg properties: String): String {
        var messages = ""
        runner.withPropertyValues(*required, *properties).run { context ->
            val failure = assertNotNull(context.startupFailure, "context started with ${properties.toList()}")
            messages = generateSequence<Throwable>(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
        }
        return messages
    }

    @Test fun `invalid retry settings fail at startup whether or not retry is enabled`() {
        for (enabled in listOf("false", "true")) {
            val on = "kraken.messaging.hub.retry.enabled=$enabled"
            for ((property, expected) in listOf(
                "kraken.messaging.hub.retry.max-attempts=0" to "maxAttempts must be >= 1",
                "kraken.messaging.hub.retry.multiplier=0.5" to "multiplier must be >= 1",
                "kraken.messaging.hub.retry.initial-backoff=-1ms" to "backoffs must not be negative",
                "kraken.messaging.hub.retry.max-backoff=-1s" to "backoffs must not be negative",
            )) {
                val messages = startupFailure(on, property)
                assertTrue(expected in messages, "$on, $property: $messages")
                assertTrue("kraken.messaging.hub.retry" in messages, "$on, $property: $messages")
            }
        }
    }

    @Test fun `outbox lease binds and must be longer than a send`() {
        runner.withPropertyValues(*required, "kraken.messaging.outbox.lease=90s").run { context ->
            assertEquals(Duration.ofSeconds(90), context.getBean(KrakenMessagingProperties::class.java).outbox.lease)
        }
        for (value in listOf("45s", "10s")) {
            val messages = startupFailure("kraken.messaging.outbox.lease=$value")
            assertTrue("kraken.messaging.outbox.lease must be longer than a send" in messages, messages)
        }
    }

    @Test fun `max-concurrent-handlers below 1 fails at startup`() {
        for (value in listOf("0", "-4")) {
            val messages = startupFailure("kraken.messaging.hub.enabled=false", "kraken.messaging.hub.max-concurrent-handlers=$value")
            assertTrue("kraken.messaging.hub.max-concurrent-handlers must be >= 1, was $value" in messages, messages)
        }
    }

    @Test fun `a negative load factor fails at startup, 0 is allowed`() {
        val messages = startupFailure("kraken.messaging.hub.enabled=false", "kraken.messaging.hub.load-factor=-1")
        assertTrue("kraken.messaging.hub.load-factor must not be negative, was -1" in messages, messages)
        runner.withPropertyValues(*required, "kraken.messaging.hub.load-factor=0").run { context ->
            assertEquals(null, context.startupFailure) // the hub reads 0 as 100
            assertEquals(0, context.getBean(KrakenMessagingProperties::class.java).hub.loadFactor)
        }
    }

    @Test fun `hub address takes a list or a dns target, and status-interval binds`() {
        runner.withPropertyValues(*required, "kraken.messaging.hub.address=static://hub-1:7000,hub-2:7000", "kraken.messaging.hub.status-interval=2s")
            .run { context ->
                val hub = context.getBean(KrakenMessagingProperties::class.java).hub
                assertEquals("static://hub-1:7000,hub-2:7000", hub.address)
                assertEquals(Duration.ofSeconds(2), hub.statusInterval)
            }
        runner.withPropertyValues(*required).run { context ->
            assertEquals(Duration.ofSeconds(5), context.getBean(KrakenMessagingProperties::class.java).hub.statusInterval)
        }
        runner.withPropertyValues(*required, "kraken.messaging.hub.address=dns:///kraken-hub:7000").run { context ->
            assertEquals(null, context.startupFailure)
        }
    }

    @Test fun `a malformed hub address or a zero status-interval fails at startup`() {
        for (address in listOf("dns:///kraken-hub", "dns:///hub-a:7000,hub-b:7000", " , ")) {
            val messages = startupFailure("kraken.messaging.hub.enabled=false", "kraken.messaging.hub.address=$address")
            assertTrue("kraken.messaging.hub.address" in messages, "$address: $messages")
        }
        val messages = startupFailure("kraken.messaging.hub.status-interval=0s")
        assertTrue("kraken.messaging.hub.status-interval must be positive" in messages, messages)
    }

    @Test fun `missing service-name fails binding`() {
        runner.withPropertyValues("kraken.messaging.type-prefix=kraken.orders").run { context ->
            val failure = assertNotNull(context.startupFailure)
            val messages = generateSequence<Throwable>(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
            assertTrue("kraken.messaging" in messages, messages)
            assertTrue("parameter serviceName" in messages, messages)
        }
    }

    @Test fun `events settings, the dead-letter mode and retention and the subscription prefix default and bind (P6a, ADR 0027)`() {
        runner.withPropertyValues(*required).run { context ->
            val p = context.getBean(KrakenMessagingProperties::class.java)
            assertEquals(null, p.events.subscriptionPrefix)
            assertEquals("orders", p.events.subscriptionPrefix("orders"))
            assertEquals(io.damoreira.kraken.messaging.deadletter.DeadLetterMode.COLLECTION, p.events.deadLetter.mode)
            assertEquals(Duration.ofDays(30), p.events.deadLetter.retention)
        }
        for (mode in listOf("event-store", "EVENT_STORE", "log")) {
            runner.withPropertyValues(*required, "kraken.messaging.events.dead-letter.mode=$mode", "kraken.messaging.events.dead-letter.retention=1s",
                "kraken.messaging.events.subscription-prefix=orders-v2", "kraken.messaging.events.source=orders.events",
                "kraken.messaging.event-store=kafka").run { context ->
                val p = context.getBean(KrakenMessagingProperties::class.java)
                assertEquals(if (mode == "log") "log" else "event-store", p.events.deadLetter.mode.wireName)
                assertEquals(Duration.ofSeconds(1), p.events.deadLetter.retention)
                assertEquals("orders-v2", p.events.subscriptionPrefix("orders"))
                assertEquals("orders.events", p.events.source)
                assertEquals("kafka", p.eventStore)
            }
        }
        runner.withPropertyValues(*required, "kraken.messaging.events.dead-letter.mode=topic").run { context ->
            assertNotNull(context.startupFailure, "the mode topic is now event-store")
        }
    }

    @Test fun `a dead-letter retention below 1 s or an unknown mode fails at startup (P6a)`() {
        runner.withPropertyValues(*required, "kraken.messaging.events.dead-letter.retention=999ms").run { context ->
            assertNotNull(context.startupFailure)
            assertTrue(generateSequence(context.startupFailure) { it.cause }.any { it.message?.contains("kraken.messaging.events.dead-letter.retention") == true })
        }
        runner.withPropertyValues(*required, "kraken.messaging.events.dead-letter.mode=queue").run { context ->
            assertNotNull(context.startupFailure)
        }
    }
}
