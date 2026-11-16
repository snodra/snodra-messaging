package io.damoreira.kraken.conformance

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse

/** Every `CONFORMANCE_*` variable onto the library's configuration; an absent one is not configured. */
class ConformanceEnvTest {
    private val base = mapOf(
        "CONFORMANCE_SERVICE_NAME" to "conformance-kotlin",
        "CONFORMANCE_INSTANCE_ID" to "i-1",
        "CONFORMANCE_HUBS" to "127.0.0.1:7000,127.0.0.1:7100",
    )

    @Test
    fun `every variable maps onto the library's settings`() {
        val properties = ConformanceEnv.read(
            base + mapOf(
                "CONFORMANCE_CAPACITY" to "4",
                "CONFORMANCE_MAX_SUBSCRIPTIONS" to "2",
                "CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS" to "2000",
                "CONFORMANCE_RETRY_MAX_ATTEMPTS" to "3",
                "CONFORMANCE_RETRY_INITIAL_BACKOFF_MS" to "50",
                "CONFORMANCE_HUB_TOKEN" to "t0ken",
                "CONFORMANCE_OAUTH2_TOKEN_URL" to "http://127.0.0.1:9/token",
                "CONFORMANCE_OAUTH2_CLIENT_ID" to "conformance-kotlin",
                "CONFORMANCE_OAUTH2_CLIENT_SECRET" to "s3cret",
                "CONFORMANCE_TLS_CA" to "/ca.pem",
                "CONFORMANCE_TLS_SERVER_NAME" to "hub.test",
                "CONFORMANCE_TLS_CLIENT_CERT" to "/c.pem",
                "CONFORMANCE_TLS_CLIENT_KEY" to "/k.pem",
            ),
        ).properties()
        val expected = mapOf(
            "kraken.messaging.service-name" to "conformance-kotlin",
            "kraken.messaging.instance-id" to "i-1",
            "kraken.messaging.hub.address" to "127.0.0.1:7000,127.0.0.1:7100",
            "kraken.messaging.hub.max-concurrent-handlers" to 4,
            "kraken.messaging.hub.max-subscriptions" to 2,
            "kraken.messaging.hub.stream-credit-timeout" to "2000ms",
            "kraken.messaging.hub.retry.enabled" to true,
            "kraken.messaging.hub.retry.max-attempts" to 3,
            "kraken.messaging.hub.retry.initial-backoff" to "50ms",
            "kraken.messaging.hub.token" to "t0ken",
            "kraken.messaging.hub.oauth2.token-uri" to "http://127.0.0.1:9/token",
            "kraken.messaging.hub.oauth2.client-id" to "conformance-kotlin",
            "kraken.messaging.hub.oauth2.client-secret" to "s3cret",
            "kraken.messaging.hub.tls.enabled" to true,
            "kraken.messaging.hub.tls.ca-file" to "/ca.pem",
            "kraken.messaging.hub.tls.server-name" to "hub.test",
            "kraken.messaging.hub.tls.client-cert-file" to "/c.pem",
            "kraken.messaging.hub.tls.client-key-file" to "/k.pem",
            "kraken.messaging.events.consumer.enabled" to false,
            "kraken.messaging.outbox.enabled" to false,
        )
        for ((key, value) in expected) assertEquals(value, properties[key], key)
    }

    @Test
    fun `an absent variable is not configured`() {
        val env = ConformanceEnv.read(base + ("CONFORMANCE_HUB_TOKEN" to " "))
        val properties = env.properties()
        for (key in listOf("hub.token", "hub.retry.enabled", "hub.tls.enabled", "hub.oauth2.token-uri", "hub.max-subscriptions")) {
            assertFalse("kraken.messaging.$key" in properties, key)
        }
        assertEquals(null, env.jwksUrl)
        assertFailsWith<IllegalStateException> { ConformanceEnv.read(base - "CONFORMANCE_HUBS") }
    }
}
