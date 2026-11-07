package io.damoreira.kraken.messaging.metrics

import org.junit.jupiter.api.Test
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The metrics export defaults (P9, ADR 0026): seconds as the OTLP base time unit, so the library's timers are `..._seconds`
 * series, and the resource identity the dashboards aggregate on: `service.name` the service name the hub sees,
 * `service.instance.id` the instance id the hub sees.
 */
class MetricsDefaultsTest {
    private val key = "management.otlp.metrics.export.base-time-unit"

    @Test fun `the base time unit defaults to seconds`() {
        val env = StandardEnvironment()
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertEquals("seconds", env.getProperty(key))
    }

    @Test fun `an application's own setting wins`() {
        val env = StandardEnvironment()
        env.propertySources.addFirst(MapPropertySource("app", mapOf(key to "milliseconds")))
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertEquals("milliseconds", env.getProperty(key))
    }

    private val instanceId = "kraken.messaging.instance-id"
    private val resourceId = "management.opentelemetry.resource-attributes.service.instance.id"
    private val resourceName = "management.opentelemetry.resource-attributes.service.name"

    @Test fun `every start gets a fresh instance id, which is the resource's service instance id`() {
        val env = StandardEnvironment()
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        val id = env.getProperty(instanceId)!!
        java.util.UUID.fromString(id)
        assertEquals(id, env.getProperty(resourceId))
        val other = StandardEnvironment().also { MetricsDefaults().postProcessEnvironment(it, SpringApplication()) }
        assertTrue(other.getProperty(instanceId) != id)
    }

    @Test fun `an instance id the application sets is the resource's too, and the service name is the one the hub sees`() {
        val env = StandardEnvironment()
        env.propertySources.addFirst(
            MapPropertySource("app", mapOf(instanceId to "pod-7", "kraken.messaging.service-name" to "orders", "spring.application.name" to "orders-app")),
        )
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertEquals("pod-7", env.getProperty(resourceId))
        assertEquals("orders", env.getProperty(resourceName))
    }

    @Test fun `without a kraken service name the resource keeps Boot's, the application name`() {
        val env = StandardEnvironment()
        env.propertySources.addFirst(MapPropertySource("app", mapOf("spring.application.name" to "orders-app")))
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertEquals("orders-app", env.getProperty(resourceName))
    }

    @Configuration(proxyBeanMethods = false)
    class Empty

    @Test fun `it is registered for every application`() {
        SpringApplicationBuilder(Empty::class.java).web(WebApplicationType.NONE)
            .properties("spring.main.banner-mode=off")
            .run().use { assertEquals("seconds", it.environment.getProperty(key)) }
    }
}
