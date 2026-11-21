package com.damoreira.snodra.messaging.metrics

import org.junit.jupiter.api.Test
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import org.springframework.core.env.SystemEnvironmentPropertySource
import org.springframework.boot.env.RandomValuePropertySource
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * The metrics export defaults: seconds as the OTLP base time unit, so the library's timers are `..._seconds`
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

    @Test fun `histograms get no max gauge, a series the Rust library does not have`() {
        val env = StandardEnvironment()
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertEquals("false", env.getProperty("management.otlp.metrics.export.publish-max-gauge-for-histograms"))
    }

    @Test fun `an application's own setting wins`() {
        val env = StandardEnvironment()
        env.propertySources.addFirst(MapPropertySource("app", mapOf(key to "milliseconds")))
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertEquals("milliseconds", env.getProperty(key))
    }

    private val instanceId = "snodra.messaging.instance-id"
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
        env.propertySources.addFirst(MapPropertySource("app", mapOf(instanceId to "pod-7", "snodra.messaging.service-name" to "orders")))
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertEquals("pod-7", env.getProperty(resourceId))
        assertEquals("orders", env.getProperty(resourceName))
    }

    @Test fun `with an application name the service name is left to Boot, so the application's traces keep their name`() {
        val env = StandardEnvironment()
        env.propertySources.addFirst(
            MapPropertySource("app", mapOf("snodra.messaging.service-name" to "orders", "spring.application.name" to "orders-app")),
        )
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertNull(env.getProperty(resourceName), "Boot's own default, spring.application.name, applies")
    }

    /** An environment whose variables are [vars] (Boot reads OTEL_* from the system environment). */
    private fun withVariables(vararg vars: Pair<String, String>) = StandardEnvironment().apply {
        propertySources.replace(
            StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
            SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, mapOf(*vars)),
        )
        propertySources.addFirst(MapPropertySource("app", mapOf("snodra.messaging.service-name" to "orders")))
    }

    @Test fun `the OTEL variables win, no default where they name the service or the instance, and the hub sees their instance`() {
        val env = withVariables("OTEL_RESOURCE_ATTRIBUTES" to "deployment.environment=prod,service.instance.id=pod-9", "OTEL_SERVICE_NAME" to "orders-otel")
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        assertNull(env.getProperty(resourceName))
        assertNull(env.getProperty(resourceId))
        assertEquals("pod-9", env.getProperty(instanceId), "the hub registration carries the instance the metrics name")

        val named = withVariables("OTEL_RESOURCE_ATTRIBUTES" to "service.name=orders-otel")
        MetricsDefaults().postProcessEnvironment(named, SpringApplication())
        assertNull(named.getProperty(resourceName))
        assertEquals(named.getProperty(instanceId), named.getProperty(resourceId))
    }

    @Test fun `an instance id given as a random value is resolved once, the same for the hub and the metrics`() {
        val env = StandardEnvironment()
        env.propertySources.addLast(RandomValuePropertySource())
        env.propertySources.addFirst(MapPropertySource("app", mapOf(instanceId to "\${random.uuid}")))
        MetricsDefaults().postProcessEnvironment(env, SpringApplication())
        val id = env.getProperty(instanceId)!!
        java.util.UUID.fromString(id)
        assertEquals(id, env.getProperty(instanceId), "read again: the same")
        assertEquals(id, env.getProperty(resourceId))
        val raw = env.propertySources.get(MetricsDefaults.NAME)!!.getProperty(resourceId)
        assertEquals(id, raw, "the resolved value, not a placeholder")
    }

    @Configuration(proxyBeanMethods = false)
    class Empty

    @Test fun `it is registered for every application`() {
        SpringApplicationBuilder(Empty::class.java).web(WebApplicationType.NONE)
            .properties("spring.main.banner-mode=off")
            .run().use { assertEquals("seconds", it.environment.getProperty(key)) }
    }
}
