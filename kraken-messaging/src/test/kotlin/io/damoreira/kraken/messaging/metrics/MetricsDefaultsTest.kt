package io.damoreira.kraken.messaging.metrics

import org.junit.jupiter.api.Test
import org.springframework.boot.SpringApplication
import org.springframework.boot.WebApplicationType
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import kotlin.test.assertEquals

/** The OTLP export's base time unit defaults to seconds, so the library's timers are `..._seconds` series (P9, ADR 0026). */
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

    @Configuration(proxyBeanMethods = false)
    class Empty

    @Test fun `it is registered for every application`() {
        SpringApplicationBuilder(Empty::class.java).web(WebApplicationType.NONE)
            .properties("spring.main.banner-mode=off")
            .run().use { assertEquals("seconds", it.environment.getProperty(key)) }
    }
}
