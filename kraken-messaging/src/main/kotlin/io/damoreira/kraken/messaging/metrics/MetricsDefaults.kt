package io.damoreira.kraken.messaging.metrics

import org.springframework.boot.EnvironmentPostProcessor
import org.springframework.boot.SpringApplication
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource

/**
 * Makes Boot's OTLP metrics export use seconds as its base time unit unless the application sets it (Boot's default is
 * milliseconds): the library's timers ([MicrometerMessagingMetrics]) then reach Prometheus as `..._seconds` series, the
 * names the dashboards query and the Rust library exports (ADR 0026). Added as the last property source, so any setting
 * of the application wins.
 */
class MetricsDefaults : EnvironmentPostProcessor {
    override fun postProcessEnvironment(environment: ConfigurableEnvironment, application: SpringApplication) {
        environment.propertySources.addLast(MapPropertySource(NAME, mapOf(BASE_TIME_UNIT to "seconds")))
    }

    companion object {
        const val NAME = "krakenMessagingMetricsDefaults"
        const val BASE_TIME_UNIT = "management.otlp.metrics.export.base-time-unit"
    }
}
