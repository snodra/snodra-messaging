package io.damoreira.kraken.messaging.metrics

import org.springframework.boot.EnvironmentPostProcessor
import org.springframework.boot.SpringApplication
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource
import java.util.UUID

/**
 * The defaults that make Boot's OTLP metrics export what the dashboards query (ADR 0026), as the last property source, so
 * any setting of the application wins:
 *
 * - `management.otlp.metrics.export.base-time-unit=seconds` (Boot's default is milliseconds): the library's timers
 *   ([MicrometerMessagingMetrics]) reach Prometheus as `..._seconds` series, as the Rust library's;
 * - the resource identity every series carries (Prometheus `job` and `instance`): `service.name` is
 *   `kraken.messaging.service-name` (the name the hub sees; Boot's own default is `spring.application.name`) and
 *   `service.instance.id` is `kraken.messaging.instance-id`, which gets a fresh UUID here per start, so the hub
 *   registration and the metrics carry the same id.
 *
 * `spring.application.group` must stay unset: Boot turns it into `service.namespace`, which changes `job`.
 */
class MetricsDefaults : EnvironmentPostProcessor {
    override fun postProcessEnvironment(environment: ConfigurableEnvironment, application: SpringApplication) {
        environment.propertySources.addLast(
            MapPropertySource(
                NAME,
                mapOf(
                    BASE_TIME_UNIT to "seconds",
                    INSTANCE_ID to UUID.randomUUID().toString(),
                    "$RESOURCE.service.instance.id" to "\${$INSTANCE_ID}",
                    "$RESOURCE.service.name" to "\${kraken.messaging.service-name:\${spring.application.name:unknown_service}}",
                ),
            ),
        )
    }

    companion object {
        const val NAME = "krakenMessagingMetricsDefaults"
        const val BASE_TIME_UNIT = "management.otlp.metrics.export.base-time-unit"
        const val INSTANCE_ID = "kraken.messaging.instance-id"
        private const val RESOURCE = "management.opentelemetry.resource-attributes"
    }
}
