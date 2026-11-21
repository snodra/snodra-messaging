package com.damoreira.snodra.messaging.metrics

import org.springframework.boot.EnvironmentPostProcessor
import org.springframework.boot.SpringApplication
import org.springframework.core.env.ConfigurableEnvironment
import org.springframework.core.env.MapPropertySource
import org.springframework.core.env.StandardEnvironment
import org.springframework.util.StringUtils
import java.nio.charset.StandardCharsets
import java.util.UUID

/**
 * The defaults that make Boot's OTLP metrics export what the dashboards query, as the last property source, so
 * any setting of the application wins. They apply to the whole application, its own meters and traces included:
 *
 * - `management.otlp.metrics.export.base-time-unit=seconds` (Boot's default is milliseconds): the library's timers
 *   ([MicrometerMessagingMetrics]) reach Prometheus as `..._seconds` series, as the Rust library's;
 * - `management.otlp.metrics.export.publish-max-gauge-for-histograms=false`: no `..._max_seconds` gauge next to each
 *   histogram, a series the Rust library does not export;
 * - the resource identity every series carries (Prometheus `job` and `instance`), set only where nothing else names it,
 *   because Boot lets `management.opentelemetry.resource-attributes.*` win over `OTEL_RESOURCE_ATTRIBUTES` and
 *   `OTEL_SERVICE_NAME`:
 *   - `service.instance.id` is `snodra.messaging.instance-id`, unless `OTEL_RESOURCE_ATTRIBUTES` names one. The instance
 *     id is resolved once here and written as a value (a `${random.uuid}` resolved twice would give the hub registration
 *     and the metrics two ids): the application's setting, else the `service.instance.id` of `OTEL_RESOURCE_ATTRIBUTES`,
 *     else a fresh UUID per start. So the hub registration and the metrics carry the same id.
 *   - `service.name` is `snodra.messaging.service-name` (the name the hub sees), unless the `OTEL_*` variables name the
 *     service or `spring.application.name` is set: then Boot's own choice stands, so the application's traces keep their
 *     name. Keep `spring.application.name` equal to the service name (or unset), or the metrics' `job` is not the
 *     `service` the hub's series carry.
 *
 * `spring.application.group` must stay unset: Boot turns it into `service.namespace`, which changes `job`.
 */
class MetricsDefaults : EnvironmentPostProcessor {
    override fun postProcessEnvironment(environment: ConfigurableEnvironment, application: SpringApplication) {
        val otel = otelResource(environment)
        val set = environment.getProperty(INSTANCE_ID)
        val instanceId = set ?: otel[SERVICE_INSTANCE_ID] ?: UUID.randomUUID().toString()
        if (set != null) {
            // Every later read (the hub registration's binding, the resource's) gets this value, not a second resolution.
            environment.propertySources.addFirst(MapPropertySource(RESOLVED, mapOf(INSTANCE_ID to instanceId)))
        }
        val defaults = mutableMapOf<String, Any>(
            BASE_TIME_UNIT to "seconds",
            "management.otlp.metrics.export.publish-max-gauge-for-histograms" to "false",
            INSTANCE_ID to instanceId,
        )
        if (SERVICE_INSTANCE_ID !in otel) defaults["$RESOURCE.$SERVICE_INSTANCE_ID"] = instanceId
        val serviceName = environment.getProperty(SERVICE_NAME_PROPERTY)
        if (serviceName != null && SERVICE_NAME !in otel && environment.getProperty("spring.application.name") == null) {
            defaults["$RESOURCE.$SERVICE_NAME"] = serviceName
        }
        environment.propertySources.addLast(MapPropertySource(NAME, defaults))
    }

    /** The resource attributes the `OTEL_*` variables give, read as Boot reads them (`OpenTelemetryResourceAttributes`). */
    private fun otelResource(environment: ConfigurableEnvironment): Map<String, String> {
        val variables = environment.propertySources.get(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME)
        fun variable(name: String) = variables?.getProperty(name)?.toString()
        val attributes = StringUtils.tokenizeToStringArray(variable("OTEL_RESOURCE_ATTRIBUTES"), ",")
            .mapNotNull { pair ->
                val at = pair.indexOf('=')
                if (at <= 0) return@mapNotNull null
                pair.substring(0, at).trim() to StringUtils.uriDecode(pair.substring(at + 1).trim(), StandardCharsets.UTF_8)
            }
            .filter { it.second.isNotEmpty() }
            .toMap()
        return attributes + listOfNotNull(variable("OTEL_SERVICE_NAME")?.let { SERVICE_NAME to it })
    }

    companion object {
        const val NAME = "snodraMessagingMetricsDefaults"
        const val BASE_TIME_UNIT = "management.otlp.metrics.export.base-time-unit"
        const val INSTANCE_ID = "snodra.messaging.instance-id"
        private const val RESOLVED = "snodraMessagingInstanceId"
        private const val SERVICE_NAME_PROPERTY = "snodra.messaging.service-name"
        private const val RESOURCE = "management.opentelemetry.resource-attributes"
        private const val SERVICE_NAME = "service.name"
        private const val SERVICE_INSTANCE_ID = "service.instance.id"
    }
}
