package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.metrics.MetricsDefaults
import io.micrometer.registry.otlp.OtlpConfig
import io.micrometer.registry.otlp.OtlpMeterRegistry
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration
import org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration
import org.springframework.boot.micrometer.metrics.autoconfigure.export.otlp.OtlpMetricsExportAutoConfiguration
import org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration
import org.springframework.boot.opentelemetry.autoconfigure.OpenTelemetrySdkAutoConfiguration
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import java.util.concurrent.TimeUnit
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The service's metrics go out over OTLP when `KRAKEN_OTLP_METRICS_ENABLED` is set, and nowhere (no error) otherwise (P9). */
class OtlpMetricsExportTest {
    private val runner = ApplicationContextRunner()
        .withInitializer(ConfigDataApplicationContextInitializer()) // this service's application.yaml
        .withInitializer { MetricsDefaults().postProcessEnvironment(it.environment, SpringApplication()) } // as at a real start
        .withConfiguration(
            AutoConfigurations.of(
                OpenTelemetrySdkAutoConfiguration::class.java,
                MetricsAutoConfiguration::class.java,
                CompositeMeterRegistryAutoConfiguration::class.java,
                OtlpMetricsExportAutoConfiguration::class.java,
                SimpleMetricsExportAutoConfiguration::class.java,
            ),
        )

    @Test fun `without the switch nothing is exported`() {
        runner.run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(context.getBeansOfType(OtlpMeterRegistry::class.java).isEmpty())
        }
    }

    @Test fun `with it the metrics go to the endpoint in seconds, as the service and instance the hub sees`() {
        runner.withPropertyValues(
            "KRAKEN_OTLP_METRICS_ENABLED=true",
            "KRAKEN_OTLP_METRICS_ENDPOINT=http://otel-lgtm:4318/v1/metrics",
            "kraken.messaging.instance-id=b-1",
        ).run { context ->
            assertEquals(null, context.startupFailure)
            assertEquals(1, context.getBeansOfType(OtlpMeterRegistry::class.java).size)
            val config = context.getBean(OtlpConfig::class.java)
            assertEquals("http://otel-lgtm:4318/v1/metrics", config.url())
            assertEquals(TimeUnit.SECONDS, config.baseTimeUnit())
            assertEquals("service-b", config.resourceAttributes()["service.name"])
            assertEquals("b-1", config.resourceAttributes()["service.instance.id"])
            assertTrue("service.namespace" !in config.resourceAttributes())
        }
    }
}
