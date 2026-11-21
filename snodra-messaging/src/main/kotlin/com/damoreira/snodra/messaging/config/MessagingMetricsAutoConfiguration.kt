package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.events.ProcessorStatusSource
import com.damoreira.snodra.messaging.metrics.MessagingMetrics
import com.damoreira.snodra.messaging.metrics.MicrometerMessagingMetrics
import com.damoreira.snodra.messaging.metrics.EventProcessorMetrics
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean

/**
 * The Micrometer meters of the outbox relay, the event processors and the hub client: only when Micrometer is
 * on the classpath and a `MeterRegistry` bean exists. [MicrometerMessagingMetrics] is the [MessagingMetrics] the components
 * report to (an application's own bean replaces it; without a registry they get
 * [com.damoreira.snodra.messaging.metrics.NoopMessagingMetrics]), and [EventProcessorMetrics] samples the processors' gauges when
 * event processors run here. The hub connection gauges are a `MeterBinder` of [HubAutoConfiguration].
 *
 * Ordered after the configurations whose beans its conditions look at and after Boot's metrics configuration (which defines
 * the registry).
 */
@AutoConfiguration(
    after = [EventsAutoConfiguration::class, HubAutoConfiguration::class],
    afterName = [
        "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration",
    ],
)
@ConditionalOnClass(MeterRegistry::class)
@ConditionalOnBean(MeterRegistry::class)
class MessagingMetricsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(MessagingMetrics::class)
    fun snodraMessagingMetrics(registry: MeterRegistry) = MicrometerMessagingMetrics(registry)

    /** The processor gauges, sampled every 5 s from the status the instance reports to the hub. */
    @Bean
    @ConditionalOnBean(ProcessorStatusSource::class)
    @ConditionalOnMissingBean
    fun snodraEventProcessorMetrics(registry: MeterRegistry, source: ProcessorStatusSource) = EventProcessorMetrics(registry, source)
}
