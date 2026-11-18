package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.saga.MicrometerSagaMetrics
import com.damoreira.snodra.messaging.saga.SagaGaugeSampler
import com.damoreira.snodra.messaging.saga.SagaMetrics
import com.damoreira.snodra.messaging.saga.SagaRuntime
import com.damoreira.snodra.messaging.store.MessageStore
import io.micrometer.core.instrument.MeterRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Conditional

/**
 * The Micrometer meters of the saga toolkit ([MicrometerSagaMetrics], and the [SagaGaugeSampler] with sagas): only when
 * Micrometer is on the classpath, a `MeterRegistry` bean exists and the application uses the toolkit (a saga runtime or an
 * idempotency interceptor bean). Without a registry the runtime, relay, scheduler and interceptor get no metrics
 * ([com.damoreira.snodra.messaging.saga.NoopSagaMetrics]); an application's own [SagaMetrics] bean replaces the Micrometer one.
 *
 * Ordered after the saga and external configurations (whose beans its conditions look at) and after Boot's metrics
 * configuration (which defines the registry).
 */
@AutoConfiguration(
    after = [SagaAutoConfiguration::class, ExternalAutoConfiguration::class],
    afterName = [
        "org.springframework.boot.micrometer.metrics.autoconfigure.MetricsAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration",
        "org.springframework.boot.micrometer.metrics.autoconfigure.export.simple.SimpleMetricsExportAutoConfiguration",
    ],
)
@ConditionalOnClass(MeterRegistry::class)
@ConditionalOnBean(MeterRegistry::class)
@Conditional(OnSagaToolkitBeans::class)
class SagaMetricsAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(SagaMetrics::class)
    fun micrometerSagaMetrics(registry: MeterRegistry, catalog: ObjectProvider<SagaCatalog>) =
        MicrometerSagaMetrics(registry, catalog.getIfAvailable()?.types?.map { it.name }.orEmpty())

    /** Samples the two store-backed gauges; only with sagas, and only when the metrics are ours. */
    @Bean
    @ConditionalOnBean(SagaRuntime::class, MicrometerSagaMetrics::class)
    @ConditionalOnMissingBean
    fun sagaGaugeSampler(store: MessageStore, metrics: MicrometerSagaMetrics) = SagaGaugeSampler(store.sagas, metrics)
}
