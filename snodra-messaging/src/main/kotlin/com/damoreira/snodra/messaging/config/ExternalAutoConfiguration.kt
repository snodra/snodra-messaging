package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.external.ExternalCallSettings
import com.damoreira.snodra.messaging.external.ExternalEventIngress
import com.damoreira.snodra.messaging.external.ExternalRecordTranslator
import com.damoreira.snodra.messaging.external.ExternalSourceProvider
import com.damoreira.snodra.messaging.external.ExternalSourceProviders
import com.damoreira.snodra.messaging.external.ExternalSources
import com.damoreira.snodra.messaging.external.IdempotencyInterceptor
import com.damoreira.snodra.messaging.external.WebhookSignatureVerifier
import com.damoreira.snodra.messaging.saga.NoopSagaMetrics
import com.damoreira.snodra.messaging.saga.SagaMetrics
import com.damoreira.snodra.messaging.store.MessageStore
import org.springframework.beans.factory.ListableBeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
import java.time.Clock

/**
 * The integration toolkit's edges, on top of [EventsAutoConfiguration]'s local dispatcher:
 *
 * - the [WebhookSignatureVerifier] and the [ExternalEventIngress], always (they are passive: a service uses them from its own endpoint);
 * - [ExternalSources], only when an event-store add-on provides partner sources ([ExternalSourceProviders]: with
 *   `snodra-messaging-kafka`, `snodra.messaging.external.kafka.sources.<name>`); each source needs an [ExternalRecordTranslator]
 *   bean named `<name>`, else the startup fails naming the source;
 * - the [IdempotencyInterceptor] over the message store's external calls (which `HandlerInvoker` then uses for
 *   `@IdempotentHandler` methods), only when the application has an `@IdempotentHandler` method (scanned from its packages)
 *   or `snodra.messaging.external.calls.enabled=true`.
 *
 * All of it needs a [MessageStore] (and the local dispatcher over it).
 */
@AutoConfiguration(after = [SnodraMessagingAutoConfiguration::class, EventsAutoConfiguration::class])
@ConditionalOnBean(MessageStore::class, LocalEventDispatcher::class)
class ExternalAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun webhookSignatureVerifier() = WebhookSignatureVerifier()

    @Bean
    @ConditionalOnMissingBean
    fun externalEventIngress(
        dispatcher: LocalEventDispatcher,
        store: MessageStore,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        clock: ObjectProvider<Clock>,
    ) = ExternalEventIngress(dispatcher, store.processedEvents, codec, typeResolver, clock.getIfAvailable { Clock.systemUTC() })

    @Bean
    @ConditionalOnMissingBean
    @ConditionalOnBean(ExternalSourceProviders::class)
    fun externalSources(
        providers: List<ExternalSourceProviders>,
        properties: SnodraMessagingProperties,
        beans: ListableBeanFactory,
        dispatcher: LocalEventDispatcher,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
    ): ExternalSources {
        val sources = LinkedHashMap<String, ExternalSourceProvider>()
        for ((name, provider) in providers.flatMap { it.providers().entries }) {
            check(sources.put(name, provider) == null) { "external source '$name' is configured twice: source names must be unique" }
        }
        return ExternalSources(
            sources = sources,
            translators = beans.getBeansOfType(ExternalRecordTranslator::class.java), // bean name -> translator: a source `printer` needs the bean named `printer`
            dispatcher = dispatcher,
            codec = codec,
            typeResolver = typeResolver,
            maxAttempts = properties.events.consumer.maxAttempts,
        )
    }

    @Configuration(proxyBeanMethods = false)
    @Conditional(OnIdempotentHandlers::class)
    @Import(SnodraClockConfiguration::class)
    class IdempotentHandlersConfiguration {
        /** Its bookkeeping writes run in transactions of their own (REQUIRES_NEW), so a rolled-back caller never forgets a call that happened. */
        @Bean
        @ConditionalOnMissingBean
        fun idempotencyInterceptor(
            store: MessageStore,
            codec: JsonCodec,
            clock: Clock,
            properties: SnodraMessagingProperties,
            metrics: ObjectProvider<SagaMetrics>,
        ) = IdempotencyInterceptor(
            store, codec, clock, ExternalCallSettings(properties.external.calls.lease), metrics.getIfAvailable { NoopSagaMetrics },
        )
    }
}
