package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.QueryUpdateEmitter
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.external.IdempotencyInterceptor
import io.damoreira.kraken.messaging.handler.HandlerBeanPostProcessor
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubQueryUpdateEmitter
import io.damoreira.kraken.messaging.hub.SubscriptionRegistry
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import org.springframework.beans.factory.ListableBeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.boot.actuate.endpoint.SanitizingFunction
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
import org.springframework.core.io.ResourceLoader
import org.springframework.context.annotation.Configuration
import org.springframework.security.oauth2.jwt.JwtDecoder

/**
 * Transport-independent beans: type names, JSON codec, handler discovery and invocation, trace header injection and
 * forwarded-JWT validation. The hub transport and the events are wired by [HubAutoConfiguration] and
 * [EventsAutoConfiguration].
 */
@AutoConfiguration
@EnableConfigurationProperties(KrakenMessagingProperties::class)
class KrakenMessagingAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun messageTypeResolver(properties: KrakenMessagingProperties) = MessageTypeResolver(properties.typePrefix)

    @Bean
    @ConditionalOnMissingBean
    fun jsonCodec() = JsonCodec()

    @Bean
    @ConditionalOnMissingBean
    fun handlerRegistry(typeResolver: MessageTypeResolver) = HandlerRegistry(typeResolver)

    /** Fails the startup when an event handler, a saga or an idempotent handler has no message store to run on (ADR 0028). */
    @Bean
    fun krakenMessageStoreGuard(beanFactory: ListableBeanFactory, registry: HandlerRegistry, environment: Environment, resourceLoader: ResourceLoader) =
        MessageStoreGuard(beanFactory, registry, environment, resourceLoader)

    /** Fails the startup when an event handler or a saga has no event store to read from, or two event stores compete (ADR 0027). */
    @Bean
    fun krakenEventStoreGuard(beanFactory: ListableBeanFactory, registry: HandlerRegistry, environment: Environment, resourceLoader: ResourceLoader) =
        EventStoreGuard(beanFactory, registry, environment, resourceLoader)

    /** Applies the message store's schema at startup (`kraken.messaging.store.init-schema`, on by default). */
    @Bean
    fun krakenMessageStoreSchema(store: ObjectProvider<MessageStore>, properties: KrakenMessagingProperties) = MessageStoreSchema(store, properties)

    @Bean
    @ConditionalOnMissingBean
    fun handlerInvoker(registry: HandlerRegistry, codec: JsonCodec, idempotency: ObjectProvider<IdempotencyInterceptor>) =
        HandlerInvoker(registry, codec, idempotency.getIfAvailable()) // the interceptor exists with an @IdempotentHandler (ExternalAutoConfiguration)

    /**
     * The subscription queries this instance serves (P4c): filled by the hub connections, read by [queryUpdateEmitter].
     * Transport-independent on purpose: with the hub disabled it simply stays empty and emitting is a no-op.
     */
    @Bean
    @ConditionalOnMissingBean
    fun subscriptionRegistry(properties: KrakenMessagingProperties) = SubscriptionRegistry(properties.hub.maxSubscriptions)

    /** Pushes updates to subscription queries; inside a Spring transaction it waits for the commit. */
    @Bean
    @ConditionalOnMissingBean
    fun queryUpdateEmitter(registry: SubscriptionRegistry, codec: JsonCodec, typeResolver: MessageTypeResolver): QueryUpdateEmitter =
        HubQueryUpdateEmitter(registry, codec, typeResolver)

    @Bean
    @ConditionalOnMissingBean
    fun forwardedIdentityResolver(decoder: ObjectProvider<JwtDecoder>) = ForwardedIdentityResolver(decoder.ifAvailable)

    /** Fallback when micrometer-tracing is absent: no trace headers. */
    @Bean
    @ConditionalOnMissingBean
    fun noopTraceHeaders() = TraceHeaders(null, null)

    /** Injects W3C trace headers when micrometer-tracing is on the classpath and its beans exist. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["io.micrometer.tracing.Tracer", "io.micrometer.tracing.propagation.Propagator"])
    class MicrometerTraceHeadersConfiguration {
        @Bean
        @ConditionalOnMissingBean
        fun traceHeaders(propagator: ObjectProvider<Propagator>, tracer: ObjectProvider<Tracer>) =
            TraceHeaders(propagator.ifAvailable, tracer.ifAvailable)
    }

    /**
     * Masks the hub token and the OAuth2 client secret in the actuator's `configprops` and `env` output whatever `show-values` is, and whichever
     * sanitization rules the application set (Spring Boot 4 has no `keys-to-sanitize`: a [SanitizingFunction] bean is the way).
     */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["org.springframework.boot.actuate.endpoint.SanitizingFunction"])
    class HubTokenSanitizingConfiguration {
        @Bean
        fun krakenHubTokenSanitizingFunction(): SanitizingFunction = SanitizingFunction { data ->
            if (HubTokenKeys.matches(data.key)) data.withSanitizedValue() else data
        }
    }

    companion object {
        /** Static so it is registered before other beans without instantiating this configuration; registry is resolved lazily. */
        @JvmStatic
        @Bean
        fun krakenHandlerBeanPostProcessor(registry: ObjectProvider<HandlerRegistry>) =
            HandlerBeanPostProcessor { registry.getObject() }

        /** Two message store add-ons without `kraken.messaging.store.type` fail the startup before any bean exists. */
        @JvmStatic
        @Bean
        fun krakenMessageStoreAddOnsCheck(environment: Environment) = MessageStoreAddOnsCheck(environment)

        /**
         * A removed setting still configured fails the startup before any bean exists (whatever beans the application supplies), naming
         * its replacement: silently ignoring a renamed group or source would re-handle or misroute events.
         */
        @JvmStatic
        @Bean
        fun krakenRemovedPropertiesGuard(environment: Environment) = RemovedPropertiesGuard(environment)

        /** Two event-store add-ons without `kraken.messaging.event-store` fail the startup before any bean exists. */
        @JvmStatic
        @Bean
        fun krakenEventStoreAddOnsCheck(environment: Environment) = EventStoreAddOnsCheck(environment)
    }
}

/**
 * Rejects settings that no longer exist, all of them in one message, each with its replacement. No alias: a removed name is never
 * read (ADR 0024).
 */
class RemovedPropertiesGuard(private val environment: Environment) : BeanFactoryPostProcessor {
    override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
        val problems = problems()
        check(problems.isEmpty()) { problems.joinToString("; ") }
    }

    /** One line per removed setting still set, with what replaces it. */
    private fun problems(): List<String> {
        val binder = Binder.get(environment)
        fun value(name: String): String? = binder.bind(name, String::class.java).orElse(null)
        val problems = mutableListOf<String>()
        if (value(REQUIRE_JWT) != null) {
            problems += "$REQUIRE_JWT was removed; requests are trusted by default, annotate handlers with @RequiresAuthentication"
        }
        RENAMED.forEach { (removed, replacement) ->
            val old = value(removed) ?: return@forEach
            problems += "$removed was removed, use $replacement" +
                if (removed == KAFKA_DEAD_LETTER_MODE && old.isTopic()) " (the mode topic is now event-store)" else ""
        }
        if (value(DEAD_LETTER_MODE)?.isTopic() == true) problems += "$DEAD_LETTER_MODE=topic was removed, use event-store"
        return problems
    }

    private fun String.isTopic() = trim().equals("topic", ignoreCase = true)

    companion object {
        const val REQUIRE_JWT = "kraken.messaging.security.require-jwt"
        private const val KAFKA_DEAD_LETTER_MODE = "kraken.messaging.kafka.dead-letter.mode"
        private const val DEAD_LETTER_MODE = "kraken.messaging.events.dead-letter.mode"

        /** The settings removed in v0.4.0 (the event store became neutral, ADR 0027), with their replacements. */
        internal val RENAMED: Map<String, String> = linkedMapOf(
            "kraken.messaging.kafka.topic" to "kraken.messaging.events.source",
            "kraken.messaging.kafka.group-id" to "kraken.messaging.events.subscription-prefix",
            "kraken.messaging.kafka.consumer.enabled" to "kraken.messaging.events.consumer.enabled",
            "kraken.messaging.kafka.consumer.max-attempts" to "kraken.messaging.events.consumer.max-attempts",
            KAFKA_DEAD_LETTER_MODE to DEAD_LETTER_MODE,
            "kraken.messaging.kafka.dead-letter.retention" to "kraken.messaging.events.dead-letter.retention",
        )
    }
}

/**
 * The names the hub token and the OAuth2 client secret go by: the properties, their relaxed environment forms and the plain
 * `KRAKEN_HUB_TOKEN` / `KRAKEN_HUB_OAUTH2_CLIENT_SECRET` (the secret's file is a path, not a secret, and stays visible).
 */
internal object HubTokenKeys {
    // Separators are dropped before comparing: the property is `client-secret`, the environment variable `CLIENT_SECRET` and the
    // configprops report says `clientSecret`.
    private val secrets = setOf(
        "krakenmessaginghubtoken", "krakenhubtoken",
        "krakenmessaginghuboauth2clientsecret", "krakenhuboauth2clientsecret",
    )

    fun matches(key: String?): Boolean = key?.lowercase()?.filter { it.isLetterOrDigit() } in secrets
}
