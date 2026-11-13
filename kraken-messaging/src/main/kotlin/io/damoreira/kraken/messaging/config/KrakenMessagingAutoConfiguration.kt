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
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.ObjectProvider
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
 * forwarded-JWT validation. The hub and Kafka transports are wired by [HubAutoConfiguration] and
 * [KafkaEventsAutoConfiguration].
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
    fun krakenMessageStoreGuard(beanFactory: BeanFactory, registry: HandlerRegistry, environment: Environment, resourceLoader: ResourceLoader) =
        MessageStoreGuard(beanFactory, registry, environment, resourceLoader)

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

    /** Fails startup when a removed property is still configured (runs regardless of user-supplied beans). */
    @Bean
    fun krakenRemovedPropertiesGuard(environment: Environment): RemovedPropertiesGuard = RemovedPropertiesGuard(environment)

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
    }
}

/** Rejects configuration keys that no longer exist, with a hint about their replacement. */
class RemovedPropertiesGuard(environment: Environment) {
    init {
        check(!Binder.get(environment).bind(REQUIRE_JWT, String::class.java).isBound) {
            "$REQUIRE_JWT was removed; requests are trusted by default, annotate handlers with @RequiresAuthentication"
        }
    }

    companion object {
        const val REQUIRE_JWT = "kraken.messaging.security.require-jwt"
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
