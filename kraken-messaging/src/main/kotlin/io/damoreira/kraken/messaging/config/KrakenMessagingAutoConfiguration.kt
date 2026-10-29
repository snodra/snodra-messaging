package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.HandlerBeanPostProcessor
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
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

    @Bean
    @ConditionalOnMissingBean
    fun handlerInvoker(registry: HandlerRegistry, codec: JsonCodec) = HandlerInvoker(registry, codec)

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
