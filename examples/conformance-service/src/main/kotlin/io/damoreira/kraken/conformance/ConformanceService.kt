package io.damoreira.kraken.conformance

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.builder.SpringApplicationBuilder
import org.springframework.context.annotation.Bean
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtValidators
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder

/**
 * The Kotlin conformance service: the contract of kraken `protocol/conformance.md` (version 1) on kraken-messaging. The runner
 * (`kraken-conformance`) starts it once or more per check with the `CONFORMANCE_*` environment, drives it through a hub and
 * reads what it reports.
 *
 * It is written the way an application uses the library: typed messages, annotated handlers that throw, the library's buses.
 * Where the library falls short of a check, the service does not work around it: the runner reports the gap.
 */
@SpringBootApplication
class ConformanceService {
    /** Callers' tokens (the `authorization` header) validated against the runner's JWKS (RS256) and issuer, when given. */
    @Bean
    fun jwtDecoder(env: ConformanceEnv): JwtDecoder? {
        val jwks = env.jwksUrl ?: return null
        return NimbusJwtDecoder.withJwkSetUri(jwks).build().apply {
            val issuer = env.issuer
            setJwtValidator(if (issuer != null) JwtValidators.createDefaultWithIssuer(issuer) else DelegatingOAuth2TokenValidator(JwtValidators.createDefault()))
        }
    }

    @Bean
    fun conformanceEnv(): ConformanceEnv = ConformanceEnv.read(System.getenv())
}

/** The `CONFORMANCE_*` environment the runner sets (conformance.md, "Starting the service"); absent = not configured. */
data class ConformanceEnv(
    val service: String,
    val instance: String,
    val hubs: String,
    val capacity: Int,
    val maxSubscriptions: Int?,
    val creditTimeoutMs: Long?,
    val retry: Pair<Int, Long>?,
    val token: String?,
    val oauth2TokenUrl: String?,
    val oauth2ClientId: String?,
    val oauth2ClientSecret: String?,
    val tlsCa: String?,
    val tlsServerName: String?,
    val tlsClientCert: String?,
    val tlsClientKey: String?,
    val jwksUrl: String?,
    val issuer: String?,
) {
    /** The library's configuration (`kraken.messaging.*`) for this environment. */
    fun properties(): Map<String, Any> = buildMap {
        put("kraken.messaging.service-name", service)
        put("kraken.messaging.type-prefix", "conformance")
        put("kraken.messaging.instance-id", instance)
        put("kraken.messaging.hub.address", hubs)
        put("kraken.messaging.hub.max-concurrent-handlers", capacity)
        maxSubscriptions?.let { put("kraken.messaging.hub.max-subscriptions", it) }
        creditTimeoutMs?.let { put("kraken.messaging.hub.stream-credit-timeout", "${it}ms") }
        // The library's retry policy for every send.
        retry?.let { (attempts, backoffMs) ->
            put("kraken.messaging.hub.retry.enabled", true)
            put("kraken.messaging.hub.retry.max-attempts", attempts)
            put("kraken.messaging.hub.retry.initial-backoff", "${backoffMs}ms")
        }
        token?.let { put("kraken.messaging.hub.token", it) }
        oauth2TokenUrl?.let { url ->
            put("kraken.messaging.hub.oauth2.token-uri", url)
            oauth2ClientId?.let { put("kraken.messaging.hub.oauth2.client-id", it) }
            oauth2ClientSecret?.let { put("kraken.messaging.hub.oauth2.client-secret", it) }
        }
        if (tlsCa != null || tlsServerName != null || tlsClientCert != null) {
            put("kraken.messaging.hub.tls.enabled", true)
            tlsCa?.let { put("kraken.messaging.hub.tls.ca-file", it) }
            tlsServerName?.let { put("kraken.messaging.hub.tls.server-name", it) }
            tlsClientCert?.let { put("kraken.messaging.hub.tls.client-cert-file", it) }
            tlsClientKey?.let { put("kraken.messaging.hub.tls.client-key-file", it) }
        }
        // Level 1: commands, queries, streams and subscriptions; no event processor, no outbox, no saga.
        put("kraken.messaging.events.consumer.enabled", false)
        put("kraken.messaging.outbox.enabled", false)
        put("kraken.messaging.sagas.enabled", false)
        // No web server; the process lives until SIGTERM.
        put("spring.main.web-application-type", "none")
        put("spring.main.keep-alive", true)
        put("spring.main.banner-mode", "off")
        put("logging.level.io.damoreira.kraken", "DEBUG")
    }

    companion object {
        fun read(env: Map<String, String>): ConformanceEnv {
            fun v(name: String) = env[name]?.takeIf { it.isNotBlank() }?.trim()
            fun required(name: String) = v(name) ?: error("$name is required")
            fun int(name: String) = v(name)?.let { it.toIntOrNull() ?: error("$name is not a number: $it") }
            fun long(name: String) = v(name)?.let { it.toLongOrNull() ?: error("$name is not a number: $it") }
            return ConformanceEnv(
                service = required("CONFORMANCE_SERVICE_NAME"),
                instance = required("CONFORMANCE_INSTANCE_ID"),
                hubs = required("CONFORMANCE_HUBS"),
                capacity = int("CONFORMANCE_CAPACITY") ?: 64,
                maxSubscriptions = int("CONFORMANCE_MAX_SUBSCRIPTIONS"),
                creditTimeoutMs = long("CONFORMANCE_STREAM_CREDIT_TIMEOUT_MS"),
                retry = int("CONFORMANCE_RETRY_MAX_ATTEMPTS")?.let { it to (long("CONFORMANCE_RETRY_INITIAL_BACKOFF_MS") ?: 100) },
                token = v("CONFORMANCE_HUB_TOKEN"),
                oauth2TokenUrl = v("CONFORMANCE_OAUTH2_TOKEN_URL"),
                oauth2ClientId = v("CONFORMANCE_OAUTH2_CLIENT_ID"),
                oauth2ClientSecret = v("CONFORMANCE_OAUTH2_CLIENT_SECRET"),
                tlsCa = v("CONFORMANCE_TLS_CA"),
                tlsServerName = v("CONFORMANCE_TLS_SERVER_NAME"),
                tlsClientCert = v("CONFORMANCE_TLS_CLIENT_CERT"),
                tlsClientKey = v("CONFORMANCE_TLS_CLIENT_KEY"),
                jwksUrl = v("CONFORMANCE_JWKS_URL"),
                issuer = v("CONFORMANCE_ISSUER"),
            )
        }
    }
}

fun main(args: Array<String>) {
    val env = ConformanceEnv.read(System.getenv())
    SpringApplicationBuilder(ConformanceService::class.java)
        .properties(env.properties())
        .run(*args)
}
