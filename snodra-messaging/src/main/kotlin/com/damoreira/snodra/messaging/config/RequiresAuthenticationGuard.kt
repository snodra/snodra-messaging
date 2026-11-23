package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.handler.HandlerRegistry
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.security.oauth2.jwt.JwtDecoder

/**
 * Fails the startup when a handler requires an authenticated caller (`@RequiresAuthentication`, whatever its expression)
 * and the service has no `JwtDecoder` (no JWKS to verify forwarded tokens with): without one a forwarded token is never an
 * identity, so such a handler could only ever refuse its callers, or worse, act on claims nobody checked. Every guarded handler
 * is named in the message.
 *
 * Runs once every singleton exists, so the [registry] holds every handler, and again for every handler registered later
 * (a lazily created bean), which then fails that bean's creation.
 */
class RequiresAuthenticationGuard(
    private val registry: HandlerRegistry,
    private val decoder: ObjectProvider<JwtDecoder>,
) : SmartInitializingSingleton {
    override fun afterSingletonsInstantiated() {
        check()
        registry.onChange(::check)
    }

    /** Throws [IllegalStateException] naming every guarded handler when there is no `JwtDecoder`. */
    fun check() {
        val guarded = (registry.commandTypes().mapNotNull(registry::commandHandler) + registry.queryTypes().mapNotNull(registry::queryHandler))
            .filter { it.authorization != null }
            .map { it.label }
            .distinct()
            .sorted()
        if (guarded.isEmpty() || decoder.getIfAvailable() != null) return
        throw IllegalStateException(
            "${guarded.joinToString()} " + (if (guarded.size == 1) "requires" else "require") + " an authenticated caller (@RequiresAuthentication) " +
                "but no JWKS is configured to verify forwarded tokens: $CONFIGURE_A_JWKS",
        )
    }

    companion object {
        /** What to do about a guarded handler without a JWKS: the end of the startup error. */
        const val CONFIGURE_A_JWKS =
            "set spring.security.oauth2.resourceserver.jwt.jwk-set-uri (or issuer-uri) so Spring Boot creates a JwtDecoder, or declare a JwtDecoder bean; " +
                "without one, forwarded tokens are not verified and every caller is anonymous"
    }
}
