package io.damoreira.kraken.messaging.security

import io.damoreira.kraken.messaging.UnauthenticatedException
import org.slf4j.LoggerFactory
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Validates the `authorization` header forwarded with a hub request (`Bearer <JWT>`) using the service's [JwtDecoder].
 * With [requireJwt] a missing header is rejected; without it the caller is anonymous (`userId = null`, no roles).
 * Without [requireJwt] and without a [decoder] a forwarded token cannot be validated, so it is ignored and the caller
 * is anonymous too (logged once per process). [requireJwt] without a decoder fails at startup (auto-configuration).
 */
class ForwardedJwtAuthenticator(private val decoder: JwtDecoder?, private val requireJwt: Boolean) {


    fun authenticate(authorization: String?): MessagePrincipal {
        if (authorization.isNullOrBlank()) {
            if (requireJwt) throw UnauthenticatedException("missing bearer token")
            return ANONYMOUS
        }
        if (decoder == null && !requireJwt) {
            if (ignoredTokenWarned.compareAndSet(false, true)) {
                log.warn("forwarded JWT ignored: no JwtDecoder configured and require-jwt=false")
            }
            return ANONYMOUS
        }
        val token = bearerToken(authorization) ?: throw UnauthenticatedException("malformed authorization header")
        val jwtDecoder = decoder ?: throw UnauthenticatedException("no JwtDecoder configured")
        val jwt = try {
            jwtDecoder.decode(token)
        } catch (e: JwtException) {
            throw UnauthenticatedException(e.message ?: "invalid token")
        }
        return MessagePrincipal(jwt.subject, RealmRoles.fromClaims(jwt.claims), verified = true)
    }

    companion object {
        private const val PREFIX = "Bearer "
        private val ANONYMOUS = MessagePrincipal(null, emptySet(), verified = false)
        private val log = LoggerFactory.getLogger(ForwardedJwtAuthenticator::class.java)
        private val ignoredTokenWarned = AtomicBoolean(false)

        /** The token of a `Bearer <token>` header value, or null when the value has another shape. */
        fun bearerToken(authorization: String): String? =
            if (authorization.regionMatches(0, PREFIX, 0, PREFIX.length, ignoreCase = true)) {
                authorization.substring(PREFIX.length).trim().ifEmpty { null }
            } else {
                null
            }

    }
}
