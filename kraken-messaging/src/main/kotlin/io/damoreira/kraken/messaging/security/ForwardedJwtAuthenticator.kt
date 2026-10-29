package io.damoreira.kraken.messaging.security

import io.damoreira.kraken.messaging.UnauthenticatedException
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtException

/**
 * Validates the `authorization` header forwarded with a hub request (`Bearer <JWT>`) using the service's [JwtDecoder].
 * With [requireJwt] a missing header is rejected; without it the caller is anonymous (`userId = null`, no roles).
 */
class ForwardedJwtAuthenticator(private val decoder: JwtDecoder?, private val requireJwt: Boolean) {

    data class Principal(val userId: String?, val roles: Set<String>)

    fun authenticate(authorization: String?): Principal {
        if (authorization.isNullOrBlank()) {
            if (requireJwt) throw UnauthenticatedException("missing bearer token")
            return Principal(null, emptySet())
        }
        val token = bearerToken(authorization) ?: throw UnauthenticatedException("malformed authorization header")
        val jwtDecoder = decoder ?: throw UnauthenticatedException("no JwtDecoder configured")
        val jwt = try {
            jwtDecoder.decode(token)
        } catch (e: JwtException) {
            throw UnauthenticatedException(e.message ?: "invalid token")
        }
        return Principal(jwt.subject, realmRoles(jwt))
    }

    companion object {
        private const val PREFIX = "Bearer "

        /** The token of a `Bearer <token>` header value, or null when the value has another shape. */
        fun bearerToken(authorization: String): String? =
            if (authorization.regionMatches(0, PREFIX, 0, PREFIX.length, ignoreCase = true)) {
                authorization.substring(PREFIX.length).trim().ifEmpty { null }
            } else {
                null
            }

        /** Keycloak realm roles: the string entries of claim `realm_access.roles`; empty when absent. */
        fun realmRoles(jwt: Jwt): Set<String> {
            val realmAccess = jwt.claims["realm_access"] as? Map<*, *> ?: return emptySet()
            val roles = realmAccess["roles"] as? Collection<*> ?: return emptySet()
            return roles.filterIsInstance<String>().toSet()
        }
    }
}
