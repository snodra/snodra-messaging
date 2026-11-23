package com.damoreira.snodra.messaging.security

import com.damoreira.snodra.messaging.UnauthenticatedException
import org.slf4j.LoggerFactory
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Resolves the caller of a hub request from its forwarded `authorization` header (`Bearer <JWT>`).
 *
 * No header means an anonymous caller and the request proceeds. A token is an identity only once it is verified: with a
 * [JwtDecoder] (the service's JWKS) it is validated (signature, expiry, issuer) and a bad token is rejected
 * (`UNAUTHENTICATED`: a bug or an attack, not a trusted peer). Without a decoder the token is never read: the caller is
 * anonymous and the token is only forwarded on by the requests the handler sends ([bearerToken]). A handler that requires an
 * authenticated caller (`@RequiresAuthentication`) cannot be registered without a decoder
 * ([com.damoreira.snodra.messaging.config.RequiresAuthenticationGuard]),
 * so no handler ever acts on claims nobody checked. A header that is not `Bearer <token>` is `UNAUTHENTICATED` either way.
 */
class ForwardedIdentityResolver(private val decoder: JwtDecoder?) {

    /**
     * The verified caller, or null for an anonymous one (no header, or no decoder to verify the token with). Throws
     * [UnauthenticatedException] for an unusable header or a token the decoder refuses.
     */
    fun resolve(authorization: String?): Identity? {
        if (authorization.isNullOrBlank()) return null
        val token = bearerToken(authorization) ?: throw UnauthenticatedException("malformed authorization header")
        if (decoder == null) {
            if (unverifiedNoted.compareAndSet(false, true)) {
                log.info("No JwtDecoder: forwarded tokens are not verified, so their callers are anonymous (the tokens are forwarded on unread)")
            }
            return null
        }
        val principal = verified(decoder, token)
        return Identity(principal, SnodraForwardedAuthentication(token, principal))
    }

    private fun verified(decoder: JwtDecoder, token: String): MessagePrincipal {
        val jwt = try {
            decoder.decode(token)
        } catch (e: JwtException) {
            throw UnauthenticatedException(e.message ?: "invalid token")
        }
        return MessagePrincipal(jwt.subject, RealmRoles.fromClaims(jwt.claims), verified = true)
    }

    companion object {
        private const val PREFIX = "Bearer "
        private val log = LoggerFactory.getLogger(ForwardedIdentityResolver::class.java)
        private val unverifiedNoted = AtomicBoolean(false)

        /** The token of a `Bearer <token>` header value, or null when the value has another shape. */
        fun bearerToken(authorization: String): String? =
            if (authorization.regionMatches(0, PREFIX, 0, PREFIX.length, ignoreCase = true)) {
                authorization.substring(PREFIX.length).trim().ifEmpty { null }
            } else {
                null
            }
    }
}
