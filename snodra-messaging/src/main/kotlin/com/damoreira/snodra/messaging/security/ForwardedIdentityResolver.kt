package com.damoreira.snodra.messaging.security

import com.nimbusds.jwt.JWTParser
import com.nimbusds.jwt.PlainJWT
import com.nimbusds.jwt.SignedJWT
import com.damoreira.snodra.messaging.UnauthenticatedException
import org.slf4j.LoggerFactory
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtException
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Resolves the caller of a hub request from its forwarded `authorization` header (`Bearer <JWT>`).
 *
 * Requests are trusted by default: no header means an anonymous caller and the request proceeds. A token is
 * validated when the service has a [JwtDecoder] (a bad token is rejected: that is a bug or an attack, not a trusted
 * peer); without a decoder the claims are read unverified, because the peer that forwarded the token validated it at
 * ingress; only signed tokens are accepted that way (`alg: none` is rejected). Whether an anonymous caller may run
 * a handler is the handler's decision (`@RequiresAuthentication`).
 */
class ForwardedIdentityResolver(private val decoder: JwtDecoder?) {

    /** The caller, or null when the header is absent. Throws [UnauthenticatedException] for an unusable header. */
    fun resolve(authorization: String?): Identity? {
        if (authorization.isNullOrBlank()) return null
        val token = bearerToken(authorization) ?: throw UnauthenticatedException("malformed authorization header")
        val principal = if (decoder != null) verified(decoder, token) else unverified(token)
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

    private fun unverified(token: String): MessagePrincipal {
        val parsed = try {
            JWTParser.parse(token)
        } catch (e: Exception) {
            throw UnauthenticatedException("unreadable token")
        }
        val claims = when (parsed) {
            is SignedJWT -> parsed.jwtClaimsSet
            is PlainJWT -> throw UnauthenticatedException("unsigned token")
            else -> throw UnauthenticatedException("unsupported token")
        }
        if (unverifiedWarned.compareAndSet(false, true)) {
            log.warn("forwarded identities are trusted without verification: no JwtDecoder bean")
        }
        return MessagePrincipal(claims.subject, RealmRoles.fromClaims(claims.claims), verified = false)
    }

    companion object {
        private const val PREFIX = "Bearer "
        private val log = LoggerFactory.getLogger(ForwardedIdentityResolver::class.java)
        private val unverifiedWarned = AtomicBoolean(false)

        /** The token of a `Bearer <token>` header value, or null when the value has another shape. */
        fun bearerToken(authorization: String): String? =
            if (authorization.regionMatches(0, PREFIX, 0, PREFIX.length, ignoreCase = true)) {
                authorization.substring(PREFIX.length).trim().ifEmpty { null }
            } else {
                null
            }
    }
}
