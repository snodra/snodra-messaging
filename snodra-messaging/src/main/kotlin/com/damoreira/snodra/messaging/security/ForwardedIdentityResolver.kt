package com.damoreira.snodra.messaging.security

import com.damoreira.snodra.messaging.UnauthenticatedException
import org.slf4j.LoggerFactory
import org.springframework.security.oauth2.jwt.JwtDecoder
import org.springframework.security.oauth2.jwt.JwtException
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.util.Base64
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Resolves the caller of a hub request from its forwarded `authorization` header (`Bearer <JWT>`).
 *
 * No header means an anonymous caller and the request proceeds. A token is an identity only once it is verified: with a
 * [JwtDecoder] (the service's JWKS) it is validated (signature, expiry, issuer) and a bad token is rejected
 * (`UNAUTHENTICATED`: a bug or an attack, not a trusted peer). Without a decoder the caller is anonymous and the token is only
 * forwarded on by the requests the handler sends ([bearerToken]); its claims are never presented as an identity, and a handler
 * that requires an authenticated caller (`@RequiresAuthentication`) cannot be registered without a decoder
 * ([com.damoreira.snodra.messaging.config.RequiresAuthenticationGuard]). What can be judged without the keys is still refused
 * with [UnauthenticatedException] ([checkShape]): a header that is not `Bearer <token>`, a token that is not a compact JWS,
 * an unsigned one and an encrypted one. Expiry and signature are not judged without the keys.
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
            checkShape(token)
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

        private val json = JsonMapper.builder().build()

        /**
         * Refuses, without the keys, a token that is not a signed compact JWS, with the message the hub's other official libraries
         * use: five segments (an encrypted token, JWE) → "unsupported token"; not three segments, a header or claims segment that is
         * not base64url (padding tolerated) of a JSON object, or an `alg` that is not a string → "unreadable token"; `alg: none` (any
         * case) or an empty signature → "unsigned token". Reads nothing into an identity.
         */
        fun checkShape(token: String) {
            val parts = token.split('.', limit = 6)
            if (parts.size == 5) throw UnauthenticatedException("unsupported token")
            if (parts.size != 3) throw UnauthenticatedException("unreadable token")
            val header = jsonObject(parts[0]) ?: throw UnauthenticatedException("unreadable token")
            val alg = header.get("alg")
            if (alg != null && !alg.isNull && !alg.isString) throw UnauthenticatedException("unreadable token")
            if (alg?.takeIf { it.isString }?.asString().equals("none", ignoreCase = true) || parts[2].isEmpty()) {
                throw UnauthenticatedException("unsigned token")
            }
            jsonObject(parts[1]) ?: throw UnauthenticatedException("unreadable token")
        }

        /** A base64url segment (padding tolerated) holding a JSON object, or null. */
        private fun jsonObject(segment: String): JsonNode? = try {
            val bytes = Base64.getUrlDecoder().decode(segment.trimEnd('='))
            val text = String(bytes, Charsets.UTF_8).trim()
            if (!text.startsWith("{")) null else json.readTree(text).takeIf { it.isObject }
        } catch (_: Exception) {
            null
        }

        /** The token of a `Bearer <token>` header value, or null when the value has another shape. */
        fun bearerToken(authorization: String): String? =
            if (authorization.regionMatches(0, PREFIX, 0, PREFIX.length, ignoreCase = true)) {
                authorization.substring(PREFIX.length).trim().ifEmpty { null }
            } else {
                null
            }
    }
}
