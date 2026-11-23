package com.damoreira.snodra.messaging.security

import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthentication
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken

/**
 * The caller of a command or query as seen by the receiving handler: the token's subject, the roles and whether the
 * identity was verified ([verified]). A forwarded token becomes an identity only once this service verified it against its
 * JWKS (a token it cannot verify leaves the caller anonymous), and the local short-circuit takes the identity the
 * application's own security established, so [verified] is true for every identity the library hands out.
 * This is also what `principal` resolves to in `@RequiresAuthentication` expressions.
 */
data class MessagePrincipal(val userId: String?, val roles: Set<String>, val verified: Boolean)

/** A resolved caller: the [principal] plus the Spring Security [authentication] the expression root evaluates against. */
data class Identity(val principal: MessagePrincipal, val authentication: Authentication) {
    val userId: String? get() = principal.userId
    val roles: Set<String> get() = principal.roles
}

/**
 * Where a forwarded token's roles come from (`snodra.messaging.security.roles-claim`): a dotted claim path, or
 * `scope` for space-separated OAuth2 scopes. Providers disagree, so it is configuration: `roles` by default,
 * `realm_access.roles` for providers that nest them there, `groups` for those that use groups.
 */
object RolesClaim {
    @Volatile
    var path: String = "roles"
        private set

    /** Sets the claim path once, at startup; a blank value keeps the default. */
    fun configure(value: String) {
        path = value.trim().ifEmpty { "roles" }
    }

    /** The roles the configured claim holds for [claims]; empty when it is absent or malformed. */
    fun read(claims: Map<String, Any?>): Set<String> = read(claims, path)

    /** The roles [claim] holds for [claims]: `scope`, or a dotted path into nested objects and an array. */
    fun read(claims: Map<String, Any?>, claim: String): Set<String> {
        if (claim.equals("scope", ignoreCase = true)) {
            val scopes = claims["scope"] as? String ?: return emptySet()
            return scopes.split(' ').filter { it.isNotBlank() }.toSet()
        }
        var node: Any? = claims
        for (segment in claim.split('.').filter { it.isNotBlank() }) {
            node = (node as? Map<*, *>)?.get(segment)
        }
        return (node as? Collection<*>)?.filterIsInstance<String>()?.toSet() ?: emptySet()
    }
}

/** Extracts the roles from token claims (see [RolesClaim]); empty when absent. */
object RealmRoles {
    fun fromClaims(claims: Map<String, Any?>): Set<String> = RolesClaim.read(claims)

    /** Role names from `ROLE_`-prefixed authorities (the prefix removed); other authorities are ignored. */
    fun fromAuthorities(authentication: Authentication): Set<String> =
        authentication.authorities.mapNotNull { it.authority?.takeIf { a -> a.startsWith(ROLE_PREFIX) }?.removePrefix(ROLE_PREFIX) }.toSet()

    const val ROLE_PREFIX = "ROLE_"
}

/** The identity of the current thread's caller for the local short-circuit path; null means anonymous. */
object LocalIdentity {
    /**
     * Normalises the current [org.springframework.security.core.Authentication] to the same shape the hub path
     * produces ([SnodraForwardedAuthentication] with `ROLE_<realm role>` authorities), so `@RequiresAuthentication`
     * expressions evaluate identically whether a call is short-circuited locally or routed through the hub, whatever
     * authority mapping the application's own resource-server converter applies (Spring's default yields `SCOPE_*`).
     */
    fun current(): Identity? {
        val auth = SecurityContextHolder.getContext().authentication ?: return null
        if (!auth.isAuthenticated || auth is AnonymousAuthenticationToken) return null
        if (auth is SnodraForwardedAuthentication) return Identity(auth.principal, auth)
        val (principal, token) = when (auth) {
            is JwtAuthenticationToken ->
                MessagePrincipal(auth.token.subject, RealmRoles.fromClaims(auth.token.claims), verified = true) to auth.token.tokenValue
            is BearerTokenAuthentication ->
                MessagePrincipal(auth.tokenAttributes["sub"] as? String, RealmRoles.fromClaims(auth.tokenAttributes), verified = true) to
                    auth.token.tokenValue
            else -> MessagePrincipal(auth.name, RealmRoles.fromAuthorities(auth), verified = true) to null
        }
        return Identity(principal, SnodraForwardedAuthentication(token, principal))
    }
}
