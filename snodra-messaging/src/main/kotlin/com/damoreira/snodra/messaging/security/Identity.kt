package com.damoreira.snodra.messaging.security

import org.springframework.security.authentication.AnonymousAuthenticationToken
import org.springframework.security.core.Authentication
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthentication
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken

/**
 * The caller of a command or query as seen by the receiving handler: the Keycloak subject, the realm roles and
 * whether the identity was cryptographically verified by this service ([verified]) or trusted as forwarded by a peer.
 * This is also what `principal` resolves to in `@RequiresAuthentication` expressions.
 */
data class MessagePrincipal(val userId: String?, val roles: Set<String>, val verified: Boolean)

/** A resolved caller: the [principal] plus the Spring Security [authentication] the expression root evaluates against. */
data class Identity(val principal: MessagePrincipal, val authentication: Authentication) {
    val userId: String? get() = principal.userId
    val roles: Set<String> get() = principal.roles
}

/** Extracts Keycloak realm roles (`realm_access.roles`) from token claims; empty when absent. */
object RealmRoles {
    fun fromClaims(claims: Map<String, Any?>): Set<String> {
        val realmAccess = claims["realm_access"] as? Map<*, *> ?: return emptySet()
        val roles = realmAccess["roles"] as? Collection<*> ?: return emptySet()
        return roles.filterIsInstance<String>().toSet()
    }

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
