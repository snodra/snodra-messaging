package com.damoreira.snodra.messaging.security

import kotlinx.coroutines.ThreadContextElement
import org.springframework.security.authentication.AbstractAuthenticationToken
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.context.SecurityContext
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.BearerTokenAuthentication
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * The acting user of the current thread, read from Spring Security's [SecurityContextHolder]: an incoming HTTP request
 * authenticated as [JwtAuthenticationToken] or [BearerTokenAuthentication], or a hub request being handled
 * ([SnodraForwardedAuthentication] for a verified caller, [SnodraForwardedToken] for a token this service cannot verify; installed
 * by the inbound dispatcher so nested sends re-forward the same token).
 */
object CallerContext {
    /** The raw bearer token (without the `Bearer ` prefix), or null for an anonymous caller. */
    fun currentBearerToken(): String? = when (val auth = SecurityContextHolder.getContext().authentication) {
        is JwtAuthenticationToken -> auth.token.tokenValue
        is BearerTokenAuthentication -> auth.token.tokenValue
        is SnodraForwardedAuthentication -> auth.token
        is SnodraForwardedToken -> auth.token
        else -> null
    }

    fun currentUserId(): String? = when (val auth = SecurityContextHolder.getContext().authentication) {
        is JwtAuthenticationToken -> auth.token.subject
        is BearerTokenAuthentication -> auth.name
        is SnodraForwardedAuthentication -> auth.principal.userId
        else -> null
    }

    /** Realm roles of the caller (without the `ROLE_` prefix). */
    fun currentRoles(): Set<String> = when (val auth = SecurityContextHolder.getContext().authentication) {
        is JwtAuthenticationToken -> RealmRoles.fromClaims(auth.token.claims)
        is SnodraForwardedAuthentication -> auth.principal.roles
        else -> emptySet()
    }
}

/**
 * Authentication of a request received from the hub: the forwarded raw [token] (null when the identity was resolved
 * without a token to re-forward) and the resolved [principal]. Always authenticated; roles become `ROLE_<role>`.
 */
class SnodraForwardedAuthentication(
    val token: String?,
    private val principal: MessagePrincipal,
) : AbstractAuthenticationToken(principal.roles.map { SimpleGrantedAuthority("${RealmRoles.ROLE_PREFIX}$it") }) {
    init {
        isAuthenticated = true
    }

    override fun getCredentials(): Any? = token
    override fun getPrincipal(): MessagePrincipal = principal
    override fun getName(): String = principal.userId.orEmpty()
}

/**
 * A hub request's forwarded [token] that this service cannot verify (it has no `JwtDecoder`): the caller is anonymous (never
 * authenticated, no user, no roles), and the token is only carried on by the requests sent while handling it.
 */
class SnodraForwardedToken(val token: String) : AbstractAuthenticationToken(emptyList()) {
    init {
        isAuthenticated = false
    }

    override fun getCredentials(): Any = token
    override fun getPrincipal(): Any? = null
    override fun setAuthenticated(authenticated: Boolean) {
        require(!authenticated) { "a forwarded token nobody verified is never an authenticated caller" }
        super.setAuthenticated(false)
    }
}

/**
 * Installs [context] into [SecurityContextHolder] on every thread the coroutine runs on and restores the thread's
 * previous context when it suspends or completes.
 */
internal class SecurityContextElement(private val context: SecurityContext) :
    ThreadContextElement<SecurityContext>, AbstractCoroutineContextElement(Key) {

    companion object Key : CoroutineContext.Key<SecurityContextElement>

    override fun updateThreadContext(context: CoroutineContext): SecurityContext {
        val previous = SecurityContextHolder.getContext()
        SecurityContextHolder.setContext(this.context)
        return previous
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: SecurityContext) {
        SecurityContextHolder.setContext(oldState)
    }
}
