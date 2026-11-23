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
 * ([SnodraForwardedAuthentication] for a verified caller, installed by the inbound dispatcher so nested sends re-forward the same
 * token). A forwarded token this service cannot verify (it has no `JwtDecoder`) is no authentication at all: the security context
 * stays empty (the caller is anonymous to Spring Security) and the token travels in a [ForwardedToken] element, read by
 * [currentBearerToken] only, so nested sends still forward it.
 */
object CallerContext {
    /** The raw bearer token (without the `Bearer ` prefix), or null for an anonymous caller. */
    fun currentBearerToken(): String? = when (val auth = SecurityContextHolder.getContext().authentication) {
        is JwtAuthenticationToken -> auth.token.tokenValue
        is BearerTokenAuthentication -> auth.token.tokenValue
        is SnodraForwardedAuthentication -> auth.token
        else -> ForwardedToken.current()
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
 * The forwarded bearer [token] of the hub request being handled when this service cannot verify it (no `JwtDecoder`): not an
 * identity, only what nested sends forward on ([CallerContext.currentBearerToken]). Mirrored into a thread-local while the
 * coroutine runs, like the security context; null clears it (an in-process call that carries no caller).
 */
internal class ForwardedToken(private val token: String?) : ThreadContextElement<String?>, AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ForwardedToken> {
        private val tokens = ThreadLocal<String?>()

        /** The forwarded token of the request being handled on this thread, or null. */
        fun current(): String? = tokens.get()
    }

    override fun updateThreadContext(context: CoroutineContext): String? = tokens.get().also { tokens.set(token) }

    override fun restoreThreadContext(context: CoroutineContext, oldState: String?) = tokens.set(oldState)
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
