package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.security.Identity
import io.damoreira.kraken.messaging.security.MessagePrincipal
import org.springframework.expression.EvaluationException
import org.springframework.expression.Expression
import org.springframework.expression.ParseException
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.expression.spel.support.StandardEvaluationContext
import org.springframework.security.access.expression.SecurityExpressionRoot
import org.springframework.security.core.Authentication

/** Outcome of evaluating a handler's `@RequiresAuthentication` expression. */
sealed interface AuthorizationDecision {
    data object Granted : AuthorizationDecision
    data class Unauthenticated(val message: String) : AuthorizationDecision
    data class Denied(val message: String) : AuthorizationDecision
}

/** Spring Security's expression root with `principal` resolving to the message [MessagePrincipal]. */
class MessageSecurityExpressionRoot(
    authentication: Authentication,
    private val principalValue: MessagePrincipal,
) : SecurityExpressionRoot<Any>(authentication) {
    override fun getPrincipal(): Any = principalValue
}

/** A parsed `@RequiresAuthentication` expression, evaluated per request. */
class CompiledAuthorization(val expression: Expression, val source: String) {

    fun check(identity: Identity?, message: Any, ctx: MessageContext, label: String): AuthorizationDecision {
        if (identity == null) return AuthorizationDecision.Unauthenticated("handler $label requires authentication")
        val root = MessageSecurityExpressionRoot(identity.authentication, identity.principal)
        val context = StandardEvaluationContext(root).apply {
            setVariable("message", message)
            setVariable("ctx", ctx)
        }
        val granted = try {
            expression.getValue(context, Boolean::class.java) == true
        } catch (e: EvaluationException) {
            return AuthorizationDecision.Denied("handler $label: access denied (${e::class.simpleName})")
        }
        return if (granted) AuthorizationDecision.Granted else AuthorizationDecision.Denied("handler $label: access denied")
    }
}

object HandlerAuthorizations {
    private val parser = SpelExpressionParser()

    /** Parses [source] once at registration; a syntax error fails startup naming the handler. */
    fun compile(source: String, label: String): CompiledAuthorization = try {
        CompiledAuthorization(parser.parseExpression(source), source)
    } catch (e: ParseException) {
        throw IllegalStateException("Handler $label: invalid @RequiresAuthentication expression '$source': ${e.message}", e)
    }
}
