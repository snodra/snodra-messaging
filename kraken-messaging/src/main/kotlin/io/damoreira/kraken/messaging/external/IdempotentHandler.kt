package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.MessageContext
import org.springframework.expression.Expression
import org.springframework.expression.ExpressionException
import org.springframework.expression.ParseException
import org.springframework.expression.spel.standard.SpelExpressionParser
import org.springframework.expression.spel.support.SimpleEvaluationContext

/**
 * Makes a `@CommandHandler` or `@EventHandler` (an adapter to an external system) safe to be called again with the same
 * key: the call is recorded in `external_calls` (`_id` = the key) and a repeat never reaches the external system twice.
 *
 * - `SUCCEEDED`: the stored reply is returned without running the body.
 * - `FAILED`: the stored error is re-thrown (permanent).
 * - A [RetryableExternalException] from the body resets the record, so the next call runs the body again; the saga relay
 *   owns the backoff, adapters never retry internally.
 * - Crash in the middle of a call: an `IN_PROGRESS` record older than its lease means the outcome is unknown. With
 *   [externalIdempotent] `false` (the default) it becomes `UNKNOWN`, the body is never run again automatically and the
 *   caller gets an error the saga relay reports as `SagaCommandFailed(reason = UNKNOWN_OUTCOME)`, so the saga decides.
 *   With `true` (the partner de-duplicates by the key the adapter passes it) the call is retried.
 *
 * [key] is a SpEL expression over `message` (the decoded message) and `headers` (the `MessageContext.headers`); the
 * default is the `idempotency-key` header that the saga relay sets (`<sagaId>:<seq>`). An adapter that polls puts the
 * attempt in the key, e.g. `"'poll:' + message.jobId + ':' + message.attempt"`. Pass the key on to the external system
 * when it accepts one. The handler must not be `suspend`, and the bookkeeping runs in short transactions of its own,
 * so the external call never holds a Mongo transaction open.
 */
@Target(AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
annotation class IdempotentHandler(
    val key: String = "headers['idempotency-key']",
    val externalIdempotent: Boolean = false,
)

/** A parsed [IdempotentHandler]: the key expression and the crash policy. */
class CompiledIdempotency(val expression: Expression, val source: String, val externalIdempotent: Boolean)

/** What an `@IdempotentHandler` key expression is evaluated against: `message` and `headers`. */
class IdempotencyRoot(val message: Any, val headers: Map<String, String>)

object IdempotentHandlers {
    private val parser = SpelExpressionParser()

    /** Parses [annotation]'s key once at registration; a syntax error fails startup naming the handler. */
    fun compile(annotation: IdempotentHandler, label: String): CompiledIdempotency = try {
        CompiledIdempotency(parser.parseExpression(annotation.key), annotation.key, annotation.externalIdempotent)
    } catch (e: ParseException) {
        throw IllegalStateException("Handler $label: invalid @IdempotentHandler key '${annotation.key}': ${e.message}", e)
    }
}

private val KEY_CONTEXT = SimpleEvaluationContext.forReadOnlyDataBinding().build()

/**
 * The idempotency key of [message] handled with [ctx]: [CompiledIdempotency.expression] over [IdempotencyRoot]. A key that
 * cannot be evaluated or comes out empty (the header is missing) is an [IllegalArgumentException] naming [label].
 */
fun CompiledIdempotency.keyOf(message: Any, ctx: MessageContext, label: String): String {
    val value = try {
        expression.getValue(KEY_CONTEXT, IdempotencyRoot(message, ctx.headers))
    } catch (e: ExpressionException) {
        throw IllegalArgumentException("Handler $label: @IdempotentHandler key '$source' cannot be evaluated: ${e.message}", e)
    }
    return value?.toString()?.takeIf { it.isNotBlank() }
        ?: throw IllegalArgumentException("Handler $label: @IdempotentHandler key '$source' is empty for this message (a missing header?)")
}
