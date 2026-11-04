package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.control.ResetContext
import io.damoreira.kraken.messaging.external.CompiledIdempotency
import java.lang.reflect.InvocationTargetException
import kotlin.reflect.KClass
import kotlin.reflect.KFunction
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.isAccessible

enum class HandlerKind { EVENT, COMMAND, QUERY }

/** A discovered handler: the bean (possibly a proxy) to invoke and the function to call on it. */
class HandlerMethod(
    val bean: Any,
    val function: KFunction<*>,
    val kind: HandlerKind,
    val messageClass: KClass<*>,
    val typeName: String,
    val wantsContext: Boolean,
    /** Compiled `@RequiresAuthentication` expression, or null when any caller may invoke the handler. */
    val authorization: CompiledAuthorization? = null,
    /** `@QueryHandler(shared = true)`: the type is registered as a shared query type. */
    val shared: Boolean = false,
    /** The message is a `StreamingQuery<R>` and the function returns `Flow<R>` (P4b): served part by part under credit. */
    val streaming: Boolean = false,
    /**
     * The message is a `SubscriptionQuery<I, U>` (P4c): the function answers the initial result `I` of a subscription
     * (the hub's `<id>#0` request); the updates come from the `QueryUpdateEmitter`.
     */
    val subscription: Boolean = false,
    /** Compiled `@IdempotentHandler`, or null: the invoker runs the handler through the idempotency interceptor. */
    val idempotency: CompiledIdempotency? = null,
    /** The event processor of an `@EventHandler` (its class's `@ProcessingGroup`, else `default`, ADR 0022); `default` for other kinds. */
    val processor: String = Processors.DEFAULT,
    /** `@DisallowReplay` on the event handler or its class (P6b): a replayed record does not run it. */
    val disallowReplay: Boolean = false,
) {
    init {
        function.isAccessible = true
    }

    /** `Bean.method` label used in diagnostics. */
    val label: String get() = "${bean::class.simpleName}.${function.name}"

    suspend fun invoke(message: Any, ctx: MessageContext): Any? {
        val args = if (wantsContext) arrayOf(bean, message, ctx) else arrayOf(bean, message)
        val result = try {
            if (function.isSuspend) function.callSuspend(*args) else function.call(*args)
        } catch (e: InvocationTargetException) {
            throw e.targetException ?: e
        }
        return result ?: Unit
    }
}

/** A `@ResetHandler` method (P6b) of [processor]: called with the [ResetContext] when it takes one. */
class ResetHandlerMethod(val bean: Any, val function: KFunction<*>, val processor: String, val wantsContext: Boolean) {
    init {
        function.isAccessible = true
    }

    /** `Bean.method` label used in diagnostics. */
    val label: String get() = "${bean::class.simpleName}.${function.name}"

    suspend fun invoke(ctx: ResetContext) {
        val args = if (wantsContext) arrayOf(bean, ctx) else arrayOf(bean)
        try {
            if (function.isSuspend) function.callSuspend(*args) else function.call(*args)
        } catch (e: InvocationTargetException) {
            throw e.targetException ?: e
        }
    }
}
