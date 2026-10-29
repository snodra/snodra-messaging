package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.MessageContext
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
