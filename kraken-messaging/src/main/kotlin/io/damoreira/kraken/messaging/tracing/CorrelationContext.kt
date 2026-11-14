package io.damoreira.kraken.messaging.tracing

import kotlinx.coroutines.ThreadContextElement
import org.slf4j.MDC
import java.util.UUID
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext

/**
 * The correlation id of the message being handled, carried in the coroutine context from an incoming hub request or
 * delivered event to every message sent while handling it. While the coroutine runs it is also mirrored into the SLF4J
 * [MDC] under [MDC_KEY] (and the thread's previous value restored when it suspends or completes), so log lines carry it
 * and blocking code on the same thread (`sendBlocking`, `publishBlocking`) still finds it.
 */
class CorrelationContext(val correlationId: String) :
    ThreadContextElement<String?>, AbstractCoroutineContextElement(Key) {

    companion object Key : CoroutineContext.Key<CorrelationContext> {
        const val MDC_KEY = "correlationId"

        /** The id to put on an outgoing message: [context]'s [CorrelationContext], else the MDC value, else a fresh UUID. */
        fun resolve(context: CoroutineContext?): String =
            context?.get(Key)?.correlationId ?: MDC.get(MDC_KEY)?.takeIf { it.isNotBlank() } ?: UUID.randomUUID().toString()
    }

    override fun updateThreadContext(context: CoroutineContext): String? {
        val previous = MDC.get(MDC_KEY)
        MDC.put(MDC_KEY, correlationId)
        return previous
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: String?) {
        if (oldState == null) MDC.remove(MDC_KEY) else MDC.put(MDC_KEY, oldState)
    }

    override fun toString(): String = "CorrelationContext($correlationId)"
}
