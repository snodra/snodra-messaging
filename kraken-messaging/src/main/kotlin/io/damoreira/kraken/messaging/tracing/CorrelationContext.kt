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
 *
 * It also carries the handled message's [traceparent] (null when it had none), the trace floor: without Micrometer Tracing,
 * what is sent while handling the message carries that value unchanged ([TraceHeaders]). While the coroutine runs it is
 * mirrored into a thread-local the same way ([currentTraceparent]).
 */
class CorrelationContext(val correlationId: String, val traceparent: String? = null) :
    ThreadContextElement<CorrelationContext.Previous>, AbstractCoroutineContextElement(Key) {

    /** What the thread held before this context ran on it. */
    class Previous(val correlationId: String?, val traceparent: String?)

    companion object Key : CoroutineContext.Key<CorrelationContext> {
        const val MDC_KEY = "correlationId"

        private val traceparents = ThreadLocal<String?>()

        /** The id to put on an outgoing message: [context]'s [CorrelationContext], else the MDC value, else a fresh UUID. */
        fun resolve(context: CoroutineContext?): String =
            context?.get(Key)?.correlationId ?: MDC.get(MDC_KEY)?.takeIf { it.isNotBlank() } ?: UUID.randomUUID().toString()

        /** The handled message's `traceparent`: [context]'s [CorrelationContext]'s, else the one running on this thread, else null. */
        fun currentTraceparent(context: CoroutineContext? = null): String? =
            context?.get(Key)?.let { return it.traceparent } ?: traceparents.get()
    }

    override fun updateThreadContext(context: CoroutineContext): Previous {
        val previous = Previous(MDC.get(MDC_KEY), traceparents.get())
        MDC.put(MDC_KEY, correlationId)
        traceparents.set(traceparent)
        return previous
    }

    override fun restoreThreadContext(context: CoroutineContext, oldState: Previous) {
        if (oldState.correlationId == null) MDC.remove(MDC_KEY) else MDC.put(MDC_KEY, oldState.correlationId)
        if (oldState.traceparent == null) traceparents.remove() else traceparents.set(oldState.traceparent)
    }

    override fun toString(): String = "CorrelationContext($correlationId)"
}
