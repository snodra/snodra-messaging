package io.damoreira.kraken.messaging.tracing

import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import kotlin.coroutines.CoroutineContext

/**
 * Writes the current trace context (W3C `traceparent`/`tracestate`, or whatever the propagator emits) into request
 * headers. [propagator] and [tracer] are typed `Any?` so the starter has no hard dependency on micrometer-tracing.
 *
 * Without tracing (either is null, or micrometer-tracing is not on the classpath) it keeps the trace floor: a message sent
 * while handling one carries the handled message's `traceparent` unchanged ([CorrelationContext.currentTraceparent]; a valid W3C one
 * only, [Traceparents]), so a trace
 * still follows the message across services; outside handling it writes nothing. With tracing the propagator alone decides.
 */
class TraceHeaders(
    private val propagator: Any? /* io.micrometer.tracing.propagation.Propagator */,
    private val tracer: Any? /* io.micrometer.tracing.Tracer */,
) {
    /** Writes the trace headers of a message sent now; [context] is the sender's coroutine context (null: the thread's). */
    fun inject(into: MutableMap<String, String>, context: CoroutineContext? = null) {
        if (propagator == null || tracer == null || !MicrometerTracing.available) {
            Traceparents.valid(CorrelationContext.currentTraceparent(context))?.let { into[TRACEPARENT] = it }
            return
        }
        MicrometerTracing.inject(propagator, tracer, into)
    }

    private companion object {
        const val TRACEPARENT = "traceparent"
    }
}

/** Keeps micrometer-tracing classes off the class-loading path when they are not on the runtime classpath. */
private object MicrometerTracing {
    val available: Boolean = try {
        Class.forName("io.micrometer.tracing.propagation.Propagator")
        Class.forName("io.micrometer.tracing.Tracer")
        true
    } catch (_: ClassNotFoundException) {
        false
    } catch (_: LinkageError) {
        false
    }

    fun inject(propagator: Any, tracer: Any, into: MutableMap<String, String>) {
        if (propagator !is Propagator || tracer !is Tracer) return
        val context = tracer.currentTraceContext().context() ?: return
        propagator.inject(context, into) { carrier, key, value -> carrier?.put(key, value) }
    }
}
