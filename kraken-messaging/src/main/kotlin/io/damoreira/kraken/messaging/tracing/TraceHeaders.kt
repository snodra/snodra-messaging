package io.damoreira.kraken.messaging.tracing

import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator

/**
 * Writes the current trace context (W3C `traceparent`/`tracestate`, or whatever the propagator emits) into request
 * headers. [propagator] and [tracer] are typed `Any?` so the starter has no hard dependency on micrometer-tracing: it is
 * a no-op when either is null or when micrometer-tracing is not on the classpath.
 */
class TraceHeaders(
    private val propagator: Any? /* io.micrometer.tracing.propagation.Propagator */,
    private val tracer: Any? /* io.micrometer.tracing.Tracer */,
) {
    fun inject(into: MutableMap<String, String>) {
        if (propagator == null || tracer == null || !MicrometerTracing.available) return
        MicrometerTracing.inject(propagator, tracer, into)
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
