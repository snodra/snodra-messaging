package com.damoreira.snodra.messaging.saga

/**
 * Inserts an event known only by wire type name and JSON into the outbox, joining the calling thread's transaction: what
 * the relay needs to publish an event a saga recorded (it has no Kotlin class for it). [payload] is the event as JSON.
 */
interface RawEventPublisher {
    /** [traceparent]: the trace of the event that caused it (the trace floor: forwarded without Micrometer Tracing); null: none. */
    fun publishRaw(type: String, key: String?, payload: ByteArray, correlationId: String, traceparent: String? = null)
}
