package io.damoreira.kraken.messaging.saga

/**
 * Inserts an event known only by wire type name and JSON into the outbox, joining the calling thread's transaction: what
 * the relay needs to publish an event a saga recorded (it has no Kotlin class for it). [payload] is the event as JSON.
 */
interface RawEventPublisher {
    fun publishRaw(type: String, key: String?, payload: ByteArray, correlationId: String)
}
