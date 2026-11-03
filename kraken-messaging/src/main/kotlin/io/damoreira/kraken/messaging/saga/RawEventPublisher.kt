package io.damoreira.kraken.messaging.saga

/**
 * Inserts an event known only by wire type name and JSON into the outbox, joining the calling thread's Mongo transaction:
 * what the relay needs to publish an event a saga recorded (it has no Kotlin class for it).
 */
interface RawEventPublisher {
    fun publishRaw(type: String, key: String?, payload: org.bson.Document, correlationId: String)
}
