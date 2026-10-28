package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Event

/** Publishes domain events to Kafka topic `kraken.events` (through the outbox when inside a Mongo transaction). */
interface EventBus {
    suspend fun publish(event: Event, key: String? = null)
}
