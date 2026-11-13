package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.api.Event
import kotlinx.coroutines.runBlocking

/** Publishes domain events to Kafka topic `kraken.events` through the outbox (joining the store transaction of the calling thread). */
interface EventBus {
    suspend fun publish(event: Event, key: String? = null)

    /** Blocking variant of [publish] for non-coroutine callers; never call it from a coroutine. */
    fun publishBlocking(event: Event, key: String? = null) = runBlocking { publish(event, key) }
}
