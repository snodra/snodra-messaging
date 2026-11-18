package com.damoreira.snodra.messaging

import com.damoreira.snodra.messaging.api.Event
import kotlinx.coroutines.runBlocking

/** Publishes domain events to the event store (`snodra.messaging.events.source`) through the outbox (joining the store transaction of the calling thread). */
interface EventBus {
    suspend fun publish(event: Event, key: String? = null)

    /** Blocking variant of [publish] for non-coroutine callers; never call it from a coroutine. */
    fun publishBlocking(event: Event, key: String? = null) = runBlocking { publish(event, key) }
}
