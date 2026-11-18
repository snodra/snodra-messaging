package com.damoreira.snodra.messaging.eventstore.memory

import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import com.damoreira.snodra.messaging.test.eventstore.EventStoreConformanceTest

/**
 * The event-store conformance kit on the in-memory event store, no container: every "instance" is the same store (an
 * in-process store is one event store for the whole JVM), the markers in the in-memory message store.
 */
class MemoryEventStoreConformanceTest : EventStoreConformanceTest() {
    private val events = MemoryEventStore()
    private val store = MemoryMessageStore()

    override val source = "conformance.events"

    override fun eventStore(): EventStore = events

    override fun messageStore(): MessageStore = store
}
