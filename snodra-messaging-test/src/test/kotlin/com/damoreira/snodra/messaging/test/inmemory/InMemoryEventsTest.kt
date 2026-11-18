package com.damoreira.snodra.messaging.test.inmemory

import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.eventstore.EventHeaders
import com.damoreira.snodra.messaging.eventstore.memory.MemoryEventStore
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import com.damoreira.snodra.messaging.test.InMemoryEvents
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals

class InMemoryEventsTest {
    @Test
    fun aPublishedEventCarriesTheSevenHeadersStampedWithTheApplicationClock() {
        val clock = Clock.fixed(Instant.parse("2026-11-14T10:00:00.123Z"), ZoneOffset.UTC)
        val events = MemoryEventStore()
        val probe = InMemoryEvents(events, MemoryMessageStore(), "orders", "snodra.events", JsonCodec(), MessageTypeResolver("orders"), clock)
        val id = probe.publish(Ordered("o-1"), key = "o-1", from = "billing")
        val stored = events.events("snodra.events").single()
        assertEquals(EventHeaders.ALL.toSet(), stored.headers.keys)
        assertEquals("2026-11-14T10:00:00.123Z", stored.headers[EventHeaders.TIMESTAMP], "the application clock")
        assertEquals(id, stored.id)
        assertEquals("billing", stored.headers[EventHeaders.SOURCE])
        assertEquals("inmemory.Ordered", stored.type)
    }
}
