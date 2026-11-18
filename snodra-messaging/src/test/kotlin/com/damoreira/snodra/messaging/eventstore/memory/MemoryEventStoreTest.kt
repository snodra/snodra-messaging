package com.damoreira.snodra.messaging.eventstore.memory

import com.damoreira.snodra.messaging.eventstore.Capability
import com.damoreira.snodra.messaging.eventstore.Delivery
import com.damoreira.snodra.messaging.eventstore.EventHeaders
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
import com.damoreira.snodra.messaging.eventstore.Subscription
import com.damoreira.snodra.messaging.eventstore.SubscriptionEvent
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.Test
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull

/** What the event-store kit does not pin about the in-memory event store: its layout, its read access and its hand-overs. */
class MemoryEventStoreTest {
    private val clock = Clock.fixed(Instant.parse("2026-11-14T10:00:00Z"), ZoneOffset.UTC)

    private fun event(id: String, key: String?, type: String = "t.Happened", source: String = "kraken.events") =
        OutgoingEvent(source, id, type, key, mapOf(EventHeaders.TYPE to type, EventHeaders.ID to id), """{"id":"$id"}""".toByteArray())

    private suspend fun Subscription.nextWithin(ms: Long = 2_000): SubscriptionEvent? = withTimeoutOrNull(ms) { next() }

    @Test
    fun aKeyGoesToTheSegmentOfItsFnv1aHash() {
        // The FNV-1a 64 hash of the key's UTF-8 bytes, unsigned, modulo the segments (the Rust library's in-memory store says the same).
        assertEquals(listOf(2, 1, 0, 1, 1), listOf("k", "order-1", "order-2", "é", "").map { MemoryEventStore.segmentOf(it, 4) })
        assertEquals(listOf(2, 3, 0, 1, 5), listOf("k", "order-1", "order-2", "é", "").map { MemoryEventStore.segmentOf(it, 6) })
    }

    @Test
    fun theReadAccessListsWhatWasPublishedInOrder() = runBlocking {
        val events = MemoryEventStore(MemoryEventStoreSettings(segments = 2), clock)
        assertEquals("memory", events.name)
        assertEquals(setOf(Capability.REPLAY, Capability.BROWSE, Capability.LAG), events.capabilities)
        events.publish(event("e-1", "order-1"))
        events.publish(event("e-2", null, type = "t.Other"))
        events.publish(event("e-3", "order-1"))
        events.publish(event("x-1", "order-1", source = "other.source"))

        assertEquals(listOf("kraken.events", "other.source"), events.sources())
        assertEquals(listOf("e-1", "e-2", "e-3"), events.events("kraken.events").map { it.id })
        assertEquals(listOf("e-1", "e-3"), events.events("kraken.events", type = "t.Happened").map { it.id })
        assertEquals(listOf("e-1", "e-3"), events.events("kraken.events", key = "order-1").map { it.id })
        val first = events.events("kraken.events").first()
        assertEquals(MemoryEventStore.segmentOf("order-1", 2).toString(), first.segment)
        assertEquals("0", first.position)
        assertEquals(clock.millis(), first.timestampMs)
        assertEquals("e-3", events.event("kraken.events", first.segment, "1")?.id)
        assertNull(events.event("kraken.events", first.segment, "2"))
        assertNull(events.event("kraken.events", "01", "0"), "only the canonical segment names")
        assertEquals(emptyList(), events.events("nothing"))
    }

    @Test
    fun aSegmentChangesHandsOnlyOnceItsHolderGaveItUp() = runBlocking {
        val events = MemoryEventStore(MemoryEventStoreSettings(segments = 2), clock)
        val spec = SubscriptionSpec("svc.default", "kraken.events")
        val a = events.subscribe(spec)
        assertEquals(SubscriptionEvent.Assigned(listOf("0", "1")), a.nextWithin())
        val b = events.subscribe(spec)
        // A member at once, holding nothing yet: segment 1 is still the first instance's until it hears of the change.
        assertEquals(SubscriptionEvent.Assigned(emptyList()), b.nextWithin(), "a new subscription's first event, empty")
        assertNull(b.nextWithin(300), "segment 1 is still the first instance's until it hears of the change")
        assertEquals(SubscriptionEvent.Revoked(listOf("1")), a.nextWithin())
        assertEquals(SubscriptionEvent.Assigned(listOf("1")), withTimeout(2_000) { b.next() }, "woken at once when the segment is free")
        b.close()
        assertEquals(SubscriptionEvent.Assigned(listOf("1")), a.nextWithin(), "a closed instance's segments go back")
        a.close()
    }

    @Test
    fun aDeliveryWaitsForAPublishAndAnAckedOneIsNotDeliveredToTheNextHolder() = runBlocking {
        val events = MemoryEventStore(MemoryEventStoreSettings(segments = 1), clock)
        val spec = SubscriptionSpec("svc.default", "kraken.events")
        val a = events.subscribe(spec)
        assertEquals(SubscriptionEvent.Assigned(listOf("0")), a.nextWithin())
        assertNull(a.nextWithin(200))
        events.publish(event("e-1", "k"))
        events.publish(event("e-2", "k"))
        val first = a.nextWithin() as Delivery
        assertEquals("e-1", first.headers[EventHeaders.ID])
        a.ack(first.segment, first.position)
        a.close()
        val b = events.subscribe(spec)
        assertEquals(SubscriptionEvent.Assigned(listOf("0")), b.nextWithin())
        assertEquals("e-2", (b.nextWithin() as Delivery).headers[EventHeaders.ID])
        b.close()
    }

    @Test
    fun whatADeliveryOrAReadHandsOutCannotChangeTheLog() = runBlocking {
        val events = MemoryEventStore(MemoryEventStoreSettings(segments = 1), clock)
        val published = event("e-1", "k")
        events.publish(published)
        published.payload.fill(0) // the publisher's own array, changed after the publish
        val spec = SubscriptionSpec("svc.default", "kraken.events")
        val sub = events.subscribe(spec)
        sub.nextWithin()
        val delivered = sub.nextWithin() as Delivery
        delivered.payload.fill(0)
        runCatching { @Suppress("UNCHECKED_CAST") (delivered.headers as MutableMap<String, String>)[EventHeaders.ID] = "changed" }
        val read = events.events("kraken.events").single()
        read.payload.fill(0)
        runCatching { @Suppress("UNCHECKED_CAST") (read.headers as MutableMap<String, String>)[EventHeaders.ID] = "changed" }

        sub.redeliver(delivered.segment, delivered.position)
        val again = sub.nextWithin() as Delivery
        assertEquals("""{"id":"e-1"}""", String(again.payload), "the log's payload is the one published")
        assertEquals("e-1", again.headers[EventHeaders.ID])
        assertEquals("""{"id":"e-1"}""", String(events.events("kraken.events").single().payload))
        assertEquals("e-1", events.events("kraken.events").single().id)
        sub.close()
    }

    @Test
    fun sourcesListsTheSourcesWithEvents() = runBlocking {
        val events = MemoryEventStore(MemoryEventStoreSettings(segments = 1), clock)
        val sub = events.subscribe(SubscriptionSpec("svc.default", "never.published"))
        sub.nextWithin()
        sub.nextWithin(100)
        assertEquals(emptyList(), events.sources(), "a subscription alone makes no source")
        events.publish(event("e-1", "k"))
        assertEquals(listOf("kraken.events"), events.sources())
        sub.close()
    }

    @Test
    fun theSegmentsSettingIsBounded() {
        assertFailsWith<IllegalArgumentException> { MemoryEventStoreSettings(segments = 0) }
        assertFailsWith<IllegalArgumentException> { MemoryEventStoreSettings(segments = 1025) }
        assertEquals(4, MemoryEventStoreSettings().segments)
    }
}
