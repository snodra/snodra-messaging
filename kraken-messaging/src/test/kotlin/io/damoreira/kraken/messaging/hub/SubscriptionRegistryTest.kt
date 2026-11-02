package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The handler side's bookkeeping of subscriptions (P4c), without a hub. */
class SubscriptionRegistryTest {
    private class Recording : SubscriptionSink {
        val parts = mutableListOf<Pair<Long, String>>()
        val ends = mutableListOf<Pair<Int, Error?>>()
        var closed = false
        override fun part(requestId: String, seq: Long, payload: ByteArray): Boolean {
            if (closed) return false
            parts += seq to String(payload)
            return true
        }
        override fun end(requestId: String, parts: Int, error: Error?): Boolean {
            ends += parts to error
            return !closed
        }
    }

    private val stream = Any()
    private val registry = SubscriptionRegistry(2)

    @Test fun `parts of one subscription are numbered from 1 and the end carries the parts sent`() {
        val sink = Recording()
        assertNull(registry.open(stream, "w#1", "t.Watch", "q", sink))
        val subscription = registry.of("t.Watch").single()
        assertTrue(subscription.update("a".toByteArray()) && subscription.update("b".toByteArray()))
        assertTrue(subscription.end(null))
        assertFalse(subscription.end(null)) // once
        assertFalse(subscription.update("late".toByteArray())) // nothing after the end
        assertEquals(listOf(1L to "a", 2L to "b"), sink.parts)
        assertEquals(listOf<Pair<Int, Error?>>(2 to null), sink.ends)
        // Another subscription starts from 1 again.
        val other = Recording()
        registry.open(stream, "w#2", "t.Watch", "q", other)
        registry.of("t.Watch").first { it.requestId == "w#2" }.update("x".toByteArray())
        assertEquals(listOf(1L to "x"), other.parts)
    }

    @Test fun `the limit refuses the next subscription with RESOURCE_EXHAUSTED, a duplicate id is INVALID_REQUEST`() {
        assertNull(registry.open(stream, "a", "t.Watch", "q", Recording()))
        assertNull(registry.open(stream, "b", "t.Watch", "q", Recording()))
        val full = assertNotNull(registry.open(stream, "c", "t.Watch", "q", Recording()))
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED to "subscription limit (2) reached", full.code to full.message)
        assertEquals("c", full.requestId)
        assertEquals(2, registry.size)
        assertEquals(ErrorCode.INVALID_REQUEST, assertNotNull(registry.open(stream, "a", "t.Watch", "q", Recording())).code)
        assertFailsWith<IllegalArgumentException> { SubscriptionRegistry(0) }
    }

    @Test fun `cancel removes one subscription of one stream, closeOwner every subscription of a stream`() {
        val other = Any()
        registry.open(stream, "same-id", "t.Watch", "q", Recording())
        registry.open(other, "same-id", "t.Watch", "q", Recording()) // the same id on another hub's stream is another subscription
        assertEquals(2, registry.size)
        assertFalse(registry.cancel(Any(), "same-id"))
        assertTrue(registry.cancel(stream, "same-id"))
        assertEquals(1, registry.size)
        assertEquals(1, registry.closeOwner(other))
        assertEquals(0, registry.size)
        assertEquals(0, registry.closeOwner(other))
    }

    @Test fun `a subscription whose stream is closed ends and is dropped by the emitter`() {
        val sink = Recording().also { it.closed = true }
        registry.open(stream, "w#1", "t.Watch", "q", sink)
        val subscription = registry.of("t.Watch").single()
        assertFalse(subscription.update("a".toByteArray()))
        assertFalse(subscription.update("b".toByteArray())) // it ended: nothing more is attempted
    }

    @Test fun `endAll ends every subscription cleanly and empties the registry`() {
        val a = Recording(); val b = Recording()
        registry.open(stream, "a", "t.Watch", "q", a)
        registry.open(stream, "b", "t.Other", "q", b)
        registry.of("t.Watch").single().update("x".toByteArray())
        registry.endAll()
        assertEquals(listOf<Pair<Int, Error?>>(1 to null), a.ends)
        assertEquals(listOf<Pair<Int, Error?>>(0 to null), b.ends)
        assertEquals(0, registry.size)
    }
}
