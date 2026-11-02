package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertIs
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

    // --- pending: registered when the request is read, before the admission ---

    private fun pending(id: String, sink: SubscriptionSink, type: String = "t.Watch") =
        (registry.openPending(stream, id, type, "{}".toByteArray(), sink) as Opening.Pending).subscription

    @Test fun `a pending subscription holds updates and the end in order, activate writes them before any later update`() {
        val sink = Recording()
        val subscription = pending("w#1", sink)
        assertEquals(1, registry.size) // counted from the moment it is read
        assertNull(subscription.query)
        assertTrue(subscription.update("a".toByteArray()) && subscription.update("b".toByteArray()))
        assertEquals(emptyList(), sink.parts) // held
        assertTrue(registry.activate(subscription, "q"))
        assertTrue(subscription.update("c".toByteArray()))
        assertEquals(listOf(1L to "a", 2L to "b", 3L to "c"), sink.parts)
        assertEquals("q", subscription.query)
    }

    @Test fun `an end during admission is held and written after the updates before it`() {
        val sink = Recording()
        val subscription = pending("w#1", sink)
        subscription.update("a".toByteArray())
        assertTrue(subscription.end(null))
        assertFalse(subscription.update("late".toByteArray())) // nothing after the end
        registry.activate(subscription, "q")
        assertEquals(listOf(1L to "a"), sink.parts)
        assertEquals(listOf<Pair<Int, Error?>>(1 to null), sink.ends)
    }

    @Test fun `a refused admission drops what was held and writes only the refusal, nothing stays open`() {
        val sink = Recording()
        val subscription = pending("w#1", sink)
        subscription.update("a".toByteArray())
        val refusal = Error.newBuilder().setRequestId("w#1").setCode(ErrorCode.PERMISSION_DENIED).setMessage("no").build()
        assertTrue(registry.refuse(subscription, refusal))
        assertEquals(emptyList(), sink.parts)
        assertEquals(listOf<Pair<Int, Error?>>(0 to refusal), sink.ends)
        assertEquals(0, registry.size)
        assertFalse(registry.activate(subscription, "q")) // too late
        assertFalse(subscription.update("b".toByteArray()))
    }

    @Test fun `a hub Cancel or a lost stream during admission drops the pending subscription and nothing is ever written`() {
        val cancelled = Recording()
        val lost = Recording()
        val a = pending("a", cancelled)
        val b = pending("b", lost)
        a.update("held".toByteArray())
        b.update("held".toByteArray())
        assertTrue(registry.cancel(stream, "a"))
        assertEquals(1, registry.closeOwner(stream))
        assertEquals(0, registry.size)
        // The admissions end later: both are no-ops, nothing is written for either.
        assertFalse(registry.activate(a, "q"))
        assertFalse(registry.refuse(b, Error.newBuilder().setCode(ErrorCode.HANDLER_ERROR).build()))
        for (sink in listOf(cancelled, lost)) {
            assertEquals(emptyList(), sink.parts)
            assertEquals(emptyList(), sink.ends)
        }
    }

    @Test fun `a pending subscription counts against the limit and a duplicate id is refused`() {
        pending("a", Recording())
        pending("b", Recording())
        val full = registry.openPending(stream, "c", "t.Watch", "{}".toByteArray(), Recording())
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED, assertIs<Opening.Refused>(full).error.code)
        val duplicate = registry.openPending(stream, "a", "t.Watch", "{}".toByteArray(), Recording())
        assertEquals(ErrorCode.INVALID_REQUEST, assertIs<Opening.Refused>(duplicate).error.code)
        assertEquals(2, registry.size)
    }
}
