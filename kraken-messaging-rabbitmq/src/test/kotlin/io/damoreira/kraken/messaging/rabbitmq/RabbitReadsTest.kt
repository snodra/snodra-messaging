package io.damoreira.kraken.messaging.rabbitmq

import io.damoreira.kraken.messaging.eventstore.EventStoreException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test

/** The decisions of the leader-side reads, on read doubles: never "empty" or "the head" from a stalled read. */
class RabbitReadsTest {
    private val chunk = ChunkRead(first = 10, count = 5, timestampMs = 1_000)

    @Test
    fun `a stream is empty of events only when neither the first nor the last read brings a chunk`() {
        assertEquals(Bounds.EMPTY, RabbitReads.boundsOf("s", 3, null, null), "only offset-tracking entries")
        assertEquals(Bounds(3, 25, 2_000), RabbitReads.boundsOf("s", 3, chunk, ChunkRead(20, 5, 2_000)))
        assertThrows(EventStoreException.Failed::class.java) { RabbitReads.boundsOf("s", 3, chunk, null) }
        assertThrows(EventStoreException.Failed::class.java) { RabbitReads.boundsOf("s", 3, null, chunk) }
    }

    @Test
    fun `a time after the last chunk of events is the head without a read`() {
        val bounds = Bounds(0, 15, lastTimestampMs = 1_000)
        val never: (Boolean) -> ChunkRead? = { error("no read expected") }
        assertEquals(15, RabbitReads.offsetAt("s", 1_001, bounds, never))
        assertEquals(0, RabbitReads.offsetAt("s", 5, Bounds.EMPTY, never), "no event at all")
    }

    @Test
    fun `a stalled read by time fails, never the head`() {
        val bounds = Bounds(0, 15, lastTimestampMs = 1_000)
        assertThrows(EventStoreException.Failed::class.java) { RabbitReads.offsetAt("s", 500, bounds) { null } }
        // One older chunk, then silence: still a failure.
        val reads = ArrayDeque(listOf(ChunkRead(0, 5, 100)))
        assertThrows(EventStoreException.Failed::class.java) { RabbitReads.offsetAt("s", 500, bounds) { reads.removeFirstOrNull() } }
    }

    @Test
    fun `a read by time finds the first chunk at or after the time`() {
        val bounds = Bounds(2, 15, lastTimestampMs = 1_000)
        val reads = ArrayDeque(listOf(ChunkRead(0, 5, 100), chunk))
        assertEquals(10, RabbitReads.offsetAt("s", 500, bounds) { reads.removeFirstOrNull() })
        assertEquals(2, RabbitReads.offsetAt("s", 50, bounds) { ChunkRead(0, 5, 100) }, "not before the first offset")
    }

    @Test
    fun `a chunk that delivered fewer messages than its records is a gap when the next one starts`() {
        val t = ChunkTracker()
        assertNull(t.started(0, 3))
        repeat(3) { t.arrived() }
        assertNull(t.started(3, 2), "complete")
        t.arrived()
        val gap = t.started(5, 1)
        assertEquals("the chunk at offset 3 delivered 1 of its 2 messages (the client could not hand out the rest)", gap)
    }
}
