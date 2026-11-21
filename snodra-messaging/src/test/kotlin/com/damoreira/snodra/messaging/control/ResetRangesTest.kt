package com.damoreira.snodra.messaging.control

import com.damoreira.snodra.messaging.eventstore.ReplayRange
import com.damoreira.snodra.messaging.eventstore.ResetTarget
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/** The replay ranges of a reset from the event store's plan, ordered by the event store's compare only. */
class ResetRangesTest {
    private val decimal = Comparator<String> { a, b -> a.toLong().compareTo(b.toLong()) }

    private fun range(segment: Int, from: Long, until: String, source: String = "t") = ReplayRange(source, segment.toString(), from.toString(), until)

    @Test fun `a range is a move whose target is before the committed position`() {
        val moves = listOf(range(10, 0, "10"), range(1, 5, "5"), range(2, 8, "3"), range(3, 0, ""), range(0, 9, "10"))
        assertEquals(
            listOf(range(0, 9, "10"), range(10, 0, "10")),
            ResetRanges.replaying(moves, ResetTarget.Earliest, emptyList(), decimal),
            "not at or past the committed position, not without one; by position, not text; in segment order",
        )
        assertTrue(ResetRanges.replaying(listOf(range(0, 0, "10")), ResetTarget.Latest, emptyList(), decimal).isEmpty(), "latest never makes a range")
    }

    @Test fun `an open range of the segment extends the new one and the others stay`() {
        val open = listOf(range(0, 2, "20"), range(1, 0, "7"), range(0, 0, "99", source = "other"))
        val moves = listOf(range(0, 4, "12"), range(1, 9, "9"))
        val new = ResetRanges.replaying(moves, ResetTarget.Timestamp(1), open, decimal)
        assertEquals(listOf(range(0, 4, "20")), new)
        assertEquals(listOf(range(0, 0, "99", source = "other"), range(0, 4, "20"), range(1, 0, "7")), ResetRanges.merged(open, new, decimal))
    }
}
