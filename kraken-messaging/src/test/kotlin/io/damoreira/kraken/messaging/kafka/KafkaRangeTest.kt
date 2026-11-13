package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.ReplayRange
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull

class KafkaRangeTest {
    @Test
    fun `a range is the partition and offsets in decimal, both ways`() {
        val range = ReplayRange("kraken.events", "2", "5", "9")
        assertEquals(KafkaRange("kraken.events", 2, 5, 9), KafkaRange.of(range, "kraken.events"))
        assertEquals(range, KafkaRange("kraken.events", 2, 5, 9).range)
    }

    @Test
    fun `a range of another topic, or not in decimal, is not Kafka's`() {
        val range = ReplayRange("kraken.events", "2", "5", "9")
        assertNull(KafkaRange.of(range, "other.events"))
        assertNull(KafkaRange.of(range.copy(segment = "kraken.events-2"), "kraken.events"))
        assertNull(KafkaRange.of(range.copy(from = "a"), "kraken.events"))
        assertNull(KafkaRange.of(range.copy(until = ""), "kraken.events"))
    }
}
