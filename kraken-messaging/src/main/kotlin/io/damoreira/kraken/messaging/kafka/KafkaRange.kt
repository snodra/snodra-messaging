package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.ReplayRange

/**
 * A replay range of [topic] as Kafka reads it: records of [partition] with `from <= offset < until`. Its neutral form
 * ([range]) carries the partition as the segment and the offsets as the positions, in decimal.
 */
internal data class KafkaRange(val topic: String, val partition: Int, val from: Long, val until: Long) {
    val range: ReplayRange get() = ReplayRange(topic, partition.toString(), from.toString(), until.toString())

    companion object {
        /** [range] as a range of [topic], when it is one and its segment and positions are decimal; else null. */
        fun of(range: ReplayRange, topic: String): KafkaRange? {
            if (range.source != topic) return null
            return KafkaRange(
                topic,
                range.segment.toIntOrNull() ?: return null,
                range.from.toLongOrNull() ?: return null,
                range.until.toLongOrNull() ?: return null,
            )
        }
    }
}
