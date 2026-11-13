package io.damoreira.kraken.messaging.kafka

/** Where a Kafka record is: its stage-1 marker carries it (P6b, [io.damoreira.kraken.messaging.store.ProcessedMarker]). */
data class RecordPosition(val topic: String, val partition: Int, val offset: Long)
