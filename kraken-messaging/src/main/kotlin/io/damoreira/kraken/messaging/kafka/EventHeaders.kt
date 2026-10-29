package io.damoreira.kraken.messaging.kafka

/** Kafka record header names of every kraken event. */
object EventHeaders {
    const val TYPE = "type"
    const val ID = "id"
    const val CORRELATION_ID = "correlationId"
    const val TIMESTAMP = "timestamp"
    const val SOURCE = "source"
    const val USER_ID = "userId"
    const val TRACEPARENT = "traceparent"
    const val TRACESTATE = "tracestate"
}
