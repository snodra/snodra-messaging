package io.damoreira.kraken.messaging.fixtures

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.StreamingQuery
import java.time.Instant

@MessageType("test.Ping")
data class Ping(val text: String) : Command<Pong>

data class Pong(val echoed: String, val at: Instant)

@MessageType("test.GetCount")
data class GetCount(val key: String) : Query<Count>

data class Count(val value: Int)

data class NoAnnotation(val x: Int) : Command<Unit>

@MessageType("test.Happened")
data class Happened(val id: String, val at: Instant) : Event

/** A streaming query (P4b): [count] rows, failing with `IllegalStateException` before row [failAt] when it is set. */
@MessageType("test.ExportRows")
data class ExportRows(val count: Int, val failAt: Int = -1) : StreamingQuery<Row>

data class Row(val n: Int)
