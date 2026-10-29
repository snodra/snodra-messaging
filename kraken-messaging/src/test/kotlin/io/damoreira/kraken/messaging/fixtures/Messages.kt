package io.damoreira.kraken.messaging.fixtures

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
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
