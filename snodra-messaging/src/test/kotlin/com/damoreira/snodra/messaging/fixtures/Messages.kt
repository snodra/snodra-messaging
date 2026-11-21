package com.damoreira.snodra.messaging.fixtures

import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.api.Query
import com.damoreira.snodra.messaging.api.StreamingQuery
import com.damoreira.snodra.messaging.api.SubscriptionQuery
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

/** A streaming query: [count] rows, failing with `IllegalStateException` before row [failAt] when it is set. */
@MessageType("test.ExportRows")
data class ExportRows(val count: Int, val failAt: Int = -1) : StreamingQuery<Row>

data class Row(val n: Int)

/** A subscription query: the balance of [account] now, then every change to it. */
@MessageType("test.WatchAccount")
data class WatchAccount(val account: Int) : SubscriptionQuery<Balance, BalanceChanged>

data class Balance(val value: Int)

data class BalanceChanged(val account: Int, val value: Int)
