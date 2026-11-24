package com.damoreira.snodra.examples.api

import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.api.StreamingQuery

/*
 * A shared streaming query type: service-b (Kotlin) and service-c both own it, so a stream goes
 * to one instance of either. Service-c keeps a mirror of these types; keep both in step.
 */

/**
 * Streaming query owned by service-b and service-c: the answering instance streams [count] rows numbered 1..count. With
 * [failAt] > 0 the handler fails on reaching that row (rows 1..failAt-1 were streamed), so the collector sees the
 * handler's error after a partial stream.
 */
@MessageType("examples.Rows")
data class Rows(val count: Int, val failAt: Int = 0) : StreamingQuery<Row>

/** One element of a [Rows] stream: the answering [service], its [instance] (the container's host name) and the row number [n]. */
data class Row(val service: String, val instance: String, val n: Int)
