package io.damoreira.kraken.examples.api

import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.StreamingQuery

/*
 * A shared streaming query type (ADR 0017, P4b): service-b (Kotlin) and service-c (Rust) both own it, so a stream goes
 * to one instance of either. Mirrored in kraken-messaging-rs/examples/service-c/src/messages.rs; keep both in step.
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
