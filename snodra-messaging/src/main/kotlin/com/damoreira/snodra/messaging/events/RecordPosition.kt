package com.damoreira.snodra.messaging.events

/**
 * Where a delivered event is: the [source] it was read from, its [segment] and [position] there (the event store's opaque
 * strings). Its stage-1 marker carries it (P6b, [com.damoreira.snodra.messaging.store.ProcessedMarker]).
 */
data class RecordPosition(val source: String, val segment: String, val position: String)
