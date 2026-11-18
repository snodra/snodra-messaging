package com.damoreira.snodra.messaging.store

import java.time.Instant

/**
 * The idempotency marker of a handled event (`processed_events`): [id] is the marker id (`processor:<name>:<eventId>`,
 * `saga:<type>:<eventId>`, `ingress:<source>:<key>`, ...), [type] the event's type, [processedAt] when it was handled (the
 * retention counts from it). A stage-1 marker of a delivered event also carries its [processor], [source], [segment] and
 * [position] (P6b, ADR 0023: the event store's opaque strings), so a reset deletes exactly the markers of its replay range
 * ([ProcessorControlStore.deleteMarkers]); every other marker leaves them null and is never deleted by a reset.
 */
data class ProcessedMarker(
    val id: String,
    val type: String,
    val processedAt: Instant,
    val processor: String? = null,
    val source: String? = null,
    val segment: String? = null,
    val position: String? = null,
)

/** The processed-event markers: what makes at-least-once delivery apply an event once. */
interface ProcessedEventStore {
    /**
     * Records [marker], joining the transaction active on the calling thread (see [MessageStore]), so a handler that fails
     * rolls its marker back with it. False when a marker with that id is recorded already (the event was handled): the
     * caller then rolls its transaction back (a database may have aborted it on the duplicate).
     */
    suspend fun record(marker: ProcessedMarker): Boolean

    /** Whether a marker with [id] is recorded. */
    suspend fun exists(id: String): Boolean

    /** Deletes the markers processed before [before]; returns how many. */
    suspend fun purge(before: Instant): Long
}
