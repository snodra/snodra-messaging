package io.damoreira.kraken.messaging.store

import java.time.Instant

/**
 * The idempotency marker of a handled event (`processed_events`): [id] is the marker id (`processor:<name>:<eventId>`,
 * `saga:<type>:<eventId>`, `ingress:<source>:<key>`, ...), [type] the event's type, [processedAt] when it was handled (the
 * retention counts from it). A stage-1 marker of a consumed record also carries the record's [processor], [topic],
 * [partition] and [offset] (P6b, ADR 0023), so a reset deletes exactly the markers of its replay range
 * ([ProcessorControlStore.deleteMarkers]); every other marker leaves them null.
 */
data class ProcessedMarker(
    val id: String,
    val type: String,
    val processedAt: Instant,
    val processor: String? = null,
    val topic: String? = null,
    val partition: Int? = null,
    val offset: Long? = null,
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
