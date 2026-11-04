package io.damoreira.kraken.messaging.control

/** Where a reset moves a processor's consumer group (P6b, ADR 0023). */
sealed interface ResetTarget {
    /** Every partition's low watermark: the whole retained topic is replayed. */
    data object Earliest : ResetTarget

    /** Every partition's high watermark: nothing is replayed, what was not consumed yet is skipped. */
    data object Latest : ResetTarget

    /** Per partition, the first record at or after [timestampMs] (the high watermark where there is none). */
    data class Timestamp(val timestampMs: Long) : ResetTarget

    /** Exactly these offsets by partition; a partition not listed keeps its committed offset. */
    data class Offsets(val offsets: Map<Int, Long>) : ResetTarget
}

/** Records of [topic]'s [partition] with `from <= offset < until` are a replay (P6b): handled again without sagas or `@DisallowReplay` handlers. */
data class ReplayRange(val topic: String, val partition: Int, val from: Long, val until: Long)

/** What a `@ResetHandler` is told (P6b): the processor, the reset's operation id, its [target] and the [ranges] it replays (never empty). */
data class ResetContext(
    val processor: String,
    val operationId: String,
    val target: ResetTarget,
    val ranges: List<ReplayRange>,
)
