package io.damoreira.kraken.messaging.control

import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.eventstore.ResetTarget

/** What a `@ResetHandler` is told (P6b): the processor, the reset's operation id, its [target] and the [ranges] it replays (never empty). */
data class ResetContext(
    val processor: String,
    val operationId: String,
    val target: ResetTarget,
    val ranges: List<ReplayRange>,
)
