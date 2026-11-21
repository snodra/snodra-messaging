package com.damoreira.snodra.messaging.control

import com.damoreira.snodra.messaging.eventstore.ReplayRange
import com.damoreira.snodra.messaging.eventstore.ResetTarget

/** What a `@ResetHandler` is told: the processor, the reset's operation id, its [target] and the [ranges] it replays (never empty). */
data class ResetContext(
    val processor: String,
    val operationId: String,
    val target: ResetTarget,
    val ranges: List<ReplayRange>,
)
