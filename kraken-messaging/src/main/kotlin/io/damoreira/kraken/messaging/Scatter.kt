package io.damoreira.kraken.messaging

import java.time.Duration

/**
 * How one scatter-gather query is sent ([QueryBus.scatterGather]). Every field is optional.
 *
 * @property timeout the deadline of the whole gather, sent as `timeout_ms` (the hub clamps it): targets that have not
 *   answered by then are counted in [ScatterResult.timedOut]. Same rules as [SendOptions.timeout]: default
 *   `kraken.messaging.hub.request-timeout`, else the hub decides; zero counts as unset; with retry it is the budget of
 *   all attempts.
 * @property maxResults end the gather after this many successful parts (`Request.max_results`); 0 = wait for every
 *   target.
 * @property priority `Request.priority` of every target request. Default: `kraken.messaging.hub.priorities.<type>`,
 *   else 0.
 * @property retry the [RetryPolicy] for the whole gather, as [SendOptions.retry].
 */
data class ScatterOptions(
    val timeout: Duration? = null,
    val maxResults: Int = 0,
    val priority: Int? = null,
    val retry: RetryPolicy? = null,
) {
    init {
        require(maxResults >= 0) { "maxResults must not be negative, was $maxResults" }
    }
}

/**
 * One answer of a scatter-gather: [result] when the target answered, [error] when it failed (its handler's error, or a
 * hub-side failure of that target such as `HANDLER_DISCONNECTED`; the hub's code is [RemoteHandlerException.code]).
 *
 * @property service the answering service (`ReplyPart.responder_service`)
 * @property instance the answering instance (`ReplyPart.responder_instance`)
 */
data class ScatterPart<R>(
    val service: String,
    val instance: String,
    val result: R?,
    val error: RemoteHandlerException?,
)

/**
 * The outcome of a scatter-gather: every part received, successes and failures, in arrival order.
 *
 * @property failed parts that carry an error (they are in [parts] too)
 * @property timedOut targets that had not answered by the deadline (they have no part)
 */
data class ScatterResult<R>(
    val parts: List<ScatterPart<R>>,
    val failed: Int,
    val timedOut: Int,
) {
    /** The results of the successful parts, in arrival order. */
    val results: List<R> get() = parts.mapNotNull { it.result }
}
