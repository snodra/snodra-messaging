package io.damoreira.kraken.messaging

import java.time.Duration

/**
 * How one streaming query is sent ([QueryBus.stream], P4b, ADR 0017). Every field is optional.
 *
 * @property timeout the **idle** timeout: the longest the hub waits for the next part (and for the handler to start);
 *   it does not run while the collector holds the handler at the window (credit 0). Sent whole as `timeout_ms` on
 *   every attempt (the hub clamps it). Same defaults as [SendOptions.timeout]: `kraken.messaging.hub.request-timeout`,
 *   else the hub decides; zero counts as unset. With retry it is also the budget of the attempts before the first part
 *   (and, with several hubs, of the wait for one). Expiry ends the stream with a [MessageTimeoutException].
 * @property maxResults end the stream after this many parts (`Request.max_results`, enforced by the hub, which then
 *   cancels the handler); 0 = until the handler's flow completes.
 * @property priority `Request.priority` of the request forwarded to the handler. Default:
 *   `kraken.messaging.hub.priorities.<type>`, else 0.
 * @property initialCredit the window (`Request.initial_credit`): how many parts the handler may send before the
 *   collector consumed any. The library grants `initialCredit / 2` more each time half of the window was consumed, so a
 *   collector that keeps up never stalls the handler, and a slow one holds it at the window. At least 1.
 * @property retry the [RetryPolicy], as [SendOptions.retry]; a stream is retried only until its first part.
 */
data class StreamOptions(
    val timeout: Duration? = null,
    val maxResults: Int = 0,
    val priority: Int? = null,
    val initialCredit: Int = 64,
    val retry: RetryPolicy? = null,
) {
    init {
        require(maxResults >= 0) { "maxResults must not be negative, was $maxResults" }
        require(initialCredit >= 1) { "initialCredit must be at least 1, was $initialCredit" }
    }
}
