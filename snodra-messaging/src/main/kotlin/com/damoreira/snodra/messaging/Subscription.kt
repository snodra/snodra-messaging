package com.damoreira.snodra.messaging

import kotlinx.coroutines.flow.Flow
import java.time.Duration

/**
 * How one subscription query is sent ([QueryBus.subscribe], P4c, ADR 0017). Every field is optional.
 *
 * @property timeout bounds the **initial result** only (queue wait at the hub plus the handler), sent as `timeout_ms`: same
 *   defaults as [SendOptions.timeout] (`snodra.messaging.hub.request-timeout`, else the hub decides; zero counts as
 *   unset). With retry it is the budget of all attempts. There is no idle timeout: a quiet subscription is not a broken one.
 * @property maxResults end the subscription after this many **updates** (`Request.max_results`, enforced by the hub, which
 *   then cancels the handlers); the initial result does not count. 0 = until the handlers complete or you cancel.
 * @property priority `Request.priority` of the initial request. Default: `snodra.messaging.hub.priorities.<type>`, else 0.
 * @property initialCredit the window (`Request.initial_credit`): how many parts (the initial result included) the hub may
 *   relay before the collector consumed any. The library grants `initialCredit / 2` more each time half of the window was
 *   consumed, the initial result counting as consumed once [QueryBus.subscribe] returned it; updates beyond the window wait
 *   at the hub (at most `SNODRA_SERVER_SUBSCRIPTION_BUFFER`, 256: one more ends the subscription with a
 *   [HandlerBusyException]). At least 1.
 * @property retry the [RetryPolicy], as [SendOptions.retry]; a subscription is retried only until its initial result.
 */
data class SubscriptionOptions(
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

/**
 * An open subscription ([QueryBus.subscribe]): the [initial] result, already received, and the [updates] that follow.
 *
 * [updates] is a **hot** flow fed by the hub stream: the hub relays updates as they arrive (up to the credit window),
 * whether or not anyone collects, and they are delivered to the collector in order. It can be collected **once**. It
 * completes when the handlers complete (a clean end, or `maxResults` updates), throws when the subscription ends with an
 * error (mapped like a unary error with the same hub code: [HandlerBusyException] for `RESOURCE_EXHAUSTED`,
 * [HandlerDisconnectedException] when the last handler instance left, [StreamCancelledException] for the hub's maximum
 * subscription duration, [RemoteHandlerException] when a handler's emitter failed, ...) and throws
 * [HubConnectionLostException] when the stream to the hub breaks. Cancelling the collector (`take(n)`, a cancelled
 * coroutine) or throwing in it cancels the subscription, as does [cancel], which also completes [updates].
 *
 * A result that is dropped without being collected or cancelled is not cleaned up: the hub keeps the subscription (and
 * buffers its updates, up to its limit, then ends it) until the sender disconnects.
 */
class SubscriptionResult<I, U>(
    val initial: I,
    val updates: Flow<U>,
    private val onCancel: () -> Unit = {},
) {
    /** Cancels the subscription: `Cancel` goes to the hub once and [updates] completes. Idempotent. */
    fun cancel() = onCancel()
}
