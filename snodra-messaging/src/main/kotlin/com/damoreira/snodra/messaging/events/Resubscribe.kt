package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.eventstore.Subscription
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import java.time.Duration

/**
 * How a reader of the event store (an event processor, an external source) keeps a subscription (ADR 0027): it subscribes, reads
 * until it stops or the subscription fails ([com.damoreira.snodra.messaging.eventstore.EventStoreException.Failed] from `next`
 * ends a subscription), closes it, and subscribes again after a backoff: [min] doubling up to [max] while the subscriptions keep
 * failing before they delivered anything; it starts again from [min] after a subscription that delivered an event or lived
 * [stable] (an assignment alone does not count). Stops when the calling coroutine is cancelled; a subscribe in flight then
 * completes, and the subscription it returns is closed without being read.
 */
class Resubscribe(
    private val min: Duration = MIN,
    private val max: Duration = MAX,
    private val stable: Duration = STABLE,
) {
    /** How one subscription ended. */
    sealed interface Ended {
        /** The reader stops. */
        data object Stopped : Ended

        /** The subscription failed with [error]; [delivered]: it handed out an event first. */
        class Failed(val error: Throwable, val delivered: Boolean) : Ended
    }

    /**
     * Runs [read] over a subscription from [subscribe] at a time until [read] returns [Ended.Stopped] or the coroutine is
     * cancelled. Each failure (of [subscribe] or of [read]'s subscription) is told to [failed] with the wait before the next try.
     */
    suspend fun run(
        subscribe: suspend () -> Subscription,
        read: suspend (Subscription) -> Ended,
        failed: (error: Throwable, wait: Duration) -> Unit,
    ) {
        var wait = min
        while (true) {
            val started = System.nanoTime()
            var subscription: Subscription? = null
            val ended: Ended.Failed = try {
                // Not cancellable: a subscription built while the reader was stopped is still closed below (the stop surfaces
                // at the first next()), never left behind holding its segments.
                withContext(NonCancellable) { subscription = subscribe() }
                // A stop during the subscribe surfaces here, before the first next(): a next() with events ready would hand
                // them out without suspending, so without this check the stopped reader would read them.
                currentCoroutineContext().ensureActive()
                when (val end = read(subscription!!)) {
                    Ended.Stopped -> return
                    is Ended.Failed -> end
                }
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                Ended.Failed(e, delivered = false)
            } finally {
                subscription?.let { withContext(NonCancellable) { runCatching { it.close() } } }
            }
            if (ended.delivered || Duration.ofNanos(System.nanoTime() - started) >= stable) wait = min
            failed(ended.error, wait)
            delay(wait.toMillis())
            wait = minOf(wait.multipliedBy(2), max)
        }
    }

    companion object {
        /** The first wait before subscribing again after a failure. */
        val MIN: Duration = Duration.ofSeconds(1)

        /** The longest wait. */
        val MAX: Duration = Duration.ofSeconds(30)

        /** A subscription that lived this long before it failed counts as a good one: the next wait starts at [MIN]. */
        val STABLE: Duration = Duration.ofSeconds(30)
    }
}
