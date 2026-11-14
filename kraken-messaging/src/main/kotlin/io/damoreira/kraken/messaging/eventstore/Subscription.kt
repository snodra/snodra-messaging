package io.damoreira.kraken.messaging.eventstore

/**
 * One instance's subscription of a processor ([EventStore.subscribe]): the segments the event store assigns it and their
 * events, one at a time.
 *
 * - **Driven by [next].** Nothing is delivered until [next] is called, and the core keeps calling it while paused: that keeps
 *   the subscription's membership alive and reports assignment changes.
 * - **Order per segment.** A segment's events come in their order, each after the [SubscriptionEvent.Assigned] naming its
 *   segment and before any [SubscriptionEvent.Revoked] of it.
 * - **At-least-once.** An event is acknowledged with [ack] after its handling committed; one not acknowledged comes again to
 *   the next subscription of the same name that gets its segment (after a [close], a crash or a reassignment), or to this one
 *   after [redeliver].
 * - **Failure.** A [EventStoreException.Failed] thrown by [next] ends the subscription: it delivers nothing more. The caller [closes][close]
 *   it and subscribes again, with a backoff of its own (what it had not acknowledged comes again, as after a crash).
 * - **Threads.** [next], [ack], [redeliver] and [close] are called by one coroutine at a time (the processor's loop); [pause],
 *   [resume] and [segments] may be called from any thread at any time (the control plane, the status report).
 */
interface Subscription {
    /**
     * The next assignment change or delivery; suspends until there is one. Cancellable: cancelling the call before it returns
     * loses nothing (the core races it with its shutdown and its timers).
     */
    suspend fun next(): SubscriptionEvent

    /**
     * Acknowledges the delivery at [position] of [segment] and every earlier one of that segment: a later subscription of this
     * name resumes after it. May reach the event store after the call returns; [close] flushes it.
     */
    fun ack(segment: String, position: String)

    /**
     * Hands the delivery at [position] of [segment] out again, without leaving the subscription: [next] delivers it and that
     * segment's later events again (what the core does with an event it could not finish, e.g. while its dead letters cannot
     * take it). Other segments are unaffected.
     */
    fun redeliver(segment: String, position: String)

    /**
     * Stops delivering without losing the place: [next] gives assignment changes only, and a segment assigned while paused is
     * paused at once. Idempotent; called before the first [next], nothing is ever delivered until [resume].
     */
    fun pause()

    /** Delivers again from where the pause stopped. Idempotent. */
    fun resume()

    /**
     * The segments assigned now, in segment order ([SegmentOrder]: names of ASCII digits by numeric value, then other names by bytes), with their positions and lag as last known (refreshed by the subscription
     * itself; never a blocking call).
     */
    fun segments(): List<SegmentStatus>

    /** Leaves the subscription after flushing the acknowledgements: its segments go to the other subscriptions of the name. */
    suspend fun close()
}
