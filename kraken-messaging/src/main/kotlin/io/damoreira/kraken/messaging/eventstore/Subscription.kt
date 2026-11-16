package io.damoreira.kraken.messaging.eventstore

/**
 * One instance's subscription of a processor ([EventStore.subscribe]): the segments the event store assigns it and their
 * events, one at a time.
 *
 * - **Driven by [next].** Nothing is delivered until [next] is called, and the core keeps calling it while paused: that keeps
 *   the subscription's membership alive and reports assignment changes.
 * - **Order per segment.** A segment's events come in their order, each after the [SubscriptionEvent.Assigned] naming its
 *   segment and before any [SubscriptionEvent.Revoked] of it.
 * - **Every [SubscriptionEvent.Revoked] is followed by a [SubscriptionEvent.Assigned]** (an empty one when nothing new comes):
 *   the assignment change is over only then (the core reports the processor `STARTING` in between). An event store that only
 *   moves segments away (a single active consumer losing one, an instance left with none) still closes the change with
 *   `Assigned([])`.
 * - **The first event is an [SubscriptionEvent.Assigned]**, within [FIRST_ASSIGNMENT_WITHIN] of joining, an empty one when the
 *   instance holds no segment (every segment held by the others): an instance that joined and holds nothing is a member, idle,
 *   and the core reports it `RUNNING`, never `STARTING`. An empty first `Assigned` may be followed by one naming segments; one
 *   naming segments is never followed by a late empty "first" one.
 * - **At-least-once.** An event is acknowledged with [ack] after its handling committed; one not acknowledged comes again to
 *   the next subscription of the same name that gets its segment (after a [close], a crash or a reassignment), or to this one
 *   after [redeliver].
 * - **Failure.** A [EventStoreException.Failed] thrown by [next] ends the subscription: it delivers nothing more. The caller [closes][close]
 *   it and subscribes again, with a backoff of its own (what it had not acknowledged comes again, as after a crash). The failure is
 *   never lost to a cancelled [next]: every later call throws it at once. What the subscription recovers from by itself (it retries
 *   inside [next]) it reports through [takeError].
 * - **Threads.** [next], [ack], [redeliver] and [close] are called by one coroutine at a time (the processor's loop); [pause],
 *   [resume] and [segments] may be called from any thread at any time (the control plane, the status report).
 */
interface Subscription {
    companion object {
        /** The longest a new subscription takes to yield its first event, an [SubscriptionEvent.Assigned] (see the class docs); the conformance kit checks it. */
        val FIRST_ASSIGNMENT_WITHIN: java.time.Duration = java.time.Duration.ofSeconds(10)
    }

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
     * The segments assigned now, in segment order ([SegmentOrder]: names of ASCII digits by numeric value, then other names by
     * bytes), with their positions and lag as last known (refreshed by the subscription itself; never a blocking call).
     */
    fun segments(): List<SegmentStatus>

    /**
     * The latest trouble the subscription recovered from by itself since the last call (a lost connection, a failed reassignment,
     * an error it retried), as text for the processor's status (`last_error`); null when there was none. Never blocking; may be
     * called from any thread.
     */
    fun takeError(): String? = null

    /** Leaves the subscription after flushing the acknowledgements: its segments go to the other subscriptions of the name. */
    suspend fun close()
}
