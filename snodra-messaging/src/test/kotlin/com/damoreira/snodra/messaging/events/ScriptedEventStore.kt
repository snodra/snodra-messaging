package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.eventstore.Capability
import com.damoreira.snodra.messaging.eventstore.Delivery
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.EventStoreException
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
import com.damoreira.snodra.messaging.eventstore.SegmentOrder
import com.damoreira.snodra.messaging.eventstore.SegmentStatus
import com.damoreira.snodra.messaging.eventstore.Subscription
import com.damoreira.snodra.messaging.eventstore.SubscriptionAdmin
import com.damoreira.snodra.messaging.eventstore.SubscriptionEvent
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.channels.Channel
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * An event store the test drives by hand, for the core's tests without a broker: each [subscribe] makes a [Scripted]
 * subscription the test feeds ([Scripted.send], [Scripted.fail]) and reads back (acknowledgements, closes). Positions are decimal.
 */
class ScriptedEventStore(
    override val name: String = "scripted",
    override val capabilities: Set<Capability> = setOf(Capability.LAG),
) : EventStore {
    val subscriptions = CopyOnWriteArrayList<Scripted>()

    /** When each [subscribe] was called, [System.nanoTime]. */
    val subscribedAt = CopyOnWriteArrayList<Long>()

    /** How many of the next [subscribe] calls fail. */
    val failSubscribe = AtomicInteger(0)

    /**
     * When set, [subscribe] builds the subscription and then waits for it before returning: a naive implementation, cancellable
     * after it built one (what the core must close all the same).
     */
    @Volatile var subscribeGate: CompletableDeferred<Unit>? = null

    /** The events published; while [failPublish] is set, a publish fails. */
    val published = CopyOnWriteArrayList<OutgoingEvent>()
    @Volatile var failPublish = false

    override suspend fun publish(event: OutgoingEvent) {
        if (failPublish) throw EventStoreException.Failed("cannot reach the event store")
        published += event
    }

    override suspend fun subscribe(spec: SubscriptionSpec): Subscription {
        subscribedAt += System.nanoTime()
        if (failSubscribe.getAndDecrement() > 0) throw EventStoreException.Failed("cannot reach the event store")
        val built = Scripted(spec).also { subscriptions += it }
        subscribeGate?.await()
        return built
    }

    override fun admin(spec: SubscriptionSpec): SubscriptionAdmin = throw EventStoreException.Unsupported(Capability.REPLAY)

    override fun compare(a: String, b: String): Int = a.toLong().compareTo(b.toLong())

    /** One subscription: what [next] gives comes from [send] and [fail]; a failure stays (every later [next] throws it). */
    class Scripted(val spec: SubscriptionSpec) : Subscription {
        /** What [next] hands out, in order: a [SubscriptionEvent] or an [EventStoreException] (which then stays). */
        private val events = Channel<Any>(Channel.UNLIMITED)
        private val failure = AtomicReference<EventStoreException?>(null)
        private val assigned = ConcurrentSkipListMap<String, SegmentStatus>(SegmentOrder)
        val acks = CopyOnWriteArrayList<Pair<String, String>>()
        val redelivered = CopyOnWriteArrayList<Pair<String, String>>()
        val errors = CopyOnWriteArrayList<String>()
        @Volatile var closed = false
        @Volatile var paused = false

        fun send(event: SubscriptionEvent) {
            events.trySend(event)
        }

        fun assign(vararg segments: String) = send(SubscriptionEvent.Assigned(segments.toList()))

        fun deliver(segment: String, position: String, type: String, id: String = "e-$position", payload: ByteArray = "{}".toByteArray()) =
            send(Delivery(segment, position, "k", mapOf("type" to type, "id" to id), payload, 0L))

        /** After what was sent before, [next] throws [error] (not an event-store failure: the reader's own end). */
        fun failWith(error: Error) {
            events.trySend(error)
        }

        /** After what was sent before, [next] fails with [why], then every later call too. */
        fun fail(why: String) {
            events.trySend(EventStoreException.Failed(why))
        }

        override suspend fun next(): SubscriptionEvent {
            failure.get()?.let { throw it }
            return when (val item = events.receive()) {
                is EventStoreException -> {
                    failure.set(item)
                    throw item
                }
                is Error -> throw item
                is SubscriptionEvent.Assigned -> item.also { it.segments.forEach { s -> assigned[s] = SegmentStatus(s) } }
                is SubscriptionEvent.Revoked -> item.also { it.segments.forEach { s -> assigned.remove(s) } }
                else -> item as SubscriptionEvent
            }
        }

        override fun ack(segment: String, position: String) {
            acks += segment to position
            assigned[segment]?.let { assigned[segment] = it.copy(committed = (position.toLong() + 1).toString()) }
        }

        override fun redeliver(segment: String, position: String) {
            redelivered += segment to position
        }

        override fun pause() {
            paused = true
        }

        override fun resume() {
            paused = false
        }

        override fun segments(): List<SegmentStatus> = assigned.values.toList()

        override fun takeError(): String? = errors.removeFirstOrNull()

        override suspend fun close() {
            closed = true
        }
    }
}
