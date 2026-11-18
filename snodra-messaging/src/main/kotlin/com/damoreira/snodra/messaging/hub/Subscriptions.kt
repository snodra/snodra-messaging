package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.Request
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Where the parts and the end of one served subscription go: the hub stream its `<id>#n` request arrived on. Both calls
 * return false when that stream is closed (the subscription is then dropped).
 */
internal interface SubscriptionSink {
    fun part(requestId: String, seq: Long, payload: ByteArray): Boolean
    fun end(requestId: String, parts: Int, error: Error?): Boolean
}

/**
 * Admits one `mode = SUBSCRIPTION` request this instance was chosen for (P4c): validates it like a handler call and, when
 * it is acceptable, calls `register` with the decoded query. Returns null when the subscription is registered, or the
 * error that ends it (`ReplyEnd{error}`): the pre-checks' or `register`'s (the instance's subscription limit).
 */
typealias SubscriptionInbound = suspend (request: Request, register: (query: Any) -> Error?) -> Error?

/** The [SubscriptionInbound] of an instance without subscription handlers: every subscription ends with `INVALID_REQUEST`. */
internal val NO_SUBSCRIPTION_HANDLERS: SubscriptionInbound = { request, _ ->
    Error.newBuilder().setRequestId(request.id).setCode(ErrorCode.INVALID_REQUEST)
        .setMessage("no subscription handler for ${request.type} on this instance").build()
}

/**
 * One subscription this instance serves: the decoded [query] (what the emitter's filter looks at), the hub stream's [sink],
 * and the `seq` of the next part (1, 2, ... per target; the hub renumbers). Parts and the end are written under the
 * subscription's own lock, so their order is the order of the calls.
 *
 * It is **registered when its request is read**, before the admission (token, pre-checks, decoding) has run: it is then
 * *pending* and has no [query] yet (the emitter decodes [payload] to match it). The hub writes `<id>#n` before the initial
 * request `<id>#0` to the same instance, so an update emitted after the initial result was read may arrive while the
 * admission still runs: such updates (and an end) are buffered in order, [activate] writes them before any later one, and
 * [refuse], [close] (a hub `Cancel`, a lost stream) drop them.
 */
internal class ServedSubscription private constructor(
    val owner: Any,
    val requestId: String,
    val type: String,
    @Volatile private var decoded: Any?,
    /** The request's payload, while the subscription is pending (the query is not decoded yet); null when it was registered decoded. */
    val payload: ByteArray?,
    private val sink: SubscriptionSink,
) {
    constructor(owner: Any, requestId: String, type: String, query: Any, sink: SubscriptionSink) :
        this(owner, requestId, type, query, null, sink)

    /** The decoded query once the subscription is admitted; null while it is pending. */
    val query: Any? get() = decoded

    private sealed interface Held {
        class Update(val payload: ByteArray) : Held
        class End(val error: Error?) : Held
    }

    private var seq = 0L
    private var ended = false // nothing more is accepted
    private var dropped = false // nothing more is written, whatever is held
    private var held: MutableList<Held>? = if (payload != null) ArrayList() else null // non-null = pending

    /** Writes one update (holds it while pending); false when the subscription ended or its stream is closed. */
    @Synchronized fun update(payload: ByteArray): Boolean {
        if (ended) return false
        held?.let { it += Held.Update(payload); return true }
        return write(payload)
    }

    /** Writes the end (clean, or with [error]; holds it while pending); false when it had ended already. */
    @Synchronized fun end(error: Error?): Boolean {
        if (ended) return false
        ended = true
        held?.let { it += Held.End(error); return true }
        writeEnd(error)
        return true
    }

    /** Ends it without writing anything (the hub cancelled it, or its stream is gone). */
    @Synchronized fun close() {
        ended = true
        dropped = true
        held = null
    }

    /**
     * The admission succeeded: records the decoded [query], writes what was held back in order and lets later calls write
     * directly. False when it was dropped meanwhile (nothing is written).
     */
    @Synchronized fun activate(query: Any): Boolean {
        if (dropped) return false
        decoded = query
        val pendingWrites = held ?: return true
        held = null
        for (item in pendingWrites) {
            when (item) {
                is Held.Update -> if (!write(item.payload)) return true
                is Held.End -> writeEnd(item.error)
            }
        }
        return true
    }

    /** The admission failed: what was held is dropped and the refusal [error] is the only thing written. False when it was dropped meanwhile. */
    @Synchronized fun refuse(error: Error): Boolean {
        if (dropped) return false
        ended = true
        dropped = true
        held = null
        sink.end(requestId, 0, error)
        return true
    }

    private fun write(payload: ByteArray): Boolean {
        if (sink.part(requestId, ++seq, payload)) return true
        ended = true
        return false
    }

    private fun writeEnd(error: Error?) {
        sink.end(requestId, seq.coerceAtMost(Int.MAX_VALUE.toLong()).toInt(), error)
    }

    companion object {
        /** A subscription registered before its admission: it has no query yet, the emitter matches it by decoding [payload]. */
        fun pending(owner: Any, requestId: String, type: String, payload: ByteArray, sink: SubscriptionSink) =
            ServedSubscription(owner, requestId, type, null, payload, sink)
    }
}

/** The outcome of [SubscriptionRegistry.openPending]. */
internal sealed interface Opening {
    /** Registered, pending: [SubscriptionRegistry.activate] or [SubscriptionRegistry.refuse] settles it. */
    class Pending(val subscription: ServedSubscription) : Opening

    /** Not registered: the `Error` that ends it (the limit, or a duplicate id). */
    class Refused(val error: Error) : Opening
}

/**
 * The subscription queries (P4c) this instance serves, over every hub it is connected to: the [QueryUpdateEmitter]
 * delivers to them, a hub `Cancel` removes one, a lost hub stream removes the ones it carried. At most [maxSubscriptions]
 * are open at once (`snodra.messaging.hub.max-subscriptions`): the next one is refused with `RESOURCE_EXHAUSTED`. A
 * subscription holds no handler permit (`HandlerGate`): it is a standing interest, not work in progress. A subscription is registered when its request is read (pending, see [ServedSubscription]) and counts against the limit from then on.
 */
class SubscriptionRegistry(val maxSubscriptions: Int = DEFAULT_MAX_SUBSCRIPTIONS) {
    private val log = LoggerFactory.getLogger(SubscriptionRegistry::class.java)

    private data class Key(val owner: Any, val requestId: String)

    private val open = ConcurrentHashMap<Key, ServedSubscription>()

    init {
        require(maxSubscriptions >= 1) { "maxSubscriptions must be >= 1, was $maxSubscriptions" }
    }

    /** Subscriptions open on this instance, over every hub. */
    val size: Int get() = open.size

    /**
     * Registers the subscription [requestId] (the hub's `<id>#n`) that arrived on [owner]'s stream, already decoded. Null when it
     * is registered, else the `Error` that ends it: the limit (`RESOURCE_EXHAUSTED`) or a duplicate id.
     */
    internal fun open(owner: Any, requestId: String, type: String, query: Any, sink: SubscriptionSink): Error? =
        register(owner, requestId) { ServedSubscription(owner, requestId, type, query, sink) }.let { (it as? Opening.Refused)?.error }

    /**
     * Registers the subscription [requestId] when its request is read, **pending** (see [ServedSubscription]): from now on the
     * emitter's updates for it are held in order, until [activate] (the admission succeeded) or [refuse]; a hub `Cancel` or a
     * lost stream ([cancel], [closeOwner]) drops them. It counts against the limit at once.
     */
    internal fun openPending(owner: Any, requestId: String, type: String, payload: ByteArray, sink: SubscriptionSink): Opening =
        register(owner, requestId) { ServedSubscription.pending(owner, requestId, type, payload, sink) }

    /** The admission of [subscription] succeeded: it becomes live and what was held is written. False when it was dropped meanwhile. */
    internal fun activate(subscription: ServedSubscription, query: Any): Boolean = subscription.activate(query)

    /** The admission of [subscription] failed with [error]: it is removed, what was held is dropped and only the refusal is written (unless it was dropped meanwhile). */
    internal fun refuse(subscription: ServedSubscription, error: Error): Boolean {
        remove(subscription)
        return subscription.refuse(error)
    }

    private fun register(owner: Any, requestId: String, create: () -> ServedSubscription): Opening {
        val key = Key(owner, requestId)
        synchronized(this) {
            if (open.containsKey(key)) {
                return Opening.Refused(error(requestId, ErrorCode.INVALID_REQUEST, "subscription $requestId is already open"))
            }
            if (open.size >= maxSubscriptions) {
                return Opening.Refused(error(requestId, ErrorCode.RESOURCE_EXHAUSTED, "subscription limit ($maxSubscriptions) reached"))
            }
            return Opening.Pending(create().also { open[key] = it })
        }
    }

    /** The hub cancelled [requestId]: it is removed, nothing is written. False when it was not open. */
    internal fun cancel(owner: Any, requestId: String): Boolean =
        open.remove(Key(owner, requestId))?.also { it.close() } != null

    /** [owner]'s stream is gone (the hub forgot its subscriptions): they are all removed. Returns how many. */
    internal fun closeOwner(owner: Any): Int {
        var closed = 0
        for ((key, subscription) in open.entries.toList()) {
            if (key.owner === owner && open.remove(key, subscription)) {
                subscription.close()
                closed++
            }
        }
        return closed
    }

    /** The open subscriptions of [type], in no particular order. */
    internal fun of(type: String): List<ServedSubscription> = open.values.filter { it.type == type }

    /** Removes [subscription] (it ended or its stream is closed). */
    internal fun remove(subscription: ServedSubscription) {
        open.remove(Key(subscription.owner, subscription.requestId), subscription)
    }

    /**
     * Ends every subscription with a clean `ReplyEnd`, best effort (this instance is shutting down): to the hub this
     * target's emitter completed; the subscriber's `updates` completes once every other target has too. Returns how many
     * subscriptions it ended.
     */
    fun endAll(): Int {
        var ended = 0
        for ((key, subscription) in open.entries.toList()) {
            if (open.remove(key, subscription)) {
                ended++
                runCatching { subscription.end(null) }.onFailure { log.debug("Ending subscription {} failed: {}", key.requestId, it.toString()) }
            }
        }
        return ended
    }

    private fun error(requestId: String, code: ErrorCode, message: String): Error =
        Error.newBuilder().setRequestId(requestId).setCode(code).setMessage(message).build()

    companion object {
        /** Default of `snodra.messaging.hub.max-subscriptions`, the hub's default per-instance limit. */
        const val DEFAULT_MAX_SUBSCRIPTIONS = 1024
    }
}
