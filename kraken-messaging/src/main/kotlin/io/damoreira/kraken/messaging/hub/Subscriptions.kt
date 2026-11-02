package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Request
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
 */
internal class ServedSubscription(
    val owner: Any,
    val requestId: String,
    val type: String,
    val query: Any,
    private val sink: SubscriptionSink,
) {
    private var seq = 0L
    private var ended = false

    /** Writes one update; false when the subscription ended or its stream is closed. */
    @Synchronized fun update(payload: ByteArray): Boolean {
        if (ended) return false
        if (sink.part(requestId, ++seq, payload)) return true
        ended = true
        return false
    }

    /** Writes the end (clean, or with [error]); false when it had ended already. */
    @Synchronized fun end(error: Error?): Boolean {
        if (ended) return false
        ended = true
        sink.end(requestId, seq.coerceAtMost(Int.MAX_VALUE.toLong()).toInt(), error)
        return true
    }

    /** Ends it without writing anything (the hub cancelled it, or its stream is gone). */
    @Synchronized fun close() {
        ended = true
    }
}

/**
 * The subscription queries (P4c) this instance serves, over every hub it is connected to: the [QueryUpdateEmitter]
 * delivers to them, a hub `Cancel` removes one, a lost hub stream removes the ones it carried. At most [maxSubscriptions]
 * are open at once (`kraken.messaging.hub.max-subscriptions`): the next one is refused with `RESOURCE_EXHAUSTED`. A
 * subscription holds no handler permit (`HandlerGate`): it is a standing interest, not work in progress.
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
     * Registers the subscription [requestId] (the hub's `<id>#n`) that arrived on [owner]'s stream. Null when it is
     * registered, else the `Error` that ends it: the limit (`RESOURCE_EXHAUSTED`) or a duplicate id.
     */
    internal fun open(owner: Any, requestId: String, type: String, query: Any, sink: SubscriptionSink): Error? {
        val key = Key(owner, requestId)
        synchronized(this) {
            if (open.containsKey(key)) {
                return error(requestId, ErrorCode.INVALID_REQUEST, "subscription $requestId is already open")
            }
            if (open.size >= maxSubscriptions) {
                return error(requestId, ErrorCode.RESOURCE_EXHAUSTED, "subscription limit ($maxSubscriptions) reached")
            }
            open[key] = ServedSubscription(owner, requestId, type, query, sink)
        }
        return null
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
        /** Default of `kraken.messaging.hub.max-subscriptions`, the hub's default per-instance limit. */
        const val DEFAULT_MAX_SUBSCRIPTIONS = 1024
    }
}
