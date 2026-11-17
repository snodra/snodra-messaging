package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.Request
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.update

/**
 * Serves one streaming request this instance was chosen for (`mode = STREAMING`, P4b): writes every part through
 * `emit` (which suspends until the hub granted credit for it, so a flow that produces faster than the sender consumes is
 * held back inside its `emit`) and returns null when the handler's flow completed, or the error that ends the stream
 * (`ReplyEnd.error`). It is cancelled when the hub sends `Cancel` for the request, or its hub stream closes.
 */
typealias StreamingInbound = suspend (request: Request, emit: suspend (payload: ByteArray) -> Unit) -> Error?

/**
 * What [HubTransport.stream] throws when the caller's `onPart` callback threw (the collector failed, a part did not
 * decode): [cause] is what it threw. It is the collector giving up, never the hub failing the request: the transport
 * writes `Cancel` for the stream and a multi-hub transport never moves the request to another hub (it was written).
 * [HubBus] unwraps it. A `CancellationException` from the callback is not wrapped: it is the collector's cancellation.
 */
internal class StreamCollectorException(cause: Throwable) : RuntimeException(cause)

/** The [StreamingInbound] of an instance without streaming handlers: every stream ends with `INVALID_REQUEST`. */
internal val NO_STREAMING_HANDLERS: StreamingInbound = { request, _ ->
    Error.newBuilder().setRequestId(request.id).setCode(ErrorCode.INVALID_REQUEST)
        .setMessage("no streaming handler for ${request.type} on this instance").build()
}

/**
 * The credit the hub granted for one outbound stream: `initial_credit` plus every `Flow`. [take] spends one and
 * suspends while none is left, which is how a handler's flow is held back by the sender's back-pressure.
 *
 * `initial_credit` and `Flow.credits` are `uint32` on the wire: a Rust caller may send `u32::MAX`, which protobuf's
 * Java binding hands over as -1. Both are read unsigned ([of]) and the window is clamped to [Int.MAX_VALUE].
 */
internal class StreamCredit(initial: Int) {
    private val credit = MutableStateFlow(of(initial))

    /** Parts the hub will still relay. */
    val available: Long get() = credit.value

    fun add(credits: Int) {
        val granted = of(credits)
        if (granted > 0) credit.update { (it + granted).coerceAtMost(Int.MAX_VALUE.toLong()) }
    }

    /** A wire `uint32` credit value as the window it means: unsigned, at most [Int.MAX_VALUE]. */
    private fun of(wire: Int): Long = Integer.toUnsignedLong(wire).coerceAtMost(Int.MAX_VALUE.toLong())

    /** Spends one credit, suspending until one is available. */
    suspend fun take() {
        while (true) {
            val now = credit.value
            if (now > 0) {
                if (credit.compareAndSet(now, now - 1)) return
            } else {
                credit.first { it > 0 }
            }
        }
    }
}
