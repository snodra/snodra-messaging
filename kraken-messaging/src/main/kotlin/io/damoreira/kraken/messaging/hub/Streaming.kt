package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Request
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

/** The [StreamingInbound] of an instance without streaming handlers: every stream ends with `INVALID_REQUEST`. */
internal val NO_STREAMING_HANDLERS: StreamingInbound = { request, _ ->
    Error.newBuilder().setRequestId(request.id).setCode(ErrorCode.INVALID_REQUEST)
        .setMessage("no streaming handler for ${request.type} on this instance").build()
}

/**
 * The credit the hub granted for one outbound stream: `initial_credit` plus every `Flow`. [take] spends one and
 * suspends while none is left, which is how a handler's flow is held back by the sender's back-pressure.
 */
internal class StreamCredit(initial: Int) {
    private val credit = MutableStateFlow(initial.toLong())

    /** Parts the hub will still relay. */
    val available: Long get() = credit.value

    fun add(credits: Int) {
        if (credits > 0) credit.update { it + credits }
    }

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
