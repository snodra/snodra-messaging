package io.damoreira.kraken.messaging.hub

import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.sync.Semaphore

/**
 * The instance-wide bound on incoming hub requests: at most [maxConcurrentHandlers] handlers run at once, whichever hub
 * the requests came from. One gate is shared by every [HubConnection] of the instance.
 */
class HandlerGate(val maxConcurrentHandlers: Int) {
    init {
        require(maxConcurrentHandlers >= 1) { "maxConcurrentHandlers must be >= 1" }
    }

    internal val permits = Semaphore(maxConcurrentHandlers)

    private val inFlight = MutableStateFlow(0)

    /** Incoming requests of every hub currently being handled (or waiting for a handler permit). */
    val inFlightHandlers: StateFlow<Int> = inFlight.asStateFlow()

    internal fun entered() = inFlight.update { it + 1 }

    internal fun left() = inFlight.update { it - 1 }
}

/** Where [HubBus] sends requests: one [HubConnection], or [HubConnections] over every hub. */
interface HubTransport {
    /** See [HubConnection.send]. */
    suspend fun send(request: io.damoreira.kraken.hub.v1.Request): io.damoreira.kraken.hub.v1.ServerMessage

    /** See [HubConnection.hubTimeoutFor]. */
    fun hubTimeoutFor(type: String): java.time.Duration

    /** See [HubConnection.gather]. A transport without scatter-gather fails with `UnsupportedByHubException`. */
    suspend fun gather(
        request: io.damoreira.kraken.hub.v1.Request,
        onPart: suspend (io.damoreira.kraken.hub.v1.ReplyPart) -> Unit,
    ): io.damoreira.kraken.hub.v1.ServerMessage = throw io.damoreira.kraken.messaging.UnsupportedByHubException(
        HubProtocol.SCATTER_GATHER,
        "${this::class.simpleName} does not support ${HubProtocol.SCATTER_GATHER}",
    )

    /** See [HubConnection.stream]. A transport without streaming fails with `UnsupportedByHubException`. */
    suspend fun stream(
        request: io.damoreira.kraken.hub.v1.Request,
        onPart: suspend (io.damoreira.kraken.hub.v1.ReplyPart) -> Unit,
    ): io.damoreira.kraken.hub.v1.ServerMessage = throw io.damoreira.kraken.messaging.UnsupportedByHubException(
        HubProtocol.STREAMING,
        "${this::class.simpleName} does not support ${HubProtocol.STREAMING}",
    )
}
