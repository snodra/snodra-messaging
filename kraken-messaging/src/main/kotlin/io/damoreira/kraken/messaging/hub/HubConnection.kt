package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Heartbeat
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.ServerMessage
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.grpc.ManagedChannel
import io.grpc.Status
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap

data class HubSettings(
    val requestTimeout: Duration,
    val heartbeatInterval: Duration,
    val backoffMin: Duration,
    val backoffMax: Duration,
)

/**
 * The single bidirectional `Hub.Connect` stream of this service instance.
 *
 * The connect loop opens a stream, writes the current `Register` first and keeps the stream alive with heartbeats.
 * Every `Register` is followed by a heartbeat; since the hub handles a stream's messages in order and fails the stream
 * on a rejected registration, the echo of that heartbeat confirms the registration ([awaitRegistered]) and the first
 * message received marks the stream [connected]. When the stream fails, every request in flight fails with
 * [HubUnavailableException] and the loop reconnects with exponential backoff (`backoffMin` doubling up to
 * `backoffMax`); an `ALREADY_EXISTS` rejection is exposed in [lastRejection] and retried at `backoffMax`.
 *
 * Each stream gets its own outbound channel, so nothing queued for a broken stream (stale replies, requests whose
 * senders were already failed) leaks into the next one ahead of its `Register`.
 */
class HubConnection(
    private val channelFactory: () -> ManagedChannel,
    private val serviceName: String,
    val instanceId: String,
    private val settings: HubSettings,
    private val inbound: suspend (Request) -> Reply,
    private val scope: CoroutineScope,
) {
    private val log = LoggerFactory.getLogger(HubConnection::class.java)

    private val connectedState = MutableStateFlow(false)
    val connected: StateFlow<Boolean> = connectedState.asStateFlow()

    private val rejection = MutableStateFlow<Status?>(null)
    val lastRejection: StateFlow<Status?> = rejection.asStateFlow()

    private val pending = ConcurrentHashMap<String, CompletableDeferred<ServerMessage>>()

    /** One open stream: its outbound queue and the heartbeats awaiting an echo (`sentAtMs` -> registration generation). */
    private class Stream(val out: Channel<ClientMessage>) {
        val probes = ConcurrentSkipListMap<Long, Long>()
        @Volatile var registeredGeneration = -1L
    }

    private val lock = Any() // guards registration, generation and current
    private var registration = Register.newBuilder().setServiceName(serviceName).setInstanceId(instanceId).build()
    private val generation = MutableStateFlow(0L) // bumped by every register(...)
    private val acknowledged = MutableStateFlow(-1L) // generation confirmed by the hub on the current stream
    @Volatile private var current: Stream? = null
    @Volatile private var loop: Job? = null
    private var lastHeartbeatAt = 0L

    /** Starts the connect loop in the connection's scope. */
    fun start() {
        synchronized(lock) {
            check(loop == null) { "HubConnection already started" }
            loop = scope.launch { connectLoop() }
        }
    }

    /** True once the hub confirmed the most recent [register] call on a live stream, false if [timeout] passes first. */
    suspend fun awaitRegistered(timeout: Duration): Boolean = withTimeoutOrNull(timeout.toMillis()) {
        combine(connectedState, acknowledged, generation) { up, acked, wanted -> up && acked >= wanted }.first { it }
    } != null

    /** Replaces the announced handler types; sent now if a stream is open and re-sent on every reconnect. */
    fun register(commandTypes: Set<String>, queryTypes: Set<String>) {
        synchronized(lock) {
            registration = Register.newBuilder()
                .setServiceName(serviceName)
                .setInstanceId(instanceId)
                .addAllCommandTypes(commandTypes.sorted())
                .addAllQueryTypes(queryTypes.sorted())
                .build()
            generation.value += 1
            current?.let(::announce)
        }
    }

    /**
     * Sends [request] and returns the hub's `Reply` or `Error` for its id. [Request.getId] must be fresh for every call.
     *
     * @throws HubUnavailableException if no stream is up within `requestTimeout` or the stream breaks before the answer
     * @throws MessageTimeoutException if nothing arrives within `max(timeoutMs, requestTimeout)`
     */
    suspend fun send(request: Request): ServerMessage {
        if (!connectedState.value) {
            withTimeoutOrNull(settings.requestTimeout.toMillis()) { connectedState.first { it } }
                ?: throw HubUnavailableException("hub not connected within ${settings.requestTimeout}")
        }
        val stream = current ?: throw HubUnavailableException("hub not connected")
        val answer = CompletableDeferred<ServerMessage>()
        require(pending.putIfAbsent(request.id, answer) == null) { "request id ${request.id} is already in flight" }
        try {
            try {
                stream.out.send(ClientMessage.newBuilder().setRequest(request).build())
            } catch (_: ClosedSendChannelException) {
                throw HubUnavailableException("hub stream closed")
            }
            val timeoutMs = request.timeoutMs.toLong().coerceAtLeast(settings.requestTimeout.toMillis())
            return withTimeoutOrNull(timeoutMs) { answer.await() } ?: throw MessageTimeoutException(request.type)
        } finally {
            pending.remove(request.id, answer)
        }
    }

    /** Closes the stream, stops reconnecting and fails every request in flight. */
    suspend fun stop() {
        val job = synchronized(lock) {
            current?.out?.close()
            loop
        }
        failPending(HubUnavailableException("stopped"))
        job?.cancelAndJoin()
        failPending(HubUnavailableException("stopped"))
    }

    private suspend fun connectLoop() {
        var backoff = settings.backoffMin
        var failures = 0
        while (currentCoroutineContext().isActive) {
            val channel = channelFactory()
            val stream = Stream(Channel(Channel.UNLIMITED))
            var status: Status = Status.CANCELLED
            var wasConnected = false
            try {
                openAndCollect(channel, stream)
                status = Status.UNAVAILABLE.withDescription("hub closed the stream")
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                status = Status.fromThrowable(e)
            } finally {
                synchronized(lock) {
                    if (current === stream) current = null
                    stream.out.close()
                }
                wasConnected = connectedState.value
                connectedState.value = false
                acknowledged.value = -1
                channel.shutdownNow()
                failPending(HubUnavailableException("hub stream closed: ${describe(status)}"))
            }
            if (wasConnected) {
                backoff = settings.backoffMin
                failures = 0
            }
            failures++
            val wait = if (status.code == Status.Code.ALREADY_EXISTS) {
                rejection.value = status
                log.error("Hub rejected the registration of {} ({}): {}; retrying in {}", serviceName, instanceId, status.description, settings.backoffMax)
                settings.backoffMax
            } else {
                val text = "Hub stream of {} ({}) closed: {}; reconnecting in {}"
                if (failures == 1) log.warn(text, serviceName, instanceId, describe(status), backoff)
                else log.debug(text, serviceName, instanceId, describe(status), backoff)
                backoff.also { backoff = minOf(backoff.multipliedBy(2), settings.backoffMax) }
            }
            delay(wait.toMillis())
        }
    }

    private suspend fun openAndCollect(channel: ManagedChannel, stream: Stream) = coroutineScope {
        synchronized(lock) {
            current = stream
            announce(stream) // the Register is the first message of every stream
        }
        val heartbeats = launch {
            while (true) {
                delay(settings.heartbeatInterval.toMillis())
                heartbeat(stream)
            }
        }
        HubGrpcKt.HubCoroutineStub(channel).connect(stream.out.receiveAsFlow()).collect { message ->
            if (!connectedState.value) {
                connectedState.value = true
                rejection.value = null
                log.info("Connected to the hub as {} ({})", serviceName, instanceId)
            }
            handle(stream, message)
        }
        heartbeats.cancel()
    }

    /** Writes the current registration followed by a heartbeat whose echo confirms it. Caller holds [lock]. */
    private fun announce(stream: Stream) {
        stream.out.trySend(ClientMessage.newBuilder().setRegister(registration).build())
        stream.registeredGeneration = generation.value
        heartbeat(stream)
    }

    private fun heartbeat(stream: Stream) {
        val sentAt = synchronized(lock) {
            // Strictly increasing so each echo identifies its heartbeat; stays within a few ms of the wall clock.
            maxOf(System.currentTimeMillis(), lastHeartbeatAt + 1).also { lastHeartbeatAt = it }
        }
        stream.probes[sentAt] = stream.registeredGeneration
        stream.out.trySend(ClientMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtMs(sentAt)).build())
    }

    private fun handle(stream: Stream, message: ServerMessage) {
        when {
            message.hasRequest() -> dispatch(stream, message.request)
            message.hasReply() -> pending.remove(message.reply.requestId)?.complete(message)
            message.hasError() -> pending.remove(message.error.requestId)?.complete(message)
            message.hasHeartbeat() -> {
                val sentAt = message.heartbeat.sentAtMs
                val generation = stream.probes[sentAt] ?: return // an unsolicited keep-alive
                stream.probes.headMap(sentAt, true).clear()
                if (current === stream) acknowledged.value = maxOf(acknowledged.value, generation)
            }
        }
    }

    /** Runs the inbound handler off the stream so a slow handler does not block it; always answers the request. */
    private fun dispatch(stream: Stream, request: Request) {
        scope.launch {
            val reply = try {
                inbound(request)
            } catch (e: CancellationException) {
                throw e
            } catch (e: Throwable) {
                log.warn("Dispatch of {} request {} failed", request.type, request.id, e)
                Reply.newBuilder()
                    .setRequestId(request.id)
                    .setError(
                        Error.newBuilder().setRequestId(request.id).setCode(ErrorCode.HANDLER_ERROR)
                            .setMessage("${e::class.simpleName}: ${e.message}"),
                    )
                    .build()
            }
            try {
                stream.out.send(ClientMessage.newBuilder().setReply(reply).build())
            } catch (_: ClosedSendChannelException) {
                log.debug("Reply to {} request {} dropped: the stream it arrived on is closed", request.type, request.id)
            }
        }
    }

    private fun failPending(cause: HubUnavailableException) {
        for (id in pending.keys.toList()) pending.remove(id)?.completeExceptionally(cause)
    }

    private fun describe(status: Status) = status.code.name + (status.description?.let { " ($it)" } ?: "")
}
