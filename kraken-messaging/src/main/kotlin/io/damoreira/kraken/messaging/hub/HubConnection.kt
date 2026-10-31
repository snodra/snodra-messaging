package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Heartbeat
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.RegisterAck
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.ServerMessage
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.MessagingException
import io.grpc.ManagedChannel
import io.grpc.Status
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.ClosedSendChannelException
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.combine
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.receiveAsFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import kotlin.random.Random

data class HubSettings(
    /**
     * Sent as `timeout_ms` when set (the caller chose a timeout). Null lets a v2 hub apply its default or per-type
     * override (`timeout_ms = 0`); against a legacy hub [HubProtocol.DEFAULT_REQUEST_TIMEOUT] is sent instead. Also
     * bounds how long [HubConnection.send] waits for a stream (default 5 s).
     */
    val requestTimeout: Duration?,
    /** Heartbeat period until, or unless, the hub announces one in its `RegisterAck` (legacy hubs never do). */
    val heartbeatInterval: Duration,
    val backoffMin: Duration,
    val backoffMax: Duration,
) {
    /** [requestTimeout] when the caller chose one; zero (or negative) counts as unset, never as an instant deadline. */
    val chosenRequestTimeout: Duration? get() = requestTimeout?.takeIf { !it.isZero && !it.isNegative }
}

/** What the hub answered on the current stream: a v2 `RegisterAck`, or only the heartbeat echo of a legacy (v1) hub. */
sealed interface HubHandshake {
    /** A protocol v2 hub; [ack] is the latest `RegisterAck` of the stream. */
    data class V2(val ack: RegisterAck) : HubHandshake

    /** A hub that echoed the first heartbeat without a `RegisterAck`: protocol v1, today's behaviour applies. */
    data object Legacy : HubHandshake
}

/**
 * Reconnect delays: exponential from `backoffMin` to `backoffMax`, each wait jittered by ±20 % and capped at the max.
 * At the max the wait is jittered downward only (80-100 % of it): clamping a ±20 % wait would put about half of a
 * reconnecting fleet on exactly the maximum.
 */
internal object ReconnectBackoff {
    fun next(current: Duration, max: Duration): Duration = minOf(current.multipliedBy(2), max)

    fun jittered(base: Duration, max: Duration, random: Random = Random.Default): Duration {
        val ms = if (base >= max) (max.toMillis() * random.nextDouble(0.8, 1.0)).toLong()
        else (base.toMillis() * random.nextDouble(0.8, 1.2)).toLong()
        return Duration.ofMillis(ms.coerceIn(0, max.toMillis()))
    }
}

/**
 * The single bidirectional `Hub.Connect` stream of this service instance.
 *
 * The connect loop opens a stream, writes the current `Register` first (protocol version 2, see [HubProtocol]) and
 * keeps the stream alive with heartbeats. Every `Register` is followed by a heartbeat; since the hub handles a stream's
 * messages in order and fails the stream on a rejected registration, the echo of that heartbeat confirms the
 * registration ([awaitRegistered]).
 *
 * Handshake: a v2 hub answers the first `Register` with a `RegisterAck` before that echo; a legacy hub sends only the
 * echo. Whichever of the two arrives first decides the stream's [handshake] ([HubHandshake.V2] or
 * [HubHandshake.Legacy]) and marks the stream [connected]. The ack's heartbeat interval replaces
 * `heartbeatInterval` for that stream.
 *
 * When the stream fails, every request written to it fails with [HubConnectionLostException] (outcome unknown) and the
 * loop reconnects with jittered exponential backoff ([ReconnectBackoff]); a stream that reached the handshake resets
 * the backoff to `backoffMin`. An `ALREADY_EXISTS` or `INVALID_ARGUMENT` rejection is exposed in [lastRejection] and
 * retried at `backoffMax`. A request that could not be written to a live stream fails with [HubUnavailableException].
 *
 * Each stream gets its own outbound channel, so nothing queued for a broken stream (stale replies, requests whose
 * senders were already failed) leaks into the next one ahead of its `Register`.
 *
 * Liveness: every heartbeat expects an echo. When the oldest unanswered heartbeat is older than three heartbeat
 * intervals the stream is treated as dead (a half-open TCP connection never fails on its own) and the loop reconnects.
 *
 * Incoming requests run on [handlerDispatcher], never on [scope]'s dispatcher, so a handler that blocks (for example
 * on a nested `sendBlocking`) cannot starve the stream collector that would deliver its own reply. At most
 * [maxConcurrentHandlers] handlers run at once; further requests wait for a permit.
 */
class HubConnection(
    private val channelFactory: () -> ManagedChannel,
    private val serviceName: String,
    val instanceId: String,
    private val settings: HubSettings,
    private val inbound: suspend (Request) -> Reply,
    private val scope: CoroutineScope,
    private val handlerDispatcher: CoroutineDispatcher = Dispatchers.IO,
    maxConcurrentHandlers: Int = 256,
) {
    private val log = LoggerFactory.getLogger(HubConnection::class.java)

    private val connectedState = MutableStateFlow(false)
    /** True while the current stream has completed its handshake (ack or first echo). */
    val connected: StateFlow<Boolean> = connectedState.asStateFlow()

    private val handshakeState = MutableStateFlow<HubHandshake?>(null)
    /** The current stream's handshake; null while no stream has completed one. */
    val handshake: StateFlow<HubHandshake?> = handshakeState.asStateFlow()

    private val rejection = MutableStateFlow<Status?>(null)
    val lastRejection: StateFlow<Status?> = rejection.asStateFlow()

    /** A request in flight, with the stream it was written to (only that stream's failure fails it). */
    private class Pending(val stream: Stream, val answer: CompletableDeferred<ServerMessage>)
    private val pending = ConcurrentHashMap<String, Pending>()

    private val handlerPermits = Semaphore(maxConcurrentHandlers.also { require(it >= 1) { "maxConcurrentHandlers must be >= 1" } })
    private val inFlight = MutableStateFlow(0) // incoming requests dispatched and not yet answered

    /** Incoming requests currently being handled (or waiting for a handler permit). */
    val inFlightHandlers: StateFlow<Int> = inFlight.asStateFlow()

    /** One open stream: its outbound queue, handshake and the heartbeats awaiting an echo (`sentAtMs` -> generation). */
    private class Stream(val out: Channel<ClientMessage>, fallbackHeartbeatMs: Long) {
        val probes = ConcurrentSkipListMap<Long, Long>()
        @Volatile var registeredGeneration = -1L
        @Volatile var handshake: HubHandshake? = null
        @Volatile var heartbeatIntervalMs: Long = fallbackHeartbeatMs
        val handshakeReached = CompletableDeferred<Unit>()
    }

    private val lock = Any() // guards registration, generation and current
    private var registration = buildRegister(emptySet(), emptySet())
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

    /**
     * True once the hub confirmed the most recent [register] call on a live stream, false if [timeout] passes first.
     *
     * @throws HubRegistrationRejectedException if the hub rejects the registration (see [lastRejection]) while waiting
     */
    suspend fun awaitRegistered(timeout: Duration): Boolean {
        val outcome = withTimeoutOrNull(timeout.toMillis()) {
            combine(connectedState, acknowledged, generation, rejection) { up, acked, wanted, rejected ->
                when {
                    up && acked >= wanted -> true
                    rejected != null -> false
                    else -> null
                }
            }.first { it != null }
        }
        if (outcome == false) {
            val status = rejection.value
            throw HubRegistrationRejectedException(
                "hub rejected the registration of $serviceName ($instanceId): " + (status?.let(::describe) ?: "unknown"),
            )
        }
        return outcome == true
    }

    /** True once no incoming request is being handled, false if [timeout] passes first. */
    suspend fun awaitHandlersIdle(timeout: Duration): Boolean =
        withTimeoutOrNull(timeout.toMillis()) { inFlight.first { it == 0 } } != null

    /** Replaces the announced handler types; sent now if a stream is open and re-sent on every reconnect. */
    fun register(commandTypes: Set<String>, queryTypes: Set<String>) {
        synchronized(lock) {
            registration = buildRegister(commandTypes, queryTypes)
            generation.value += 1
            current?.let(::announce)
        }
    }

    /**
     * Sends [request] and returns the hub's `Reply` or `Error` for its id. [Request.getId] must be fresh for every call.
     *
     * `timeout_ms` is the caller's choice: a positive [Request.getTimeoutMs] is sent as is. Zero means "unset": a v2 hub
     * gets 0 (its default or per-type override applies), a legacy hub gets `requestTimeout` (default 5 s) as before.
     *
     * @throws HubUnavailableException if no stream completed its handshake within `requestTimeout` (default 5 s), or the
     *   stream closed before the request was written; the request never reached the hub
     * @throws HubConnectionLostException if the stream broke after the request was written; the outcome is unknown
     * @throws MessageTimeoutException if nothing arrives within the local guard: the hub's deadline bound plus
     *   [HubProtocol.LOCAL_GUARD_MARGIN] (v2), or `max(timeoutMs, requestTimeout)` (legacy)
     */
    suspend fun send(request: Request): ServerMessage {
        val wait = settings.chosenRequestTimeout ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT
        if (!connectedState.value) {
            withTimeoutOrNull(wait.toMillis()) { connectedState.first { it } }
                ?: throw HubUnavailableException("hub not connected within $wait")
        }
        val stream = current ?: throw HubUnavailableException("hub not connected")
        val handshake = stream.handshake ?: throw HubUnavailableException("hub not connected")
        val (outgoing, guardMs) = prepare(request, handshake)
        val entry = Pending(stream, CompletableDeferred())
        require(pending.putIfAbsent(request.id, entry) == null) { "request id ${request.id} is already in flight" }
        try {
            try {
                stream.out.send(ClientMessage.newBuilder().setRequest(outgoing).build())
            } catch (_: ClosedSendChannelException) {
                throw HubUnavailableException("hub stream closed before the request was written")
            }
            return withTimeoutOrNull(guardMs) { entry.answer.await() } ?: throw MessageTimeoutException(request.type)
        } finally {
            pending.remove(request.id, entry)
        }
    }

    /** The request as written to [handshake]'s stream, and the local guard in milliseconds. */
    private fun prepare(request: Request, handshake: HubHandshake): Pair<Request, Long> = when (handshake) {
        is HubHandshake.V2 -> request to HubProtocol.localGuardMs(request.timeoutMs, request.type, handshake.ack)
        HubHandshake.Legacy -> {
            val fallback = (settings.chosenRequestTimeout ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT).toMillis()
            val outgoing = if (request.timeoutMs > 0) request
            else request.toBuilder().setTimeoutMs(fallback.coerceIn(0, Int.MAX_VALUE.toLong()).toInt()).build()
            outgoing to outgoing.timeoutMs.toLong().coerceAtLeast(fallback)
        }
    }

    /** Closes the stream, stops reconnecting and fails every request in flight. */
    suspend fun stop() {
        val job = synchronized(lock) {
            current?.out?.close()
            loop
        }
        failPending(null, HubConnectionLostException("hub connection stopped"))
        job?.cancelAndJoin()
        failPending(null, HubConnectionLostException("hub connection stopped"))
    }

    private suspend fun connectLoop() {
        var backoff = settings.backoffMin
        var failures = 0
        while (currentCoroutineContext().isActive) {
            var channel: ManagedChannel? = null
            val stream = Stream(Channel(Channel.UNLIMITED), settings.heartbeatInterval.toMillis())
            var status: Status = Status.CANCELLED
            var reachedHandshake = false
            try {
                // Inside the try: a factory that throws (e.g. an unparsable address) is a failed attempt, not the end of the loop.
                channel = channelFactory()
                openAndCollect(channel, stream)
                status = Status.UNAVAILABLE.withDescription("hub closed the stream")
            } catch (e: CancellationException) {
                throw e
            } catch (e: HeartbeatTimeoutException) {
                status = Status.UNAVAILABLE.withDescription(e.message)
            } catch (e: Throwable) {
                status = Status.fromThrowable(e)
            } finally {
                synchronized(lock) {
                    if (current === stream) current = null
                    stream.out.close()
                }
                reachedHandshake = stream.handshake != null
                connectedState.value = false
                handshakeState.value = null
                acknowledged.value = -1
                channel?.shutdownNow()
                failPending(stream, HubConnectionLostException("hub stream closed: ${describe(status)}"))
            }
            // A stream the hub answered (ack or echo) proves the hub is back: start again from the floor.
            if (reachedHandshake) {
                backoff = settings.backoffMin
                failures = 0
            }
            failures++
            val wait = if (status.code == Status.Code.ALREADY_EXISTS || status.code == Status.Code.INVALID_ARGUMENT) {
                rejection.value = status
                val retry = ReconnectBackoff.jittered(settings.backoffMax, settings.backoffMax)
                log.error("Hub rejected the registration of {} ({}): {}; retrying in {}", serviceName, instanceId, status.description, retry)
                retry
            } else {
                val retry = ReconnectBackoff.jittered(backoff, settings.backoffMax)
                val text = "Hub stream of {} ({}) closed: {}; reconnecting in {}"
                if (failures == 1) log.warn(text, serviceName, instanceId, describe(status), retry)
                else log.debug(text, serviceName, instanceId, describe(status), retry)
                backoff = ReconnectBackoff.next(backoff, settings.backoffMax)
                retry
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
            // The ack may shorten the interval: wait for the handshake (at most one fallback interval) before the first tick.
            withTimeoutOrNull(settings.heartbeatInterval.toMillis()) { stream.handshakeReached.await() }
            while (true) {
                val interval = stream.heartbeatIntervalMs
                delay(interval)
                // Throwing fails this coroutineScope, which cancels the collector and ends the stream like any failure.
                val oldest = stream.probes.firstEntry()?.key
                if (oldest != null && System.currentTimeMillis() - oldest > 3 * interval) {
                    stream.probes.clear()
                    throw HeartbeatTimeoutException("no heartbeat echo from the hub for ${System.currentTimeMillis() - oldest} ms")
                }
                heartbeat(stream)
            }
        }
        HubGrpcKt.HubCoroutineStub(channel).connect(stream.out.receiveAsFlow()).collect { message -> handle(stream, message) }
        heartbeats.cancel()
    }

    private fun buildRegister(commandTypes: Set<String>, queryTypes: Set<String>): Register = Register.newBuilder()
        .setServiceName(serviceName)
        .setInstanceId(instanceId)
        .addAllCommandTypes(commandTypes.sorted())
        .addAllQueryTypes(queryTypes.sorted())
        .setProtocolVersion(HubProtocol.PROTOCOL_VERSION)
        .addAllCapabilities(HubProtocol.CAPABILITIES)
        .setClientVersion(HubProtocol.CLIENT_VERSION)
        .build()

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
            message.hasReply() -> pending.remove(message.reply.requestId)?.answer?.complete(message)
            message.hasError() -> pending.remove(message.error.requestId)?.answer?.complete(message)
            message.hasRegisterAck() -> onAck(stream, message.registerAck)
            message.hasHeartbeat() -> {
                if (stream.handshake == null) completeHandshake(stream, HubHandshake.Legacy) // echo before any ack: v1 hub
                val sentAt = message.heartbeat.sentAtMs
                val generation = stream.probes[sentAt] ?: return // an unsolicited keep-alive
                stream.probes.headMap(sentAt, true).clear()
                if (current === stream) acknowledged.value = maxOf(acknowledged.value, generation)
            }
        }
    }

    private fun onAck(stream: Stream, ack: RegisterAck) {
        when (stream.handshake) {
            null -> completeHandshake(stream, HubHandshake.V2(ack))
            is HubHandshake.V2 -> { // the answer to a re-registration: keep the latest values
                applyAck(stream, ack)
                stream.handshake = HubHandshake.V2(ack)
                if (current === stream) handshakeState.value = stream.handshake
            }
            HubHandshake.Legacy -> log.debug("RegisterAck after the legacy handshake on the stream of {} ignored", instanceId)
        }
    }

    private fun applyAck(stream: Stream, ack: RegisterAck) {
        if (ack.heartbeatIntervalMs > 0) stream.heartbeatIntervalMs = ack.heartbeatIntervalMs.toLong()
    }

    private fun completeHandshake(stream: Stream, handshake: HubHandshake) {
        if (handshake is HubHandshake.V2) applyAck(stream, handshake.ack)
        stream.handshake = handshake
        if (current === stream) {
            handshakeState.value = handshake
            connectedState.value = true
            rejection.value = null
            when (handshake) {
                is HubHandshake.V2 -> log.info(
                    "Connected to hub {} as {} ({}): protocol v{}, heartbeat every {} ms",
                    handshake.ack.hubId, serviceName, instanceId, handshake.ack.protocolVersion, stream.heartbeatIntervalMs,
                )
                HubHandshake.Legacy -> log.info(
                    "Connected to a legacy (protocol v1) hub as {} ({}): heartbeat every {} ms", serviceName, instanceId, stream.heartbeatIntervalMs,
                )
            }
        }
        stream.handshakeReached.complete(Unit)
    }

    /**
     * Runs the inbound handler on [handlerDispatcher] (bounded by [handlerPermits]) so neither a slow nor a blocking
     * handler holds up the stream; always answers the request, on the stream it arrived on.
     */
    private fun dispatch(stream: Stream, request: Request) {
        inFlight.update { it + 1 }
        val job = scope.launch(handlerDispatcher) {
            val reply = try {
                handlerPermits.withPermit { inbound(request) }
            } catch (e: Throwable) {
                // Only this coroutine's own cancellation propagates; a CancellationException leaking out of the handler
                // (e.g. a TimeoutCancellationException from its own withTimeout) is a handler failure like any other.
                if (e is CancellationException && !currentCoroutineContext().isActive) throw e
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
        job.invokeOnCompletion { inFlight.update { it - 1 } }
    }

    /** Fails the requests written to [stream] (every request when null) with [cause]. */
    private fun failPending(stream: Stream?, cause: MessagingException) {
        for ((id, entry) in pending.entries.toList()) {
            if (stream == null || entry.stream === stream) {
                if (pending.remove(id, entry)) entry.answer.completeExceptionally(cause)
            }
        }
    }

    private fun describe(status: Status) = status.code.name + (status.description?.let { " ($it)" } ?: "")

    private class HeartbeatTimeoutException(message: String) : RuntimeException(message)
}
