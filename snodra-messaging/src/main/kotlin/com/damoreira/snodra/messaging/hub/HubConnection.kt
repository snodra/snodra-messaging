package com.damoreira.snodra.messaging.hub

import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.Cancel
import com.damoreira.snodra.server.v1.Credentials
import com.damoreira.snodra.server.v1.ClientMessage
import com.damoreira.snodra.server.v1.ControlRefusal
import com.damoreira.snodra.server.v1.ControlRefused
import com.damoreira.snodra.server.v1.ControlReply
import com.damoreira.snodra.server.v1.ControlRequest
import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.Flow
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.Heartbeat
import com.damoreira.snodra.server.v1.HubGrpcKt
import com.damoreira.snodra.server.v1.InstanceStatus
import com.damoreira.snodra.server.v1.ProcessorStatus
import com.damoreira.snodra.server.v1.Permits
import com.damoreira.snodra.server.v1.Register
import com.damoreira.snodra.server.v1.RegisterAck
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.ReplyEnd
import com.damoreira.snodra.server.v1.ReplyPart
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestMode
import com.damoreira.snodra.server.v1.ServerMessage
import com.damoreira.snodra.messaging.HubConnectionLostException
import com.damoreira.snodra.messaging.HubRegistrationRejectedException
import com.damoreira.snodra.messaging.HubUnavailableException
import com.damoreira.snodra.messaging.InvalidMessageException
import com.damoreira.snodra.messaging.MessageTimeoutException
import com.damoreira.snodra.messaging.MessagingException
import com.damoreira.snodra.messaging.events.EventConsumer
import io.grpc.ManagedChannel
import io.grpc.Status
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
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
import kotlinx.coroutines.sync.withPermit
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.random.Random

data class HubSettings(
    /**
     * Sent as `timeout_ms` when set (the caller chose a timeout). Null lets the hub apply its default or per-type
     * override (`timeout_ms = 0`). Also bounds how long [HubConnection.send] waits for a stream (default 5 s).
     */
    val requestTimeout: Duration?,
    val backoffMin: Duration,
    val backoffMax: Duration,
    /**
     * The longest a streaming handler waits for credit before it ends its stream with `TIMEOUT` (`snodra.messaging.hub
     * .stream-credit-timeout`, default 10 min = the hub's default maximum stream duration): a hub that forgot the
     * stream (a race, a lost `Flow` or `Cancel`) would otherwise leave the handler's coroutine and permit held forever.
     */
    val streamCreditTimeout: Duration = HubProtocol.DEFAULT_STREAM_CREDIT_TIMEOUT,
    /**
     * The static token sent in `Register.credentials.token`, asked for once per stream. Its `toString`
     * is redacted, so printing the settings never prints the token.
     */
    val token: HubToken = HubToken.NONE,
    /**
     * The OAuth2 access token of this service, shared by every hub connection: when set, each stream registers
     * with `Register.credentials.jwt` instead of [token] and gets every renewal as `ClientMessage.credentials`. Its `toString`
     * is redacted too.
     */
    val tokenProvider: HubTokenProvider? = null,
    /**
     * The client certificate this service presents in the TLS handshake: when set, no credentials are ever sent (the hub reads the
     * identity from the certificate, ignores a `ClientMessage.credentials` on such a session and refuses a stale token next to it).
     */
    val clientCertificate: HubClientCertificate? = null,
    /**
     * How long a new stream waits for the hub's `RegisterAck` ([HubProtocol.HANDSHAKE_TIMEOUT]): without one in time the
     * stream fails ("registration not confirmed in time") and the connection reconnects with backoff.
     */
    val handshakeTimeout: Duration = HubProtocol.HANDSHAKE_TIMEOUT,
) {
    /** [requestTimeout] when the caller chose one; zero (or negative) counts as unset, never as an instant deadline. */
    val chosenRequestTimeout: Duration? get() = requestTimeout?.takeIf { !it.isZero && !it.isNegative }
}

/**
 * True when the hub closed the stream because it refuses this registration: a service name or handled type another
 * service owns (`ALREADY_EXISTS`), an invalid registration (`INVALID_ARGUMENT`), no or an unknown token
 * (`UNAUTHENTICATED`), or a service name or type the token's ACL entry does not grant (`PERMISSION_DENIED`). Trying
 * again at once cannot succeed, so these are retried at the backoff cap and exposed in [HubConnection.lastRejection].
 */
internal fun isRegistrationRejection(status: Status): Boolean = when (status.code) {
    Status.Code.ALREADY_EXISTS, Status.Code.INVALID_ARGUMENT, Status.Code.UNAUTHENTICATED, Status.Code.PERMISSION_DENIED -> true
    else -> false
}

/** Heartbeat intervals without an echo, and heartbeats left unanswered, after which a silent hub is dropped. */
internal const val SILENT_HEARTBEATS = 3

/**
 * The silent-hub decision, taken on a heartbeat tick before that tick's heartbeat is written: more than [SILENT_HEARTBEATS]
 * heartbeat intervals ([intervalMs]) have passed since the last echo ([sinceEchoNanos]; the `RegisterAck` counts as the first
 * one) **and** the [unanswered] heartbeats written since that echo are at least [SILENT_HEARTBEATS]. The second condition keeps
 * a tick that fires a little late from dropping the stream exactly three intervals after the last echo: the drop always comes
 * on the tick after the third unanswered heartbeat, between three and four intervals after the last echo, never before three.
 */
internal fun hubSilent(sinceEchoNanos: Long, unanswered: Int, intervalMs: Long): Boolean =
    unanswered >= SILENT_HEARTBEATS && sinceEchoNanos > SILENT_HEARTBEATS * intervalMs * 1_000_000

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
 * The connect loop opens a stream, writes the current `Register` first (see [HubProtocol]) and keeps the stream alive
 * with heartbeats. Every `Register` is followed by a heartbeat; since the hub handles a stream's messages in order and
 * fails the stream on a rejected registration, the echo of that heartbeat confirms that registration ([awaitRegistered]).
 *
 * Handshake: the hub answers every accepted `Register` with a `RegisterAck`, written before the echo of the heartbeat
 * that follows it. The ack of the stream's first `Register` completes the handshake: it marks the stream [connected], and
 * its heartbeat interval is the stream's ([ack] keeps the latest). A stream without an ack within
 * [HubSettings.handshakeTimeout] fails ("registration not confirmed in time") and the loop reconnects.
 *
 * When the stream fails, every request written to it fails with [HubConnectionLostException] (outcome unknown) and the
 * loop reconnects with jittered exponential backoff ([ReconnectBackoff]); a stream that reached the handshake resets
 * the backoff to `backoffMin`. A stream the hub closed with `ALREADY_EXISTS`, `INVALID_ARGUMENT`, `UNAUTHENTICATED` or
 * `PERMISSION_DENIED` is a registration rejection ([isRegistrationRejection]): exposed in [lastRejection], retried at
 * `backoffMax` and logged at `error` once per distinct message (then at `debug`). The static token
 * ([HubSettings.token]) goes in `Register.credentials` of every stream, read when the stream opens (a token file is re-read
 * on every reconnect) and never logged. A request that could not be written to a live stream fails with [HubUnavailableException].
 *
 * Each stream gets its own outbound channel, so nothing queued for a broken stream (stale replies, requests whose
 * senders were already failed) leaks into the next one ahead of its `Register`.
 *
 * Liveness: every heartbeat expects an echo. From the `RegisterAck` on, each heartbeat tick checks, before writing its
 * heartbeat, whether more than three heartbeat intervals have passed since the last echo (the ack counts as the first) and
 * the three heartbeats written since that echo are all unanswered ([hubSilent]); then the stream is treated as dead (a
 * half-open TCP connection never fails on its own) and the loop reconnects. The drop comes between three and four intervals
 * after the last echo, never before three. Before the ack there is no such check: the handshake timeout covers that.
 *
 * Incoming requests run on [handlerDispatcher], never on [scope]'s dispatcher, so a handler that blocks (for example
 * on a nested `sendBlocking`) cannot starve the stream collector that would deliver its own reply. At most
 * [maxConcurrentHandlers] handlers run at once; further requests wait for a permit.
 *
 * Flow control: `Register` carries `initial_permits = maxConcurrentHandlers`, [loadFactor] and [tags]. The hub
 * forwards only against permits, and every completed inbound handler grants one back on the stream the request arrived
 * on (never on a newer stream, which starts again from its own initial permits). Grants go back at once while the hub
 * holds at most `min(PERMIT_BATCH, initial_permits / 2)` permits for the stream, otherwise in `Permits` messages of
 * [HubProtocol.PERMIT_BATCH] or after [HubProtocol.PERMIT_FLUSH_DELAY] ([PermitLedger] has the rule).
 *
 * Multi-hub: [HubConnections] runs one connection per hub [target], all sharing one [HandlerGate] (the local bound
 * on handlers over every hub) and announcing [initialPermits] (read when each stream opens) as `initial_permits`. A
 * request still queued for a stream when it breaks was never written: it fails with [HubUnavailableException], so it
 * can go to another hub. [reportStatus] writes an `InstanceStatus` to the hub.
 *
 * Scatter-gather: `Register` lists shared query types in `shared_query_types`. [gather] sends a
 * `SCATTER_GATHER` request; its pending entry collects the `ReplyPart`s until the `ReplyEnd` (or an `Error`), under one
 * local guard for the whole gather. Parts and ends for ids nobody waits for are ignored.
 *
 * Streaming: as a sender, [stream] sends a `STREAMING` request, hands each `ReplyPart` to the caller
 * (checking `seq` runs 1, 2, 3, ...
 * and, at the end, that `ReplyEnd.parts` is what arrived: a part the hub dropped fails the stream as lost), writes
 * `Flow{initial_credit / 2}` each time that many parts were consumed (the caller's callback returned) and `Cancel`
 * when the caller gives up before the end (cancellation, an exception, the local idle guard, a lost part). As a
 * handler, a `STREAMING` request runs [inboundStream] under the handler permit for the whole stream: each part waits
 * for credit ([StreamCredit]: the request's `initial_credit` plus every `Flow` for its id, both `uint32`) and goes out
 * with `seq` 1, 2, ..., then `ReplyEnd` (with the error the handler returned, or `HANDLER_ERROR` for an exception that
 * leaked). A `Cancel` from the hub cancels the
 * handler's coroutine, for a stream (no end follows) as for a unary or gather request (no reply follows); the stream
 * closing cancels every streaming handler it carried, which would otherwise wait for credit forever. A handler that
 * waits for credit longer than [HubSettings.streamCreditTimeout] (the hub forgot the stream: a race, a lost `Flow` or
 * `Cancel`) ends its stream with `ReplyEnd{TIMEOUT, "no credit within ..."}` (its flow sees a `CancellationException`,
 * as at a hub `Cancel`) and releases its handler slot and hub permit. The permit of every inbound request goes back
 * to the hub when its coroutine completes, whichever way.
 *
 * Subscriptions: every `Register` declares the query types served by a subscription handler (exclusive or shared) in
 * `subscription_query_types` ([register]); the hub serves those types in mode `SUBSCRIPTION` only, and that mode for those
 * types only (anything else is its `Error{INVALID_REQUEST}`). As a sender, [subscribe] sends a `SUBSCRIPTION` request, waits for the initial part and
 * returns the open
 * [HubSubscription]: its [HubSubscription.next] hands out the updates in order (checking `seq` runs 1, 2, 3, ... and the end's
 * `parts` count, refilling `Flow{initial_credit / 2}` per that many parts consumed, the initial part included) and
 * [HubSubscription.cancel] writes `Cancel` once. As a handler, a `SUBSCRIPTION` request is an update target the hub wrote
 * outside flow control: it takes no handler permit and no [gate] slot, and no permit is paid back for it ([PermitLedger]
 * never sees it). It is registered in [subscriptions] when the request is read (pending: the hub writes `<id>#n` before the
 * initial request `<id>#0` to the same instance, so an update may be emitted before the admission ends; such updates are held
 * in order) and then admitted by [inboundSubscription], which makes it live or refuses it; a hub `Cancel` removes it, the
 * stream closing removes every subscription the stream carried, and the update emitter writes its parts and its end on the
 * stream it arrived on. The initial result is an ordinary unary request (id `<id>#0`).
 *
 * Processor control: a `ControlRequest` goes to [control] in arrival order from one lane per stream, never
 * from the read loop (a control handler that blocks holds up only the control requests behind it, not the stream), and its
 * `ControlReply` is written on the stream it arrived on.
 */
class HubConnection(
    private val channelFactory: () -> ManagedChannel,
    private val serviceName: String,
    val instanceId: String,
    private val settings: HubSettings,
    private val inbound: suspend (Request) -> Reply,
    private val scope: CoroutineScope,
    private val handlerDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val maxConcurrentHandlers: Int = 256,
    private val loadFactor: Int = 100,
    private val tags: Map<String, String> = emptyMap(),
    /** The hub's gRPC target (`host:port`). */
    val target: String = "hub",
    private val gate: HandlerGate = HandlerGate(maxConcurrentHandlers),
    /** `Register.initial_permits` of a new stream (splits `maxConcurrentHandlers` over the hubs). */
    private val initialPermits: () -> Int = { maxConcurrentHandlers },
    /** Serves `mode = STREAMING` requests; without streaming handlers every stream ends with `INVALID_REQUEST`. */
    private val inboundStream: StreamingInbound = NO_STREAMING_HANDLERS,
    /** The subscriptions this instance serves, shared by every hub connection; the update emitter delivers to them. */
    private val subscriptions: SubscriptionRegistry = SubscriptionRegistry(),
    /** Admits `mode = SUBSCRIPTION` requests; without subscription handlers every one ends with `INVALID_REQUEST`. */
    private val inboundSubscription: SubscriptionInbound = NO_SUBSCRIPTION_HANDLERS,
    /** Called for every `ClientMessage.credentials` this connection writes ([HubConnections] keeps the process-wide, monotonic total). */
    private val onCredentialsRefreshSent: () -> Unit = {},
    /** Called for every refusal of this connection's registration ([HubConnections] keeps the newest of all its hubs). */
    private val onRejected: (seq: Long, rejection: HubRejection) -> Unit = { _, _ -> },
    /** Stamps [HubRejection.at]. */
    private val clock: Clock = Clock.systemUTC(),
    /** Serves the hub's `ControlRequest`s; without event processors every one is `UNKNOWN_PROCESSOR`. */
    private val control: ControlInbound = NO_PROCESSOR_CONTROL,
) : HubTransport {
    private val log = LoggerFactory.getLogger(HubConnection::class.java)

    private val connectedState = MutableStateFlow(false)
    /** True while the current stream has completed its handshake (the hub's `RegisterAck` arrived). */
    val connected: StateFlow<Boolean> = connectedState.asStateFlow()

    private val ackState = MutableStateFlow<RegisterAck?>(null)
    /** The latest `RegisterAck` of the current stream; null while no stream has completed its handshake. */
    val ack: StateFlow<RegisterAck?> = ackState.asStateFlow()

    private val rejection = MutableStateFlow<Status?>(null)
    /**
     * The refusal (or the failure to get a token) that keeps this connection from registering now; cleared once a stream completes
     * its handshake. The application reads [HubConnections.latestRejection], which a later acceptance never clears.
     */
    internal val lastRejection: StateFlow<Status?> = rejection.asStateFlow()

    /** The id of the hub that last acknowledged a stream of this connection, kept after that stream ended; null before the first. */
    @Volatile private var lastHubId: String? = null

    /** A request in flight, with the stream it was written to (only that stream's failure fails it). */
    private sealed class Pending(val stream: Stream) {
        /** Fails the waiting caller with [cause]. */
        abstract fun fail(cause: Throwable)

        /** Delivers the request's terminal message (`Reply`, `Error` or `ReplyEnd`). */
        abstract fun finish(message: ServerMessage)

        /** A unary request: one `Reply` or `Error`. */
        class Unary(stream: Stream) : Pending(stream) {
            val answer = CompletableDeferred<ServerMessage>()
            override fun fail(cause: Throwable) {
                answer.completeExceptionally(cause)
            }
            override fun finish(message: ServerMessage) {
                answer.complete(message)
            }
        }

        /**
         * A scatter-gather, a stream or a subscription: its `ReplyPart`s in arrival order, then its `ReplyEnd` (or an `Error`), then closed.
         * [boundedByDeadline] is false for a stream and a subscription this instance consumes: they can run for minutes or forever, so a
         * rollover does not wait for them ([HubConnection.rolloverIdle]); a gather ends within its deadline and is waited for.
         */
        class Multi(stream: Stream, val boundedByDeadline: Boolean = true) : Pending(stream) {
            val messages = Channel<ServerMessage>(Channel.UNLIMITED)
            override fun fail(cause: Throwable) {
                messages.close(cause)
            }
            override fun finish(message: ServerMessage) {
                messages.trySend(message)
                messages.close()
            }
        }
    }
    private val pending = ConcurrentHashMap<String, Pending>()

    /** An inbound request being handled: its coroutine (cancelled by a hub `Cancel`) and, for a stream, its credit. */
    private class Running(val stream: Stream, val credit: StreamCredit?) {
        lateinit var job: Job
    }
    private val running = ConcurrentHashMap<String, Running>()

    init {
        require(loadFactor >= 0) { "loadFactor must not be negative, was $loadFactor" } // uint32 on the wire, 0 reads as 100
    }
    private val inFlight = MutableStateFlow(0) // incoming requests dispatched and not yet answered

    /** Incoming requests of this hub currently being handled (or waiting for a handler permit). */
    val inFlightHandlers: StateFlow<Int> = inFlight.asStateFlow()

    /**
     * One open stream: its outbound queue, handshake, the heartbeats awaiting an echo (`sentAtMs` -> generation) and the
     * permit accounting against its `initial_permits`.
     */
    private class Stream(val out: Channel<ClientMessage>, val initialPermits: Int) {
        /** The credential the hub knows for this stream: the one `Register` carries, then the latest refresh written (guarded by the stream itself). */
        var credentials: Credentials? = null
        val probes = ConcurrentSkipListMap<Long, Long>()
        val permits = PermitLedger(initialPermits)
        /** `initial_permits` plus every top-up written on this stream (guarded by the stream itself). */
        var offered = initialPermits
        @Volatile var registeredGeneration = -1L
        /** The latest `RegisterAck` of the stream; null until the handshake. */
        @Volatile var ack: RegisterAck? = null
        @Volatile var heartbeatIntervalMs: Long = HubProtocol.DEFAULT_HEARTBEAT_INTERVAL.toMillis()
        val handshakeReached = CompletableDeferred<Unit>()
        /** Completed once the connect loop is done with this stream (the hub finished it, or it failed) and cleaned up. */
        val closed = CompletableDeferred<Unit>()
        /** A rollover told the hub this instance is stopping: every `Register` of the stream carries no types from now on. */
        @Volatile var draining = false
        /** The `ControlRequest`s read on this stream and not yet handed to the control, in arrival order. */
        val controls = Channel<ControlRequest>(Channel.UNLIMITED)

        // The silent-hub watch, armed by the ack (guarded by itself): the last sign of life (the ack, then each echo; System.nanoTime)
        // and the heartbeats written since it.
        private val silence = Any()
        private var watching = false
        private var lastEchoNanos = 0L
        private var unanswered = 0

        /** The handshake: the ack is the first sign of life the silence is measured from. */
        fun watchSilence() = synchronized(silence) {
            watching = true
            lastEchoNanos = System.nanoTime()
            unanswered = 0
        }

        /** An echo (any heartbeat the hub writes) proves the hub alive; before the ack it confirms nothing. */
        fun echoed() = synchronized(silence) {
            if (watching) {
                lastEchoNanos = System.nanoTime()
                unanswered = 0
            }
        }

        fun heartbeatWritten() = synchronized(silence) { if (watching) unanswered++ }

        /** The time since the last echo, in ms, when the hub is silent by [hubSilent] on this tick; null otherwise (and before the ack). */
        fun silentFor(intervalMs: Long): Long? = synchronized(silence) {
            val since = System.nanoTime() - lastEchoNanos
            if (watching && hubSilent(since, unanswered, intervalMs)) since / 1_000_000 else null
        }
    }

    private val lock = Any() // guards registration, generation and current
    private var registration = buildRegister(emptySet(), emptySet(), emptySet(), emptySet())
    private val generation = MutableStateFlow(0L) // bumped by every register(...)
    private val acknowledged = MutableStateFlow(-1L) // generation confirmed by the hub on the current stream
    @Volatile private var current: Stream? = null
    /** The channel of [current]'s attempt, for a rollover that has to cut a hub that does not end its side. */
    @Volatile private var liveChannel: ManagedChannel? = null
    /** The stream a rollover is replacing: its end is the start of the next attempt, with no back-off. */
    @Volatile private var rollingStream: Stream? = null
    /**
     * The generation of [HubClientCertificate] read just before the channel of the current attempt was built: a pair adopted after it is
     * not presented by this stream, so [HubConnections] rolls the stream over. -1 before the first attempt.
     */
    @Volatile internal var certificateGeneration = -1L
    /** The pair the current attempt read just before its channel was built (what the stream presents, up to a pair adopted in the meantime); null before the first attempt. */
    @Volatile internal var certificatePresented: ClientCertificate? = null
    /** [HubConnections] is rolling this connection over: it takes no new work from this instance's senders (see [HubConnections]). */
    @Volatile internal var rolling = false
    @Volatile private var loop: Job? = null
    @Volatile private var renewals: Job? = null
    /** [stop] was called: the connect loop ends with its current stream instead of opening another. */
    @Volatile private var stopping = false
    private var lastHeartbeatAt = 0L

    /** The hub's id: `RegisterAck.hub_id` of the current stream ([target] when it is empty), null before the handshake. */
    val hubId: String? get() = ackState.value?.hubId?.ifEmpty { target }

    /** True while the hub has confirmed the most recent [register] call on the live stream. */
    internal val registered: Boolean get() = connectedState.value && acknowledged.value >= generation.value

    private val firstAttemptEnded = MutableStateFlow(false)

    /** True until the first connection attempt has ended (the connect loop is still on its first stream). */
    internal val onFirstAttempt: Boolean get() = !firstAttemptEnded.value

    /** Becomes true when the first connection attempt ends ([onFirstAttempt] turns false). */
    internal val firstAttemptOver: StateFlow<Boolean> = firstAttemptEnded.asStateFlow()

    /** No request of this instance is waiting for this hub's answer and no request of this hub is being handled. */
    internal val idle: Boolean get() = pending.isEmpty() && inFlight.value == 0

    /**
     * What a rollover waits for: [idle], but the streams and subscriptions this instance itself consumes ([Pending.Multi] with
     * `boundedByDeadline` false) do not count: they never finish on their own and are cut by the roll (the consumer sees the loss of the stream).
     */
    private val rolloverIdle: Boolean get() = inFlight.value == 0 && pending.values.all { it is Pending.Multi && !it.boundedByDeadline }

    /** Starts the connect loop in the connection's scope. */
    fun start() {
        synchronized(lock) {
            check(loop == null) { "HubConnection already started" }
            loop = scope.launch { connectLoop() }
            settings.tokenProvider?.let { provider ->
                // Every renewal goes to the live stream (a stream that cannot take it, or none, is skipped: see refreshCredentials).
                renewals = scope.launch { provider.current.collect { current?.let(::refreshCredentials) } }
            }
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
                if (status?.cause is HubTokenException) "no access token for $serviceName ($instanceId): ${status.cause?.message}"
                else "hub rejected the registration of $serviceName ($instanceId): " + (status?.let(::describe) ?: "unknown"),
            )
        }
        return outcome == true
    }

    /**
     * Moves this hub's session onto the pair [HubSettings.clientCertificate] holds now, without failing a request that was
     * accepted: a TLS session cannot change its certificate, so the stream is replaced.
     *
     *  1. Tells the hub the instance is stopping: a `Register` with no types (the shutdown path's), so it routes new work to other instances or
     *     other hubs. What the connection registers on its next stream is untouched.
     *  2. Waits up to [drainTimeout] for the work in flight on the stream (requests this instance sent and has no answer for, requests it is handling).
     *     Streams and subscriptions this instance itself consumes are not waited for (they have no end of their own): the close cuts them,
     *     the consumer's `updates`/stream fail with [HubConnectionLostException] and it subscribes again (see [rolloverIdle]).
     *  3. Closes it gracefully, as [stop] does: the request side is half-closed after its last message and the hub's end awaited for
     *     [STOP_CLOSE_TIMEOUT] at most, then the channel is shut down.
     *  4. Reconnects at once (no back-off) with the pair the files hold, and waits up to [reconnectTimeout] for the new stream's registration.
     *
     * [onDrained] runs after step 2, before the close (the owner takes this hub out of routing there when it is the only one).
     *
     * Requests queued and not yet written when the stream closes fail with [HubUnavailableException] (never reached the hub: a caller may send
     * them again), the ones written fail as for any lost stream. Returns true when the connection has a stream registered again, or had none
     * to replace (it presents the current pair when it connects); false when [stop] came first or the new stream did not register in time.
     */
    internal suspend fun rollOver(drainTimeout: Duration, reconnectTimeout: Duration, onDrained: () -> Unit = {}): Boolean {
        val stream = synchronized(lock) { if (stopping) null else current } ?: return true
        if (stream.ack == null) return true // not registered yet: it is ahead of nothing, and its generation tells the next tick to roll it
        log.info("Rolling the hub stream of {} ({}) to {} over to the renewed client certificate", serviceName, instanceId, target)
        val probe = synchronized(lock) {
            stream.draining = true
            announce(stream)
        }
        val drained = withTimeoutOrNull(drainTimeout.toMillis()) {
            // The hub reads a stream in order: once it echoes the heartbeat that follows the empty Register, no request it forwarded before
            // reading that Register is still on its way, so what is in flight now is all there will be.
            while (stream.probes.containsKey(probe)) delay(10)
            while (!rolloverIdle) delay(20)
            true
        } == true
        if (!drained) {
            log.warn("{} request(s) still in flight on the hub stream to {} after {}; rolling it over anyway", pending.values.count { it !is Pending.Multi || it.boundedByDeadline } + inFlight.value, target, drainTimeout)
        }
        onDrained() // the work is done: from here on this stream takes nothing new
        synchronized(lock) {
            rollingStream = stream
            if (current === stream) stream.out.close() // half-close after the last queued message
        }
        if (withTimeoutOrNull(STOP_CLOSE_TIMEOUT.toMillis()) { stream.closed.await() } == null) {
            liveChannel?.shutdownNow() // the hub did not finish its side
            withTimeoutOrNull(STOP_CLOSE_TIMEOUT.toMillis()) { stream.closed.await() }
        }
        if (stopping) return false
        return try {
            awaitRegistered(reconnectTimeout)
        } catch (e: HubRegistrationRejectedException) {
            false
        }
    }

    /** True once no incoming request is being handled, false if [timeout] passes first. */
    suspend fun awaitHandlersIdle(timeout: Duration): Boolean =
        withTimeoutOrNull(timeout.toMillis()) { inFlight.first { it == 0 } } != null

    /**
     * Replaces the announced handler types ([sharedQueryTypes]: `@QueryHandler(shared = true)`, sent as
     * `shared_query_types`; [subscriptionQueryTypes]: the query types, of either list, served by a subscription handler, sent as
     * `subscription_query_types`); sent now if a stream is open and re-sent on every reconnect.
     */
    fun register(
        commandTypes: Set<String>,
        queryTypes: Set<String>,
        sharedQueryTypes: Set<String> = emptySet(),
        subscriptionQueryTypes: Set<String> = emptySet(),
    ) {
        synchronized(lock) {
            registration = buildRegister(commandTypes, queryTypes, sharedQueryTypes, subscriptionQueryTypes)
            generation.value += 1
            current?.let(::announce)
        }
    }

    /**
     * Sends [request] and returns the hub's `Reply` or `Error` for its id. [Request.getId] must be fresh for every call.
     *
     * `timeout_ms` is the caller's choice: a positive [Request.getTimeoutMs] is sent as is. Zero means "unset": the hub's
     * default or per-type override applies.
     *
     * @throws HubUnavailableException if no stream completed its handshake within `requestTimeout` (default 5 s), or the
     *   stream closed before the request was written; the request never reached the hub
     * @throws HubConnectionLostException if the stream broke after the request was written; the outcome is unknown
     * @throws MessageTimeoutException if nothing arrives within the local guard: the hub's deadline bound plus
     *   [HubProtocol.LOCAL_GUARD_MARGIN]
     */
    override suspend fun send(request: Request): ServerMessage = send(request, awaitStream = true)

    /**
     * [send]; with [awaitStream] false a connection without a live stream fails at once with [HubUnavailableException]
     * instead of waiting for one (the multi-hub client then tries another hub).
     */
    internal suspend fun send(request: Request, awaitStream: Boolean): ServerMessage {
        val stream = liveStream(awaitStream)
        val guardMs = HubProtocol.localGuardMs(request.timeoutMs, request.type, stream.ack!!)
        val entry = Pending.Unary(stream)
        require(pending.putIfAbsent(request.id, entry) == null) { "request id ${request.id} is already in flight" }
        try {
            write(stream, request)
            return withTimeoutOrNull(guardMs) { entry.answer.await() } ?: throw MessageTimeoutException(request.type)
        } finally {
            pending.remove(request.id, entry)
        }
    }

    /**
     * Sends [request] as a scatter-gather (`mode = SCATTER_GATHER` is set here), hands every `ReplyPart` to [onPart] in
     * arrival order and returns the terminal message: the `ReplyEnd`, or the hub's `Error` for the request itself (no
     * owner, a command, ...). [Request.getId] must be fresh for every call.
     *
     * @throws HubUnavailableException, HubConnectionLostException as [send]; parts already handed to [onPart] stay
     * @throws MessageTimeoutException if the end does not arrive within the local guard (the hub's deadline bound for the
     *   whole gather plus [HubProtocol.LOCAL_GUARD_MARGIN])
     */
    override suspend fun gather(request: Request, onPart: suspend (ReplyPart) -> Unit): ServerMessage =
        gather(request, awaitStream = true, onPart)

    /** [gather]; [awaitStream] as for [send]. */
    internal suspend fun gather(request: Request, awaitStream: Boolean, onPart: suspend (ReplyPart) -> Unit): ServerMessage {
        val stream = liveStream(awaitStream)
        val ack = stream.ack!!
        val outgoing = request.toBuilder().setMode(RequestMode.SCATTER_GATHER).build()
        val guardMs = HubProtocol.localGuardMs(outgoing.timeoutMs, outgoing.type, ack)
        val entry = Pending.Multi(stream)
        require(pending.putIfAbsent(request.id, entry) == null) { "request id ${request.id} is already in flight" }
        try {
            write(stream, outgoing)
            var terminal: ServerMessage? = null
            withTimeoutOrNull(guardMs) {
                for (message in entry.messages) {
                    if (message.hasPart()) {
                        onPart(message.part)
                    } else {
                        terminal = message
                        break
                    }
                }
            }
            return terminal ?: throw MessageTimeoutException(request.type)
        } finally {
            pending.remove(request.id, entry)
        }
    }

    /**
     * Sends [request] as a streaming query (`mode = STREAMING` is set here; `initial_credit` is the caller's window,
     * at least 1), hands every `ReplyPart` to [onPart] in order and returns the terminal message: the `ReplyEnd` (with
     * the handler's or the hub's error when the stream failed), or the hub's `Error` for the request itself (no owner,
     * a command, ...). [Request.getId] must be fresh for every call.
     *
     * Credits: after every `initial_credit / 2` (at least 1) parts [onPart] consumed, `Flow` grants that many more.
     * Giving up: when [onPart] throws or the caller is cancelled before the end, `Cancel{id, reason}` is written and
     * the exception rethrown; nothing of the stream follows. The local guard is per part: the hub's deadline bound for
     * `timeout_ms` (the idle timeout) plus [HubProtocol.LOCAL_GUARD_MARGIN], twice the bound for the first part (the
     * hub bounds a queue wait by the timeout, then starts the idle timeout over at the forward).
     *
     * @throws HubUnavailableException, HubConnectionLostException as [send]; parts already handed to [onPart] stay
     * @throws MessageTimeoutException if no part or end arrives within the local guard (a `Cancel` is written)
     * @throws InvalidMessageException `stream <type> lost N part(s)` when a part's `seq` jumps (the hub drops a part it
     *   could not deliver within 5 s; `Cancel` is written, the stream is still open) or the `ReplyEnd.parts` count
     *   exceeds the parts received (the stream has ended; nothing is written); also for a `seq` out of order
     * @throws StreamCollectorException wrapping what [onPart] threw (other than a `CancellationException`): whatever it
     *   was, also a `HubUnavailableException` of a nested send, it is the collector's failure, not this request's
     *   transport failing; `Cancel` is written
     */
    override suspend fun stream(request: Request, onPart: suspend (ReplyPart) -> Unit): ServerMessage =
        stream(request, awaitStream = true, onPart)

    /** [stream]; [awaitStream] as for [send]. */
    internal suspend fun stream(request: Request, awaitStream: Boolean, onPart: suspend (ReplyPart) -> Unit): ServerMessage {
        val stream = liveStream(awaitStream)
        val ack = stream.ack!!
        val window = request.initialCredit.takeIf { it > 0 } ?: HubProtocol.DEFAULT_INITIAL_CREDIT
        val outgoing = request.toBuilder().setMode(RequestMode.STREAMING).setInitialCredit(window).build()
        val bound = HubProtocol.hubDeadlineBoundMs(outgoing.timeoutMs, outgoing.type, ack)
        val margin = HubProtocol.LOCAL_GUARD_MARGIN.toMillis()
        val refill = maxOf(1, window / 2)
        val entry = Pending.Multi(stream, boundedByDeadline = false)
        require(pending.putIfAbsent(request.id, entry) == null) { "request id ${request.id} is already in flight" }
        var terminal: ServerMessage? = null
        var failure: Throwable? = null
        try {
            write(stream, outgoing)
            var guardMs = 2 * bound + margin
            var consumed = 0
            var received = 0L
            while (true) {
                // The guard covers the wait for the next message only, never the caller's handling of a part.
                val message = withTimeoutOrNull(guardMs) { entry.messages.receive() } ?: throw MessageTimeoutException(request.type)
                if (!message.hasPart()) {
                    terminal = message
                    // The hub drops a part it could not deliver within 5 s (full buffer) and counts it anyway: the end's
                    // count against the parts received tells a loss the seq check could not (the last part).
                    if (message.hasEnd()) {
                        val counted = Integer.toUnsignedLong(message.end.parts)
                        if (counted > received) throw InvalidMessageException("stream ${request.type} lost ${counted - received} part(s)")
                    }
                    break
                }
                // Parts arrive with seq 1, 2, 3, ... (the handler's): a jump is a part the hub dropped.
                val seq = message.part.seq
                when {
                    seq == received + 1 -> received++
                    seq > received + 1 -> throw InvalidMessageException("stream ${request.type} lost ${seq - received - 1} part(s)")
                    else -> throw InvalidMessageException("stream ${request.type} part seq $seq out of order (expected ${received + 1})")
                }
                try {
                    onPart(message.part)
                } catch (e: CancellationException) {
                    throw e
                } catch (e: Throwable) {
                    throw StreamCollectorException(e)
                }
                if (++consumed >= refill) {
                    consumed = 0
                    stream.out.trySend(ClientMessage.newBuilder().setFlow(Flow.newBuilder().setRequestId(request.id).setCredits(refill)).build())
                }
                guardMs = bound + margin
            }
            return terminal!!
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            pending.remove(request.id, entry)
            // The stream is still open on the hub when it was not the stream to the hub that failed: tell it to stop.
            if (terminal == null && failure !is HubConnectionLostException && failure !is HubUnavailableException) {
                val reason = when (failure) {
                    null, is CancellationException -> "collector cancelled"
                    is MessageTimeoutException -> "no part within the local guard"
                    is InvalidMessageException -> failure.message // a lost or out-of-order part
                    is StreamCollectorException -> failure.cause!!.let { "collector failed: ${it::class.simpleName}: ${it.message}" }
                    else -> "collector failed: ${failure::class.simpleName}: ${failure.message}"
                }
                stream.out.trySend(ClientMessage.newBuilder().setCancel(Cancel.newBuilder().setRequestId(request.id).setReason(reason)).build())
            }
        }
    }

    /**
     * Sends [request] as a subscription query (`mode = SUBSCRIPTION` is set here; `initial_credit` is the caller's window,
     * at least 1) and waits for the initial part: returns [SubscribeResult.Open] with the open [HubSubscription], or
     * [SubscribeResult.Closed] with the terminal message when the hub refused it or the initial result failed (an `Error`,
     * or a `ReplyEnd` with an error). [Request.getId] must be fresh for every call. The pending entry stays registered
     * after an open result until the subscription ends or is cancelled; see [HubSubscription].
     *
     * The local guard is the unary one: the hub's deadline bound for `timeout_ms` plus [HubProtocol.LOCAL_GUARD_MARGIN]
     * (`timeout_ms` bounds the initial request, queue wait included); a quiet subscription afterwards is never timed out.
     *
     * @throws HubUnavailableException, HubConnectionLostException as [send]
     * @throws MessageTimeoutException if no part or end arrives within the local guard (a `Cancel` is written)
     * @throws InvalidMessageException `subscription <type> lost N part(s)` when the first part's `seq` is not 1
     */
    override suspend fun subscribe(request: Request): SubscribeResult = subscribe(request, awaitStream = true)

    /** [subscribe]; [awaitStream] as for [send]. */
    internal suspend fun subscribe(request: Request, awaitStream: Boolean): SubscribeResult {
        val stream = liveStream(awaitStream)
        val ack = stream.ack!!
        val window = request.initialCredit.takeIf { it > 0 } ?: HubProtocol.DEFAULT_INITIAL_CREDIT
        val outgoing = request.toBuilder().setMode(RequestMode.SUBSCRIPTION).setInitialCredit(window).build()
        val guardMs = HubProtocol.localGuardMs(outgoing.timeoutMs, outgoing.type, ack)
        val entry = Pending.Multi(stream, boundedByDeadline = false)
        require(pending.putIfAbsent(request.id, entry) == null) { "request id ${request.id} is already in flight" }
        var opened = false
        var terminal: ServerMessage? = null
        var failure: Throwable? = null
        try {
            write(stream, outgoing)
            val first = withTimeoutOrNull(guardMs) { entry.messages.receive() } ?: throw MessageTimeoutException(request.type)
            if (!first.hasPart()) {
                terminal = first
                return SubscribeResult.Closed(first)
            }
            // The initial part is seq 1 (the hub's own numbering): another number is a part the hub dropped.
            val seq = first.part.seq
            if (seq != 1L) {
                throw InvalidMessageException(
                    if (seq > 1) "subscription ${request.type} lost ${seq - 1} part(s)" else "subscription ${request.type} part seq $seq out of order (expected 1)",
                )
            }
            opened = true
            return SubscribeResult.Open(OpenSubscription(request, stream, entry, first.part, window))
        } catch (e: Throwable) {
            failure = e
            throw e
        } finally {
            if (!opened) {
                pending.remove(request.id, entry)
                // The subscription is still open on the hub when it was not the stream to the hub that failed: tell it to stop.
                if (terminal == null && failure !is HubConnectionLostException && failure !is HubUnavailableException) {
                    val reason = when (failure) {
                        null, is CancellationException -> "subscriber cancelled"
                        is MessageTimeoutException -> "no initial result within the local guard"
                        else -> "subscriber failed: ${failure::class.simpleName}: ${failure.message}"
                    }
                    stream.out.trySend(ClientMessage.newBuilder().setCancel(Cancel.newBuilder().setRequestId(request.id).setReason(reason)).build())
                }
            }
        }
    }

    /** An open subscription of this instance as a sender: reads the parts of [entry], refills the credit, cancels on request. */
    private inner class OpenSubscription(
        private val request: Request,
        private val stream: Stream,
        private val entry: Pending.Multi,
        override val initial: ReplyPart,
        window: Int,
    ) : HubSubscription {
        /** Parts consumed per `Flow{refill}`: half the window, at least 1 (the initial part counts: it took a credit). */
        private val refill = maxOf(1, window / 2)
        private var received = 1L // the initial part
        private var consumed = 0
        private var handedOver = false // the part returned by the last next(): consumed when next() is called again
        private val cancelled = AtomicBoolean(false)

        init {
            // The initial part went to the caller with subscribe(): it counts as consumed now, not at the first next(), so a
            // caller that starts collecting late does not hold the hub's credit back (as the Rust library does).
            consumedOne()
        }

        private fun consumedOne() {
            if (++consumed >= refill) {
                consumed = 0
                stream.out.trySend(ClientMessage.newBuilder().setFlow(Flow.newBuilder().setRequestId(request.id).setCredits(refill)).build())
            }
        }

        override suspend fun next(): ServerMessage? {
            if (handedOver) { // the part returned last time is consumed now
                handedOver = false
                consumedOne()
            }
            val result = entry.messages.receiveCatching()
            val message = result.getOrNull()
            if (message == null) {
                result.exceptionOrNull()?.let { throw it } // the stream broke: HubConnectionLostException
                return null // cancelled locally (or the terminal message was handed over already)
            }
            if (message.hasPart()) {
                val seq = message.part.seq
                when {
                    seq == received + 1 -> received++
                    seq > received + 1 -> lost("subscription ${request.type} lost ${seq - received - 1} part(s)")
                    else -> lost("subscription ${request.type} part seq $seq out of order (expected ${received + 1})")
                }
                handedOver = true
                return message
            }
            // The end's count against the parts received tells a loss the seq check could not (the last part).
            if (message.hasEnd()) {
                val counted = Integer.toUnsignedLong(message.end.parts)
                if (counted > received) lost("subscription ${request.type} lost ${counted - received} part(s)")
            }
            return message
        }

        private fun lost(text: String): Nothing {
            cancel(text) // still open on the hub when a part went missing mid-way; a no-op after the end
            throw InvalidMessageException(text)
        }

        override fun cancel(reason: String) {
            if (!cancelled.compareAndSet(false, true)) return
            // Still registered = not ended and its stream not broken: only then is there anything to cancel.
            val live = pending.remove(request.id, entry)
            entry.messages.close()
            if (live) {
                stream.out.trySend(ClientMessage.newBuilder().setCancel(Cancel.newBuilder().setRequestId(request.id).setReason(reason)).build())
            }
        }
    }

    /** The live stream (waiting up to `requestTimeout` for one when [awaitStream]), its handshake completed. */
    private suspend fun liveStream(awaitStream: Boolean): Stream {
        val wait = settings.chosenRequestTimeout ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT
        if (!awaitStream && current?.ack == null) throw HubUnavailableException("hub $target not connected")
        if (!connectedState.value) {
            withTimeoutOrNull(wait.toMillis()) { connectedState.first { it } }
                ?: throw HubUnavailableException("hub not connected within $wait")
        }
        val stream = current ?: throw HubUnavailableException("hub not connected")
        if (stream.ack == null) throw HubUnavailableException("hub not connected")
        return stream
    }

    private suspend fun write(stream: Stream, request: Request) {
        try {
            stream.out.send(ClientMessage.newBuilder().setRequest(request).build())
        } catch (_: ClosedSendChannelException) {
            throw HubUnavailableException("hub stream closed before the request was written")
        }
    }

    /**
     * The hub's deadline for a request of [type] sent with `timeout_ms = 0`: the ack's per-type override, else its
     * default; [HubProtocol.DEFAULT_REQUEST_TIMEOUT] before any handshake. The retry budget when the caller chose no
     * timeout.
     */
    override fun hubTimeoutFor(type: String): Duration {
        val ack = ackState.value ?: return HubProtocol.DEFAULT_REQUEST_TIMEOUT
        val ms = ack.typeTimeoutsMsMap[type]?.takeIf { it > 0 } ?: ack.defaultTimeoutMs.takeIf { it > 0 }
        return ms?.let { Duration.ofMillis(it.toLong()) } ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT
    }

    /**
     * The longest the hub may keep a request of [type] sent with [timeoutMs] before it answers `TIMEOUT`
     * ([HubProtocol.hubDeadlineBoundMs]); before any handshake by the rule's fallback clamp (60 s).
     */
    override fun hubDeadlineBound(type: String, timeoutMs: Int): Duration =
        Duration.ofMillis(HubProtocol.hubDeadlineBoundMs(timeoutMs, type, ackState.value ?: RegisterAck.getDefaultInstance()))

    /**
     * [send] waits up to `request-timeout` (else 5 s) for a stream, then the request's local guard: the hub's deadline bound for
     * [timeoutMs] plus [HubProtocol.LOCAL_GUARD_MARGIN].
     */
    override fun callBound(type: String, timeoutMs: Int): Duration =
        HubProtocol.hubWait(0, settings.requestTimeout).plus(hubDeadlineBound(type, timeoutMs)).plus(HubProtocol.LOCAL_GUARD_MARGIN)

    /**
     * Closes the stream, stops reconnecting and fails every request in flight. What is queued for the hub (a subscription's
     * last updates and `ReplyEnd`) is written first: the outbound queue is closed, which half-closes the request side after
     * its last message, and the hub's answer (it finishing its side) is awaited, for [STOP_CLOSE_TIMEOUT] at most.
     * Cancelling the loop at once would shut the channel down under what gRPC had not written yet, and a hub that never saw
     * a subscription target's end fails the subscription with `HANDLER_DISCONNECTED`.
     */
    suspend fun stop() {
        val (job, stream) = synchronized(lock) {
            stopping = true
            current?.out?.close()
            loop to current
        }
        failPending(null, HubConnectionLostException("hub connection stopped"))
        if (stream != null) withTimeoutOrNull(STOP_CLOSE_TIMEOUT.toMillis()) { stream.closed.await() }
        renewals?.cancel()
        job?.cancelAndJoin()
        failPending(null, HubConnectionLostException("hub connection stopped"))
    }

    /** The rejection message last logged at `error`; touched only by the connect loop. */
    private var lastLoggedRejection: String? = null

    /** [rejection] currently says the token endpoint refused (no stream was opened), not the hub; touched only by the connect loop. */
    private var tokenRejected = false

    /** What a new stream's `Register` carries: the access token or the static token, or nothing. */
    private suspend fun initialCredentials(): Credentials? {
        if (settings.clientCertificate != null) return null // the TLS handshake is the credential
        val provider = settings.tokenProvider
        return if (provider != null) Credentials.newBuilder().setJwt(provider.token().value).build() else staticCredentials()
    }

    private fun staticCredentials(): Credentials? = settings.token.current()?.let { Credentials.newBuilder().setToken(it).build() }

    private suspend fun connectLoop() {
        var backoff = settings.backoffMin
        var failures = 0
        var hintLogged = false // the TLS hint has reached WARN since the last handshake
        while (currentCoroutineContext().isActive && !stopping) {
            var channel: ManagedChannel? = null
            val stream = Stream(Channel(Channel.UNLIMITED), initialPermits().coerceAtLeast(1))
            var status: Status = Status.CANCELLED
            var reachedHandshake = false
            var noToken: HubTokenException? = null
            try {
                // A stream is not opened without its credential: the token provider answers first (the cached token, or one fetch for everybody).
                stream.credentials = initialCredentials()
                if (tokenRejected) { // the endpoint answers again
                    tokenRejected = false
                    rejection.value = null
                    lastLoggedRejection = null
                }
                // The files are read again for every attempt; what this stream presents is at least this generation.
                settings.clientCertificate?.let {
                    certificatePresented = it.refresh(connecting = true)
                    certificateGeneration = it.generation
                    // The hub refuses a certificate that has ended: say so here, through the rejection path, without a handshake.
                    if (it.expired()) {
                        throw LocalRejection(
                            Status.UNAUTHENTICATED.withDescription("client certificate ${it.current.fingerprint} of ${it.current.service} expired at ${it.current.notAfter}: renew the files"),
                        )
                    }
                }
                // Inside the try: a factory that throws (e.g. an unparsable address) is a failed attempt, not the end of the loop.
                channel = channelFactory()
                liveChannel = channel
                openAndCollect(channel, stream)
                status = Status.UNAVAILABLE.withDescription("hub closed the stream")
            } catch (e: CancellationException) {
                throw e
            } catch (e: LocalRejection) {
                status = e.status
            } catch (e: HubTokenException) {
                noToken = e
                status = Status.UNAUTHENTICATED.withDescription("no access token: ${e.message}").withCause(e)
            } catch (e: HeartbeatTimeoutException) {
                status = Status.UNAVAILABLE.withDescription(e.message)
            } catch (e: HandshakeTimeoutException) {
                status = Status.DEADLINE_EXCEEDED.withDescription(e.message)
            } catch (e: Throwable) {
                status = Status.fromThrowable(e)
            } finally {
                synchronized(lock) {
                    if (current === stream) current = null
                    stream.out.close()
                }
                reachedHandshake = stream.ack != null
                firstAttemptEnded.value = true
                connectedState.value = false
                ackState.value = null
                acknowledged.value = -1
                channel?.shutdownNow()
                // Requests still in the closed outbound queue were never handed to gRPC: never sent, so retryable elsewhere.
                failUnsent(stream, "hub stream to $target closed before the request was written: ${describe(status)}")
                failPending(stream, HubConnectionLostException("hub stream closed: ${describe(status)}"))
                // The hub forgot this stream's streaming targets (their senders got HANDLER_DISCONNECTED): a handler's
                // flow waiting for credit would otherwise never move again, holding its permit.
                cancelStreamingHandlers(stream, "hub stream closed: ${describe(status)}")
                // Likewise its subscriptions: the hub forgot them, nobody would ever hear their updates.
                val dropped = subscriptions.closeOwner(stream)
                if (dropped > 0) log.info("Hub stream of {} ({}) closed: {} subscription(s) dropped", serviceName, instanceId, dropped)
                stream.closed.complete(Unit)
            }
            if (stopping) break
            if (rollingStream === stream) {
                // A rollover closed this stream on purpose: reconnect at once with the pair the files hold, nothing failed.
                rollingStream = null
                backoff = settings.backoffMin
                failures = 0
                hintLogged = false
                continue
            }
            // The hub does not accept the token this stream presented: the next stream must not present it again.
            if (noToken == null && status.code == Status.Code.UNAUTHENTICATED) {
                val provider = settings.tokenProvider
                val jwt = synchronized(stream) { stream.credentials }?.takeIf { it.hasJwt() }?.jwt
                if (provider != null && jwt != null) provider.invalidate(jwt)
            }
            // A stream the hub acknowledged proves the hub is back: start again from the floor.
            if (reachedHandshake) {
                backoff = settings.backoffMin
                failures = 0
                hintLogged = false
                rejection.value = null // a rejection no longer describes a hub that has answered since
                lastLoggedRejection = null
            }
            failures++
            val wait = if (noToken != null) {
                // Not the hub's answer: the stream was never opened. Retried with the normal back-off (the hub may be fine, the endpoint is not).
                tokenRejected = true
                rejection.value = status
                val retry = ReconnectBackoff.jittered(backoff, settings.backoffMax)
                backoff = ReconnectBackoff.next(backoff, settings.backoffMax)
                val text = "No access token for {} ({}): {}; not connecting to {}, retrying in {}"
                val message = describe(status)
                if (message != lastLoggedRejection) {
                    lastLoggedRejection = message
                    log.error(text, serviceName, instanceId, noToken.message, target, retry)
                } else {
                    log.debug(text, serviceName, instanceId, noToken.message, target, retry)
                }
                retry
            } else if (reachedHandshake && settings.clientCertificate != null && status.code == Status.Code.UNAUTHENTICATED &&
                status.description?.contains("certificate expired") == true
            ) {
                // The hub closes a certificate session at notAfter, with no grace: the files are read again, so a renewed pair is presented.
                val retry = ReconnectBackoff.jittered(settings.backoffMin, settings.backoffMax)
                log.info(
                    "The hub closed the session of {} ({}) because its client certificate ended; reconnecting in {} with the pair the files hold now",
                    serviceName, instanceId, retry,
                )
                retry
            } else if (isRegistrationRejection(status)) {
                rejection.value = status
                onRejected(REJECTION_SEQ.incrementAndGet(), HubRejection(lastHubId ?: target, status.code.name, status.description, clock.instant()))
                val retry = ReconnectBackoff.jittered(settings.backoffMax, settings.backoffMax)
                // The same message repeats at every attempt: one error line, then debug (a refused service retries for as long as it runs).
                val text = "Hub rejected the registration of {} ({}): {} {}; retrying in {}"
                val message = describe(status)
                if (message != lastLoggedRejection) {
                    lastLoggedRejection = message
                    log.error(text, serviceName, instanceId, status.code.name, status.description, retry)
                } else {
                    log.debug(text, serviceName, instanceId, status.code.name, status.description, retry)
                }
                retry
            } else {
                val retry = ReconnectBackoff.jittered(backoff, settings.backoffMax)
                val text = "Hub stream of {} ({}) closed: {}{}; reconnecting in {}"
                val hint = if (reachedHandshake) "" else transportHint(status)
                // WARN on the first failure and, once, on the first failure that carries the TLS hint whatever its number
                // (the first attempt may have been a refusal because the hub was still starting); DEBUG otherwise.
                val firstHint = hint.isNotEmpty() && !hintLogged
                if (hint.isNotEmpty()) hintLogged = true
                if (failures == 1 || firstHint) log.warn(text, serviceName, instanceId, describe(status), hint, retry)
                else log.debug(text, serviceName, instanceId, describe(status), hint, retry)
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
            // The ack sets the interval: the first tick waits for the handshake. A hub that does not acknowledge the
            // registration in time fails the stream (throwing here ends it like any failure).
            withTimeoutOrNull(settings.handshakeTimeout.toMillis()) { stream.handshakeReached.await() }
                ?: throw HandshakeTimeoutException("registration not confirmed in time: no RegisterAck within ${settings.handshakeTimeout.toMillis()} ms")
            while (true) {
                val interval = stream.heartbeatIntervalMs
                delay(interval)
                // The silent-hub check, on the tick and before its heartbeat (see hubSilent). Throwing fails this coroutineScope, which
                // cancels the collector and ends the stream like any failure.
                stream.silentFor(interval)?.let { ms ->
                    throw HeartbeatTimeoutException("no heartbeat echo from the hub for $ms ms ($SILENT_HEARTBEATS heartbeats unanswered)")
                }
                heartbeat(stream)
                if (settings.tokenProvider == null) refreshCredentials(stream) // a rotated token file reaches the hub without a reconnect
            }
        }
        // One lane per stream for the control requests: in order, and never on the read loop below. In the connection's
        // scope, so the end of the stream never waits for a control handler.
        val controls = scope.launch(handlerDispatcher) {
            for (request in stream.controls) serveControl(stream, request)
        }
        try {
            HubGrpcKt.HubCoroutineStub(channel).connect(stream.out.receiveAsFlow()).collect { message -> handle(stream, message) }
        } finally {
            stream.controls.close()
            controls.cancel()
        }
        heartbeats.cancel()
    }

    /** Hands [request] to the control; its answer goes back on [stream] (dropped when the stream is gone by then: the hub failed it). */
    private fun serveControl(stream: Stream, request: ControlRequest) {
        val reply = { answer: ControlReply -> stream.out.trySend(ClientMessage.newBuilder().setControlReply(answer).build()); Unit }
        try {
            control.handle(request, reply)
        } catch (e: Exception) {
            log.warn("Control request {} on processor {} failed: {}", request.controlId, request.processor, e.toString())
            reply(
                ControlReply.newBuilder().setControlId(request.controlId).setRefused(
                    ControlRefused.newBuilder().setCode(ControlRefusal.CONTROL_REFUSAL_FAILED).setMessage(EventConsumer.utf8Cut(e.toString(), 512)),
                ).build(),
            )
        }
    }

    private fun buildRegister(
        commandTypes: Set<String>,
        queryTypes: Set<String>,
        sharedQueryTypes: Set<String>,
        subscriptionQueryTypes: Set<String>,
    ): Register = Register.newBuilder()
        .setServiceName(serviceName)
        .setInstanceId(instanceId)
        .addAllCommandTypes(commandTypes.sorted())
        .addAllQueryTypes(queryTypes.sorted())
        .addAllSharedQueryTypes(sharedQueryTypes.sorted())
        .addAllSubscriptionQueryTypes(subscriptionQueryTypes.sorted())
        .setClientVersion(HubProtocol.CLIENT_VERSION)
        .setLoadFactor(loadFactor)
        .putAllTags(tags)
        .build()

    /** Writes the current registration followed by a heartbeat whose echo confirms it; returns that heartbeat's `sent_at_ms`. Caller holds [lock]. */
    private fun announce(stream: Stream): Long {
        val register = registration.toBuilder().setInitialPermits(stream.initialPermits)
            .apply { if (stream.draining) clearCommandTypes().clearQueryTypes().clearSharedQueryTypes().clearSubscriptionQueryTypes() }
            // Every Register of the stream carries the stream's token (the hub reads it from the first one only).
            .apply { synchronized(stream) { stream.credentials }?.let { credentials = it } }
            .build()
        stream.out.trySend(ClientMessage.newBuilder().setRegister(register).build())
        stream.registeredGeneration = generation.value
        return heartbeat(stream)
    }

    /**
     * Writes an `InstanceStatus` with the event [processors] on the live stream; false (nothing written) while no
     * stream has completed its handshake.
     */
    fun reportStatus(connectedHubIds: List<String>, handlersInFlight: Int, processors: List<ProcessorStatus> = emptyList()): Boolean {
        val stream = current ?: return false
        if (stream.ack == null) return false
        val status = InstanceStatus.newBuilder()
            .addAllConnectedHubIds(connectedHubIds)
            .setHandlersInFlight(handlersInFlight.coerceAtLeast(0))
            .addAllProcessors(processors)
        return stream.out.trySend(ClientMessage.newBuilder().setStatus(status).build()).isSuccess
            .also { if (it) statusReports.incrementAndGet() }
    }

    /**
     * Tops the live stream up to [share] permits offered in total (`initial_permits` plus earlier top-ups; multi-hub: a
     * hub went away and [HubConnections] raises the others' share). Writes `Permits{count = share - offered, grant =
     * share}` when that is positive and the stream completed its handshake: the grant raises the hub's cap on the session
     * (`initial_permits` until then) to the share, so the top-up is kept rather than clamped away. Returns the count
     * written (0 = nothing).
     */
    internal fun topUpPermits(share: Int): Int {
        val stream = current ?: return 0
        if (stream.ack == null) return 0
        val extra = synchronized(stream) {
            val extra = share - stream.offered
            if (!stream.permits.toppedUp(extra)) return 0
            stream.offered = share
            extra
        }
        writePermits(stream, extra, grant = share)
        return extra
    }

    private val statusReports = AtomicLong()

    /** `InstanceStatus` reports written to this hub over all of its streams. */
    val statusReportsWritten: Long get() = statusReports.get()

    private fun heartbeat(stream: Stream): Long {
        val sentAt = synchronized(lock) {
            // Strictly increasing so each echo identifies its heartbeat; stays within a few ms of the wall clock.
            maxOf(System.currentTimeMillis(), lastHeartbeatAt + 1).also { lastHeartbeatAt = it }
        }
        stream.probes[sentAt] = stream.registeredGeneration
        if (stream.out.trySend(ClientMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtMs(sentAt)).build()).isSuccess) {
            stream.heartbeatWritten()
        }
        return sentAt
    }

    private fun handle(stream: Stream, message: ServerMessage) {
        when {
            message.hasRequest() -> dispatch(stream, message.request)
            message.hasReply() -> pending.remove(message.reply.requestId)?.finish(message)
            message.hasError() -> pending.remove(message.error.requestId)?.finish(message)
            // Parts and ends of a gather or stream nobody waits for (it ended locally, or never was ours) are dropped.
            message.hasPart() -> (pending[message.part.requestId] as? Pending.Multi)?.messages?.trySend(message)
            message.hasEnd() -> {
                val id = message.end.requestId
                (pending[id] as? Pending.Multi)?.let { if (pending.remove(id, it)) it.finish(message) }
            }
            // Credit for a stream this instance serves; a Flow for an unknown id (ended, or never forwarded here) is dropped.
            message.hasFlow() -> running[message.flow.requestId]?.credit?.add(message.flow.credits)
            // The hub gave up on a request this instance is handling (a stream, or a gather target past its end): stop it.
            message.hasCancel() -> {
                running[message.cancel.requestId]?.let { entry ->
                    log.debug("Hub cancelled request {}: {}", message.cancel.requestId, message.cancel.reason)
                    entry.job.cancel(CancellationException("cancelled by the hub: ${message.cancel.reason}"))
                }
                // A subscription this instance serves: after the job above, so a registration still running sees
                // its cancellation (see serveSubscription) and one already registered is found here.
                if (subscriptions.cancel(stream, message.cancel.requestId)) {
                    log.debug("Hub cancelled subscription {}: {}", message.cancel.requestId, message.cancel.reason)
                }
            }
            message.hasControl() -> stream.controls.trySend(message.control)
            message.hasRegisterAck() -> onAck(stream, message.registerAck)
            message.hasHeartbeat() -> {
                stream.echoed()
                val sentAt = message.heartbeat.sentAtMs
                val generation = stream.probes[sentAt] ?: return // an unsolicited keep-alive
                stream.probes.headMap(sentAt, true).clear()
                if (current === stream) acknowledged.value = maxOf(acknowledged.value, generation)
            }
        }
    }

    /** A `RegisterAck`: the first one of the stream completes the handshake, a later one (a re-registration) replaces the values. */
    private fun onAck(stream: Stream, ack: RegisterAck) {
        if (ack.heartbeatIntervalMs > 0) stream.heartbeatIntervalMs = ack.heartbeatIntervalMs.toLong()
        val first = stream.ack == null
        stream.ack = ack
        lastHubId = ack.hubId.ifEmpty { target }
        if (current === stream) {
            ackState.value = ack
            if (first) {
                connectedState.value = true
                rejection.value = null
                log.info("Connected to hub {} as {} ({}): heartbeat every {} ms", ack.hubId, serviceName, instanceId, stream.heartbeatIntervalMs)
            }
        }
        if (!first) return
        stream.watchSilence()
        stream.handshakeReached.complete(Unit)
        refreshCredentials(stream) // a credential renewed between this stream's Register and its ack
    }

    private val refreshes = AtomicLong()

    /** `ClientMessage.credentials` written to this hub over all of its streams (`snodra_hub_client_credentials_refreshes_sent_total`). */
    val credentialsRefreshesSent: Long get() = refreshes.get()

    /**
     * Gives the live [stream] the credential the hub should know now when it differs from the one it was told: the provider's
     * latest access token, or a static token that changed in its file (a rotation without reconnecting). Nothing before the
     * handshake: the ack calls it again.
     */
    private fun refreshCredentials(stream: Stream) {
        if (current !== stream || settings.clientCertificate != null) return
        if (stream.ack == null) return
        val provider = settings.tokenProvider
        val wanted = (if (provider != null) provider.current.value?.let { Credentials.newBuilder().setJwt(it.value).build() } else staticCredentials()) ?: return
        val changed = synchronized(stream) { (wanted != stream.credentials).also { if (it) stream.credentials = wanted } }
        if (changed && stream.out.trySend(ClientMessage.newBuilder().setCredentials(wanted).build()).isSuccess) {
            refreshes.incrementAndGet()
            onCredentialsRefreshSent()
            log.debug("Credentials refreshed on the stream of {} ({}) to {}", serviceName, instanceId, target)
        }
    }

    /**
     * Runs the inbound handler on [handlerDispatcher] (bounded by the [gate]'s permits) so neither a slow nor a
     * blocking handler holds up the stream; always answers the request, on the stream it arrived on: a `Reply`, or
     * for a `STREAMING` request the parts and the `ReplyEnd` ([serveStream]). Unless the hub cancelled it: then nothing
     * goes back. The hub's permit for the request is paid back when the coroutine completes, whichever way.
     */
    private fun dispatch(stream: Stream, request: Request) {
        if (request.mode == RequestMode.SUBSCRIPTION) {
            dispatchSubscription(stream, request)
            return
        }
        stream.permits.requestReceived()
        inFlight.update { it + 1 }
        gate.entered()
        val streaming = request.mode == RequestMode.STREAMING
        val entry = Running(stream, if (streaming) StreamCredit(request.initialCredit) else null)
        // Lazy, so a Flow or Cancel arriving right after the request finds the entry.
        val job = scope.launch(handlerDispatcher, start = CoroutineStart.LAZY) {
            if (streaming) serveStream(stream, request, entry) else serveUnary(stream, request)
        }
        entry.job = job
        running[request.id] = entry
        job.invokeOnCompletion {
            running.remove(request.id, entry)
            inFlight.update { it - 1 }
            gate.left()
            grantPermit(stream)
        }
        job.start()
    }

    /**
     * A `SUBSCRIPTION` request (an update target): the hub wrote it outside flow control, so it takes no handler
     * permit, no [gate] slot and is no in-flight handler, and none is paid back ([PermitLedger] never sees it). It is
     * registered in [subscriptions] right here, when the request is read, as pending: the admission then runs on
     * [handlerDispatcher] (it validates the forwarded token), and in the meantime the emitter's updates for it are held in
     * order (written once it is admitted, dropped when it is refused or the hub cancels it or its stream is lost).
     */
    private fun dispatchSubscription(stream: Stream, request: Request) {
        val sink = subscriptionSink(stream)
        // Registered here, in the reader, before the admission runs (pending): the hub writes `<id>#n` before the initial
        // request `<id>#0` to this instance, and an update emitted after #0 was read must not slip past the registration.
        val subscription = when (val opening = subscriptions.openPending(stream, request.id, request.type, request.payload.toByteArray(), sink)) {
            is Opening.Refused -> {
                sink.end(request.id, 0, opening.error) // the limit, or a duplicate id: nothing was registered
                return
            }
            is Opening.Pending -> opening.subscription
        }
        val entry = Running(stream, null)
        // Lazy, so a Cancel arriving right after the request finds the entry.
        val job = scope.launch(handlerDispatcher, start = CoroutineStart.LAZY) { serveSubscription(stream, request, subscription) }
        entry.job = job
        running[request.id] = entry
        job.invokeOnCompletion { cause ->
            running.remove(request.id, entry)
            if (cause != null) { // cancelled before it was admitted or refused: nothing of it stays
                subscriptions.remove(subscription)
                subscription.close()
            }
        }
        job.start()
    }

    private fun subscriptionSink(stream: Stream) = object : SubscriptionSink {
        override fun part(requestId: String, seq: Long, payload: ByteArray): Boolean =
            stream.out.trySend(
                ClientMessage.newBuilder().setPart(
                    ReplyPart.newBuilder().setRequestId(requestId).setPayload(ByteString.copyFrom(payload)).setSeq(seq),
                ).build(),
            ).isSuccess

        override fun end(requestId: String, parts: Int, error: Error?): Boolean {
            val end = ReplyEnd.newBuilder().setRequestId(requestId).setParts(parts)
            if (error != null) end.setError(error)
            return stream.out.trySend(ClientMessage.newBuilder().setEnd(end).build()).isSuccess
        }
    }

    /** Admits the pending [subscription]: it goes live (what was held is written) or is refused with the admission's error. */
    private suspend fun serveSubscription(stream: Stream, request: Request, subscription: ServedSubscription) {
        val error: Error? = try {
            inboundSubscription(request) { query -> subscriptions.activate(subscription, query); null }
        } catch (e: Throwable) {
            if (e is CancellationException && !currentCoroutineContext().isActive) throw e // a hub Cancel during the admission
            log.warn("Subscription {} request {} failed", request.type, request.id, e)
            Error.newBuilder().setRequestId(request.id).setCode(ErrorCode.HANDLER_ERROR).setMessage("${e::class.simpleName}: ${e.message}").build()
        }
        if (error == null) {
            // Admitted. A hub Cancel that arrived meanwhile cancelled this job and removed the entry (dropped: nothing was
            // written); one that raced the activation is caught here (see handle()).
            if (!currentCoroutineContext().isActive) subscriptions.cancel(stream, request.id)
            return
        }
        // Refused: the target ends with the error and the hub ends the whole subscription with it. Nothing is written when
        // the subscription was cancelled or its stream lost meanwhile.
        subscriptions.refuse(subscription, error)
    }

    private suspend fun serveUnary(stream: Stream, request: Request) {
        val reply = try {
            gate.permits.withPermit { inbound(request) }
        } catch (e: Throwable) {
            // Only this coroutine's own cancellation (a hub Cancel) propagates; a CancellationException leaking out of the
            // handler (e.g. a TimeoutCancellationException from its own withTimeout) is a handler failure like any other.
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

    /**
     * Serves a `STREAMING` request: [inboundStream] runs under the handler permit; each part it emits waits for credit
     * (at most [HubSettings.streamCreditTimeout]: then the stream ends with `TIMEOUT`, the hub has forgotten it) and
     * goes out with the next `seq`; then the `ReplyEnd` (with the error it returned, or `HANDLER_ERROR` for an
     * exception that leaked). Cancelled by the hub's `Cancel` or the stream closing: nothing goes back then.
     */
    private suspend fun serveStream(stream: Stream, request: Request, entry: Running) {
        val credit = entry.credit!!
        val creditTimeoutMs = settings.streamCreditTimeout.toMillis()
        var seq = 0L
        val error: Error? = try {
            gate.permits.withPermit {
                inboundStream(request) { payload ->
                    withTimeoutOrNull(creditTimeoutMs) { credit.take() } ?: throw NoCreditException(creditTimeoutMs)
                    val part = ReplyPart.newBuilder().setRequestId(request.id).setPayload(ByteString.copyFrom(payload)).setSeq(++seq)
                    try {
                        stream.out.send(ClientMessage.newBuilder().setPart(part).build())
                    } catch (_: ClosedSendChannelException) {
                        // The stream it arrived on is gone, the hub with its credits: stop the handler's flow.
                        entry.job.cancel(CancellationException("hub stream closed while streaming ${request.type} ${request.id}"))
                        throw CancellationException("hub stream closed")
                    }
                }
            }
        } catch (e: NoCreditException) {
            // The handler's flow was held in emit for the whole wait: the hub lost the stream (or the collector stopped
            // consuming without cancelling). The flow saw a cancellation, as at a hub Cancel; the permits go back with
            // this coroutine.
            log.warn("Streaming {} request {} ended: {}", request.type, request.id, e.message)
            Error.newBuilder().setRequestId(request.id).setCode(ErrorCode.TIMEOUT).setMessage(e.message).build()
        } catch (e: Throwable) {
            if (e is CancellationException && !currentCoroutineContext().isActive) throw e // a hub Cancel: no end follows
            log.warn("Streaming {} request {} failed", request.type, request.id, e)
            Error.newBuilder().setRequestId(request.id).setCode(ErrorCode.HANDLER_ERROR).setMessage("${e::class.simpleName}: ${e.message}").build()
        }
        val end = ReplyEnd.newBuilder().setRequestId(request.id).setParts(seq.coerceAtMost(Int.MAX_VALUE.toLong()).toInt())
        if (error != null) end.setError(error)
        try {
            stream.out.send(ClientMessage.newBuilder().setEnd(end).build())
        } catch (_: ClosedSendChannelException) {
            log.debug("End of {} stream {} dropped: the stream it arrived on is closed", request.type, request.id)
        }
    }

    /** Cancels the streaming handlers of requests that arrived on [stream] (the hub has forgotten them). */
    private fun cancelStreamingHandlers(stream: Stream, reason: String) {
        for (entry in running.values) {
            if (entry.stream === stream && entry.credit != null) entry.job.cancel(CancellationException(reason))
        }
    }

    /** One completed handler: pays a permit back to the stream the request arrived on. */
    private fun grantPermit(stream: Stream) = apply(stream, stream.permits.handlerCompleted())

    private fun apply(stream: Stream, step: PermitLedger.Step) {
        when (step) {
            is PermitLedger.Step.Flush -> writePermits(stream, step.count)
            PermitLedger.Step.ArmTimer -> scope.launch {
                delay(HubProtocol.PERMIT_FLUSH_DELAY.toMillis())
                writePermits(stream, stream.permits.timerFired())
            }
            PermitLedger.Step.Wait -> Unit
        }
    }

    private fun writePermits(stream: Stream, count: Int, grant: Int = 0) {
        if (count <= 0) return
        // A closed stream drops them: the hub forgot that session's permits with it.
        stream.out.trySend(ClientMessage.newBuilder().setPermits(Permits.newBuilder().setCount(count).setGrant(grant)).build())
    }

    /**
     * Fails, with [HubUnavailableException] marked [HubUnavailableException.lostWhileQueued], the requests left in [stream]'s
     * closed outbound queue: each may move to another hub ([HubConnections.MAX_UNWRITTEN_MOVES] times at most).
     */
    private fun failUnsent(stream: Stream, message: String) {
        while (true) {
            val queued = stream.out.tryReceive().getOrNull() ?: break
            if (!queued.hasRequest()) continue
            val id = queued.request.id
            val entry = pending[id] ?: continue
            if (entry.stream === stream && pending.remove(id, entry)) entry.fail(HubUnavailableException(message).apply { lostWhileQueued = true })
        }
    }

    /** Fails the requests written to [stream] (every request when null) with [cause]. */
    private fun failPending(stream: Stream?, cause: MessagingException) {
        for ((id, entry) in pending.entries.toList()) {
            if (stream == null || entry.stream === stream) {
                if (pending.remove(id, entry)) entry.fail(cause)
            }
        }
    }

    /**
     * What to check when a stream died before any handshake in the way a TLS mismatch dies (gRPC reports plain text to a
     * TLS listener as an `http2 exception` or a bare `io exception`, and TLS to a plain-text one as an `ssl exception`, never
     * as a refusal).
     */
    private fun transportHint(status: Status): String {
        // The rustls hub answers plain text with a TLS alert, which an HTTP/2 client reads as a protocol error (INTERNAL).
        if (status.code != Status.Code.UNAVAILABLE && status.code != Status.Code.INTERNAL) return ""
        val text = status.description?.lowercase() ?: return ""
        if (listOf("io exception", "http2 exception", "ssl", "tls", "handshake", "network closed for unknown reason").none { it in text }) return ""
        // A hub that cannot be reached at all (nothing listens, no route, no such host) has nothing to do with TLS.
        val unreachable = generateSequence(status.cause) { it.cause }.any {
            it is java.net.ConnectException || it is java.net.UnknownHostException || it is java.net.NoRouteToHostException || it is java.net.SocketTimeoutException
        }
        if (unreachable) return ""
        settings.clientCertificate?.current?.let { certificate ->
            // TLS is on already (a client certificate implies it): the other end of the handshake did not accept what this one presented, or the reverse.
            return " (the TLS handshake failed before the hub answered: check that the hub's SNODRA_SERVER_TLS_CLIENT_CA trusts the issuer of the client certificate " +
                "${certificate.fingerprint} of ${certificate.service} and that it has not ended (${certificate.notAfter}), and that snodra.messaging.hub.tls.ca-file " +
                "trusts the hub's own certificate)"
        }
        return " (the transport failed before the hub answered: if the hub serves TLS set snodra.messaging.hub.tls.enabled=true" +
            " and snodra.messaging.hub.tls.ca-file, if it serves plain text leave snodra.messaging.hub.tls.enabled off; a hub that requires client certificates " +
            "does that to a client that presents none: set snodra.messaging.hub.tls.client-cert-file and client-key-file)"
    }

    private fun describe(status: Status) = status.code.name + (status.description?.let { " ($it)" } ?: "")

    private class HeartbeatTimeoutException(message: String) : RuntimeException(message)

    private class HandshakeTimeoutException(message: String) : RuntimeException(message)

    /** A registration this client refuses to send (a client certificate that has ended): handled like the hub's `UNAUTHENTICATED`, without a stream. */
    private class LocalRejection(val status: Status) : RuntimeException(status.description)

    /**
     * A streaming handler waited [HubSettings.streamCreditTimeout] for credit: its stream ends with `TIMEOUT`. A
     * cancellation, so the handler's flow and the invoker let it through as they do a hub `Cancel` (anything else
     * would come back as the handler's own `HANDLER_ERROR`).
     */
    private class NoCreditException(timeoutMs: Long) : CancellationException("no credit within $timeoutMs ms")

    private companion object {
        /** How long [stop] waits for the hub to finish a stream it half-closed. */
        val STOP_CLOSE_TIMEOUT: Duration = Duration.ofSeconds(2)
    }
}
