package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Heartbeat
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.InstanceStatus
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.RegisterAck
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.ReplyEnd
import io.damoreira.kraken.hub.v1.ReplyPart
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.hub.v1.RequestMode
import io.damoreira.kraken.hub.v1.ServerMessage
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.channels.SendChannel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.launch
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit

/**
 * An in-process stand-in for the Rust kraken hub, for starter and service unit tests that must not start a container.
 *
 * It mirrors the observable protocol behaviour: the first message must be a `Register` (`FAILED_PRECONDITION`), a type
 * owned by another service is refused (`ALREADY_EXISTS`), a later `Register` replaces an instance's types, requests are
 * forwarded unchanged round-robin to instances of the owning service (`NO_HANDLER` if nobody owns the type), replies are
 * relayed by request id, `TIMEOUT` fires after the effective timeout and a late reply is dropped,
 * `HANDLER_DISCONNECTED` is sent for in-flight requests when the handler stream ends, heartbeats are echoed, empty
 * names are `INVALID_ARGUMENT` and malformed or duplicate in-flight requests are `INVALID_REQUEST`.
 *
 * Protocol v2 (the default): every accepted `Register` with `protocol_version >= 2` is answered with a `RegisterAck`
 * (written before the echo of the heartbeat that follows it) carrying [hubId], [heartbeatInterval],
 * [heartbeatTimeout], [replyTimeout] as the default, [maxTimeout] and [typeTimeouts]. The effective timeout follows the
 * hub's rule: `timeoutMs > 0` -> `min(timeoutMs, override or maxTimeout)`, `timeoutMs == 0` -> `override or
 * replyTimeout`. Set [legacy] to emulate a protocol v1 hub that never sends an ack.
 *
 * Flow control (protocol P1, capability `flow-control`): a registrant that declares the capability gets it back in the
 * ack; with `initial_permits > 0` it is permit-managed: a request is forwarded only while it has permits (one per
 * forward), otherwise queued per instance ordered by `Request.priority` (higher first) then arrival, keeping its own
 * deadline. `Permits` messages add permits and drain the queue; like kraken-hub, a session never holds more than its cap,
 * which starts at `initial_permits` and is raised (never lowered) to `Permits.grant` when a grant is above it (P3
 * multi-hub top-up). Set [permitCap] false to drop the cap (permits add up without bound) or [permitGrants] false to
 * emulate a hub that predates `Permits.grant` (the cap stays at `initial_permits`). A full queue ([queueCapacity]) answers
 * `RESOURCE_EXHAUSTED` to senders that share `flow-control` and `HANDLER_ERROR "handler busy"` to others. Set
 * [flowControl] false to emulate the P0 hub (protocol v2 without the capability: everything unlimited).
 *
 * Multi-hub status (P3, capability `instance-status`): a registrant that declares the capability gets it back in the ack
 * and its `InstanceStatus` reports are kept ([lastStatusFrom]); reports on a stream that did not negotiate it are counted
 * ([statusMessagesFrom]) and ignored. Set [instanceStatus] false to emulate the P2 hub. [pauseReading] stops reading
 * every stream (what clients write stays queued on their side), and [requestsFrom] counts requests per sender.
 *
 * Query modes (P4a, ADR 0017, capability `scatter-gather`): `shared_query_types` may be owned by many services; a type
 * registered exclusive (or as a command) by one registrant and shared by another, even of the same service, is
 * `ALREADY_EXISTS`, and one listed both ways in a single `Register` is `INVALID_ARGUMENT`. A point query on a shared type
 * round-robins over every owner's instances. A `SCATTER_GATHER` request from a session that negotiated the capability
 * sends every registered instance owning the type (sorted by service, then instance id) an ordinary unary request with
 * id `<id>#<n>`, flow control and queues applying per target; answers go back as `ReplyPart`s in arrival order (`seq`,
 * `responder_service`, `responder_instance`), then one `ReplyEnd{parts, failed, timed_out}` once every target answered,
 * after `max_results` successful parts, or at the deadline. A target whose handler stream ends fails as a part
 * (`HANDLER_DISCONNECTED`). No target: `NO_HANDLER`; a command: `INVALID_REQUEST`; a v2 sender without the capability
 * (or any other mode): `UNSUPPORTED`, a v1 sender `INVALID_REQUEST`. Set [scatterGather] false to emulate the P3 hub:
 * no capability in the ack and `shared_query_types` ignored. [inject] writes any message to an instance's stream.
 *
 * One session per instance id, like the real hub: the first `Register` of a stream whose instance id already has a
 * registered stream replaces that older stream (it ends, its in-flight requests fail with `HANDLER_DISCONNECTED`;
 * [sessionsReplacedOf] counts it). Set [replaceSessions] false to accept parallel streams of one instance id.
 *
 * It does not implement consistent hashing on `routing_key`, metrics, or the real hub's owner grace period and
 * heartbeat expiry.
 */
class InProcessHub(val name: String = "kraken-hub-" + UUID.randomUUID()) : AutoCloseable {

    /** Default reply timeout for requests with `timeoutMs == 0`. */
    @Volatile
    var replyTimeout: Duration = Duration.ofSeconds(5)

    /** Set false to stop echoing heartbeats (still counted), simulating a hub that silently stopped answering. */
    @Volatile
    var echoHeartbeats: Boolean = true

    /** Emulates a protocol v1 hub: no `RegisterAck`, ever. */
    @Volatile
    var legacy: Boolean = false

    /** `RegisterAck.hub_id`. */
    val hubId: String get() = name

    /** Announced in the ack; this stand-in never evicts on missed heartbeats. */
    @Volatile
    var heartbeatInterval: Duration = Duration.ofSeconds(3)

    /** Announced in the ack; this stand-in never evicts on missed heartbeats. */
    @Volatile
    var heartbeatTimeout: Duration = Duration.ofSeconds(10)

    /** Upper clamp for `Request.timeoutMs` on types without an override. */
    @Volatile
    var maxTimeout: Duration = Duration.ofSeconds(60)

    /** Per-type timeout overrides (`KRAKEN_HUB_TIMEOUT_OVERRIDES` of the real hub). */
    @Volatile
    var typeTimeouts: Map<String, Duration> = emptyMap()

    /** Set false to never send `TIMEOUT` (a hub that loses requests), so the client's local guard is what fires. */
    @Volatile
    var emitTimeouts: Boolean = true

    /** Set false to emulate the P0 hub: a v2 ack without the `flow-control` capability, every instance unlimited. */
    @Volatile
    var flowControl: Boolean = true

    /**
     * True (the default, as the real hub): a permit-managed session never holds more than its cap (`initial_permits`,
     * raised by `Permits.grant`). False lets permits add up without bound.
     */
    @Volatile
    var permitCap: Boolean = true

    /** True (the default, as the real hub): `Permits.grant` raises the cap. False emulates a hub that predates it. */
    @Volatile
    var permitGrants: Boolean = true

    /** Set false to emulate the P2 hub: no `instance-status` capability in the ack, reports ignored. */
    @Volatile
    var instanceStatus: Boolean = true

    /** Set false to emulate the P3 hub: no `scatter-gather` capability in the ack, `shared_query_types` ignored. */
    @Volatile
    var scatterGather: Boolean = true

    /** While true, no stream's next message is read (the message being handled when it is set still completes). */
    @Volatile
    var pauseReading: Boolean = false

    /**
     * True (the default, as the real hub): a new stream registering an instance id that already has a stream replaces
     * the older one. False keeps both.
     */
    @Volatile
    var replaceSessions: Boolean = true

    /**
     * Delays each `RegisterAck` (and the stream's later messages, read in order) by this much, as network latency would;
     * a replaced older stream still ends at once. Zero by default.
     */
    @Volatile
    var ackDelay: Duration = Duration.ZERO

    /** Requests queued per permit-managed instance before new ones are rejected (`KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE`). */
    @Volatile
    var queueCapacity: Int = 10_000

    private class Session(val out: SendChannel<ServerMessage>) {
        @Volatile var serviceName: String = ""
        @Volatile var instanceId: String = ""
        @Volatile var types: Set<String> = emptySet()
        @Volatile var commandTypes: Set<String> = emptySet()
        @Volatile var sharedTypes: Set<String> = emptySet()
        @Volatile var protocolVersion = 0
        /** Negotiated `scatter-gather` (fixed by the stream's first Register). */
        @Volatile var scatterGather = false
        @Volatile var registered = false
        /** Shares `flow-control` with this hub (fixed by the stream's first Register). */
        @Volatile var flowControl = false
        /** Negotiated `instance-status` (fixed by the stream's first Register). */
        @Volatile var instanceStatus = false
        /** The ack of the Register just handled, written by the stream's collector (after [ackDelay]). */
        var ack: ServerMessage? = null
        /** Remaining permits; null = unlimited (not permit-managed). Guarded by [lock]. */
        var permits: Int? = null
        /** Most permits the session may hold: `initial_permits`, raised by `Permits.grant`. Guarded by [lock]. */
        var cap: Int = 0
        /** Requests waiting for a permit, highest priority first, then arrival. Guarded by [lock]. */
        val queue = ArrayList<Queued>()
        lateinit var collector: Job
    }

    private class Queued(val request: Request, val seq: Long)

    /** A forwarded request: a unary one with its own deadline [timer], or one target of [gather]. */
    private class Pending(val sender: Session, val handler: Session, val timer: Job?, val gather: Gather? = null)

    /** A scatter-gather in progress; guarded by [lock]. */
    private class Gather(val id: String, val sender: Session, val maxResults: Int) {
        val remaining = LinkedHashSet<String>() // target ids that have not answered
        var seq = 0L
        var parts = 0
        var failed = 0
        var ok = 0
        lateinit var timer: Job
    }

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val lock = Any() // guards sessions, pending and rotation; never held across a suspension
    private val sessions = CopyOnWriteArrayList<Session>()
    private val pending = HashMap<String, Pending>()
    private val gathers = HashMap<String, Gather>()
    private val rotation = HashMap<String, Int>()
    private val heartbeats = ConcurrentHashMap<String, Int>()
    private val registrations = ConcurrentHashMap<String, Int>()
    private val lastRegisters = ConcurrentHashMap<String, Register>()
    private val acks = ConcurrentHashMap<String, Int>()
    private val permitsGranted = ConcurrentHashMap<String, Int>()
    private val permitMessages = ConcurrentHashMap<String, Int>()
    private val forwardedOrder = CopyOnWriteArrayList<String>()
    private val requestsBySender = ConcurrentHashMap<String, Int>()
    private val statusMessages = ConcurrentHashMap<String, Int>()
    private val lastStatuses = ConcurrentHashMap<String, InstanceStatus>()
    private val streamsOpened = ConcurrentHashMap<String, Int>()
    private val streamsReplaced = ConcurrentHashMap<String, Int>()
    private var seq = 0L
    @Volatile private var lastRequest: Request? = null
    private val channels = CopyOnWriteArrayList<ManagedChannel>()
    @Volatile private var lastHeaders: Map<String, String>? = null
    private var server: Server? = null

    fun start(): InProcessHub {
        check(server == null) { "hub already started" }
        server = InProcessServerBuilder.forName(name).addService(Service()).build().start()
        return this
    }

    fun channel(): ManagedChannel =
        InProcessChannelBuilder.forName(name).build().also { channels += it }

    fun registeredTypes(): Set<String> = sessions.flatMapTo(HashSet()) { it.types }

    fun connectedInstances(): Set<String> = sessions.filter { it.registered }.mapTo(HashSet()) { it.instanceId }

    /** Ends the stream of [instanceId] as if its process crashed; the client sees the stream fail with UNAVAILABLE. */
    fun dropInstance(instanceId: String) {
        sessions.filter { it.registered && it.instanceId == instanceId }.forEach {
            it.out.close(Status.UNAVAILABLE.withDescription("instance $instanceId dropped by test").asException())
            it.collector.cancel()
        }
    }

    /** Open streams that registered as [instanceId]. */
    fun sessionsOf(instanceId: String): Int = sessions.count { it.registered && it.instanceId == instanceId }

    /** Streams whose first `Register` named [instanceId]. */
    fun streamsOpenedBy(instanceId: String): Int = streamsOpened[instanceId] ?: 0

    /** Streams of [instanceId] ended because a newer stream registered the same instance id ([replaceSessions]). */
    fun sessionsReplacedOf(instanceId: String): Int = streamsReplaced[instanceId] ?: 0

    /** `Request` messages received from [instanceId] over all of its streams. */
    fun requestsFrom(instanceId: String): Int = requestsBySender[instanceId] ?: 0

    /** `InstanceStatus` messages received from [instanceId], kept or ignored. */
    fun statusMessagesFrom(instanceId: String): Int = statusMessages[instanceId] ?: 0

    /** The latest `InstanceStatus` kept from [instanceId] (only on streams that negotiated `instance-status`), or null. */
    fun lastStatusFrom(instanceId: String): InstanceStatus? = lastStatuses[instanceId]

    fun heartbeatsFrom(instanceId: String): Int = heartbeats[instanceId] ?: 0

    /** Accepted `Register` messages from [instanceId] over all of its streams. */
    fun registrationsFrom(instanceId: String): Int = registrations[instanceId] ?: 0

    /** The most recent accepted `Register` from [instanceId], or null. */
    fun lastRegisterFrom(instanceId: String): Register? = lastRegisters[instanceId]

    /** `RegisterAck`s sent to [instanceId] over all of its streams. */
    fun acksSentTo(instanceId: String): Int = acks[instanceId] ?: 0

    /** The most recent `Request` any client sent (as received, before routing), or null. */
    fun lastRequest(): Request? = lastRequest

    /** Sum of `Permits.count` received from [instanceId] over all of its streams. */
    fun permitsGrantedBy(instanceId: String): Int = permitsGranted[instanceId] ?: 0

    /** Number of `Permits` messages received from [instanceId] over all of its streams. */
    fun permitMessagesFrom(instanceId: String): Int = permitMessages[instanceId] ?: 0

    /** Permits [instanceId] has left on its current stream; null when it is not permit-managed (unlimited). */
    fun permitsAvailable(instanceId: String): Int? = synchronized(lock) {
        sessions.firstOrNull { it.registered && it.instanceId == instanceId }?.permits
    }

    /** Requests waiting in [instanceId]'s queue. */
    fun queuedFor(instanceId: String): Int = synchronized(lock) {
        sessions.filter { it.registered && it.instanceId == instanceId }.sumOf { it.queue.size }
    }

    /** Writes [message] to the stream of [instanceId] as if the hub had sent it (for example a part nobody waits for). */
    fun inject(instanceId: String, message: ServerMessage) {
        sessions.filter { it.registered && it.instanceId == instanceId }.forEach { it.out.trySend(message) }
    }

    /** Request ids in the order they were forwarded to handlers. */
    fun forwardedOrder(): List<String> = forwardedOrder.toList()

    /** The ack this hub sends to a v2 registrant that declared [capabilities]. */
    fun registerAck(capabilities: List<String> = emptyList()): RegisterAck = RegisterAck.newBuilder()
        .addAllCapabilities(
            listOfNotNull(
                FLOW_CONTROL.takeIf { flowControl && it in capabilities },
                INSTANCE_STATUS.takeIf { instanceStatus && it in capabilities },
                SCATTER_GATHER.takeIf { scatterGather && it in capabilities },
            ),
        )
        .setHubId(hubId)
        .setProtocolVersion(2)
        .setHeartbeatIntervalMs(heartbeatInterval.toMillis().toInt())
        .setHeartbeatTimeoutMs(heartbeatTimeout.toMillis().toInt())
        .setDefaultTimeoutMs(replyTimeout.toMillis().toInt())
        .setMaxTimeoutMs(maxTimeout.toMillis().toInt())
        .setContext("default")
        .putAllTypeTimeoutsMs(typeTimeouts.mapValues { it.value.toMillis().toInt() })
        .build()

    /** Headers of the most recently forwarded request, or null if none was forwarded yet. */
    fun lastRequestHeaders(): Map<String, String>? = lastHeaders

    override fun close() {
        scope.cancel()
        channels.forEach { it.shutdownNow() }
        server?.shutdownNow()
        server?.awaitTermination(5, TimeUnit.SECONDS)
        channels.forEach { it.awaitTermination(5, TimeUnit.SECONDS) }
    }

    private inner class Service : HubGrpcKt.HubCoroutineImplBase() {
        override fun connect(requests: Flow<ClientMessage>): Flow<ServerMessage> = channelFlow {
            val session = Session(channel)
            sessions += session
            session.collector = launch {
                try {
                    var first = true
                    requests.collect { msg ->
                        if (first && !msg.hasRegister()) {
                            throw Status.FAILED_PRECONDITION.withDescription("first message must be Register").asException()
                        }
                        first = false
                        while (pauseReading) delay(5)
                        handle(session, msg)
                        session.ack?.let { ack ->
                            session.ack = null
                            if (!ackDelay.isZero) delay(ackDelay.toMillis())
                            session.out.trySend(ack)
                        }
                    }
                } finally {
                    cleanup(session)
                }
            }
        }.buffer(Channel.UNLIMITED)
    }

    private fun handle(session: Session, msg: ClientMessage) {
        when {
            msg.hasRegister() -> register(session, msg.register)
            msg.hasRequest() -> forward(session, msg.request)
            msg.hasReply() -> relay(session, msg)
            msg.hasPermits() -> grant(session, msg.permits.count, msg.permits.grant)
            msg.hasStatus() -> {
                statusMessages.merge(session.instanceId, 1, Int::plus)
                if (session.instanceStatus) lastStatuses[session.instanceId] = msg.status
            }
            msg.hasHeartbeat() -> {
                heartbeats.merge(session.instanceId, 1, Int::plus)
                if (echoHeartbeats) session.out.trySend(
                    ServerMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtMs(msg.heartbeat.sentAtMs)).build(),
                )
            }
        }
    }

    private fun register(session: Session, reg: Register) {
        if (reg.serviceName.isEmpty() || reg.instanceId.isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("service_name and instance_id must not be empty").asException()
        }
        val exclusive = (reg.commandTypesList + reg.queryTypesList).toSet()
        // A P3 hub predates shared_query_types: the field is ignored (the types are simply not registered).
        val shared = if (scatterGather && !legacy) reg.sharedQueryTypesList.toSet() else emptySet()
        shared.firstOrNull { it in exclusive }?.let { type ->
            throw Status.INVALID_ARGUMENT.withDescription("type $type is listed as shared and as exclusive").asException()
        }
        val types = exclusive + shared
        val replaced = synchronized(lock) {
            for (type in exclusive) {
                val owner = sessions.firstOrNull { it !== session && type in it.types && it.serviceName != reg.serviceName }
                if (owner != null) {
                    throw Status.ALREADY_EXISTS.withDescription("type $type is owned by service ${owner.serviceName}").asException()
                }
                val sharedOwner = sessions.firstOrNull { it !== session && type in it.sharedTypes }
                if (sharedOwner != null) {
                    throw Status.ALREADY_EXISTS.withDescription("type $type is shared by service ${sharedOwner.serviceName}").asException()
                }
            }
            for (type in shared) {
                val owner = sessions.firstOrNull { it !== session && type in it.types && type !in it.sharedTypes }
                if (owner != null) {
                    throw Status.ALREADY_EXISTS.withDescription("type $type is owned exclusively by service ${owner.serviceName}").asException()
                }
            }
            val first = !session.registered
            // Like the real hub: one session per instance id, the newer stream wins and the older one stops being routed to.
            val older = if (first && replaceSessions) {
                sessions.filter { it !== session && it.registered && it.instanceId == reg.instanceId }.onEach { it.registered = false }
            } else {
                emptyList()
            }
            if (first) { // like the real hub: the first Register of a stream fixes flow control
                session.flowControl = !legacy && flowControl && reg.protocolVersion >= 2 && FLOW_CONTROL in reg.capabilitiesList
                session.permits = if (session.flowControl && reg.initialPermits > 0) reg.initialPermits else null
                session.cap = reg.initialPermits
                session.instanceStatus = !legacy && instanceStatus && reg.protocolVersion >= 2 && INSTANCE_STATUS in reg.capabilitiesList
                session.scatterGather = !legacy && scatterGather && reg.protocolVersion >= 2 && SCATTER_GATHER in reg.capabilitiesList
                session.protocolVersion = if (legacy) 0 else reg.protocolVersion
            }
            session.serviceName = reg.serviceName
            session.instanceId = reg.instanceId
            session.registered = true
            session.types = types
            session.commandTypes = reg.commandTypesList.toSet()
            session.sharedTypes = shared
            if (first) streamsOpened.merge(reg.instanceId, 1, Int::plus)
            older
        }
        // The older stream ends (its in-flight requests fail in its cleanup) before this one is acknowledged.
        for (old in replaced) {
            streamsReplaced.merge(reg.instanceId, 1, Int::plus)
            old.out.close()
            old.collector.cancel()
        }
        registrations.merge(reg.instanceId, 1, Int::plus)
        lastRegisters[reg.instanceId] = reg
        if (!legacy && reg.protocolVersion >= 2) {
            // Written before this stream's next message is handled, so it precedes the echo of the following heartbeat.
            session.ack = ServerMessage.newBuilder().setRegisterAck(registerAck(reg.capabilitiesList)).build()
            acks.merge(reg.instanceId, 1, Int::plus)
        }
    }

    private fun forward(sender: Session, req: Request) {
        lastRequest = req
        requestsBySender.merge(sender.instanceId, 1, Int::plus)
        if (req.id.isEmpty() || req.type.isEmpty()) {
            sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request id and type must not be empty")
            return
        }
        if (req.mode != RequestMode.UNARY) {
            // A v1 session cannot know UNSUPPORTED: it gets INVALID_REQUEST, like from the real hub.
            val refusal = if (sender.protocolVersion >= 2) ErrorCode.UNSUPPORTED else ErrorCode.INVALID_REQUEST
            if (req.mode != RequestMode.SCATTER_GATHER) {
                sender.fail(req.id, refusal, "request mode ${req.mode} is not supported")
            } else if (!sender.scatterGather) {
                sender.fail(req.id, refusal, "scatter-gather needs the $SCATTER_GATHER capability")
            } else {
                scatter(sender, req)
            }
            return
        }
        synchronized(lock) {
            if (req.id in pending || req.id in gathers) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request ${req.id} is already in flight")
                return
            }
            val candidates = sessions.filter { it.registered && req.type in it.types }
            if (candidates.isEmpty()) {
                sender.fail(req.id, ErrorCode.NO_HANDLER, "no handler registered for ${req.type}")
                return
            }
            val index = rotation.merge(req.type, 1, Int::plus)!! - 1
            val handler = candidates[index % candidates.size]
            val timeout = effectiveTimeout(req)
            val timer = scope.launch {
                delay(timeout.toMillis())
                if (!emitTimeouts) return@launch
                val expired = synchronized(lock) {
                    handler.queue.removeIf { it.request.id == req.id } // a queued request keeps its own deadline
                    pending.remove(req.id)
                }
                expired?.sender?.fail(req.id, ErrorCode.TIMEOUT, "no reply for ${req.type} within ${timeout.toMillis()} ms")
            }
            val permits = handler.permits
            if (permits != null && permits <= 0) {
                if (handler.queue.size >= queueCapacity) {
                    timer.cancel()
                    if (sender.flowControl) sender.fail(req.id, ErrorCode.RESOURCE_EXHAUSTED, "queue of ${handler.instanceId} is full")
                    else sender.fail(req.id, ErrorCode.HANDLER_ERROR, "handler busy")
                    return
                }
                pending[req.id] = Pending(sender, handler, timer)
                lastHeaders = req.headersMap.toMap()
                handler.queue += Queued(req, seq++)
                handler.queue.sortWith(compareByDescending<Queued> { it.request.priority }.thenBy { it.seq })
                return
            }
            pending[req.id] = Pending(sender, handler, timer)
            lastHeaders = req.headersMap.toMap()
            deliver(handler, req)
        }
    }

    private fun effectiveTimeout(req: Request): Duration {
        val override = typeTimeouts[req.type]
        return if (req.timeoutMs > 0) minOf(Duration.ofMillis(req.timeoutMs.toLong()), override ?: maxTimeout) else override ?: replyTimeout
    }

    private fun scatter(sender: Session, req: Request) {
        synchronized(lock) {
            if (req.id in pending || req.id in gathers) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request ${req.id} is already in flight")
                return
            }
            val owners = sessions.filter { it.registered && req.type in it.types }
            if (req.kind == RequestKind.COMMAND || owners.any { req.type in it.commandTypes }) {
                sender.fail(req.id, ErrorCode.INVALID_REQUEST, "scatter-gather is for queries only")
                return
            }
            if (owners.isEmpty()) {
                sender.fail(req.id, ErrorCode.NO_HANDLER, "no handler registered for ${req.type}")
                return
            }
            lastHeaders = req.headersMap.toMap()
            val gather = Gather(req.id, sender, req.maxResults)
            gathers[req.id] = gather
            val timeout = effectiveTimeout(req)
            gather.timer = scope.launch {
                delay(timeout.toMillis())
                if (!emitTimeouts) return@launch
                synchronized(lock) { closeGather(gather, timedOut = gather.remaining.size) }
            }
            val targets = owners.sortedWith(compareBy({ it.serviceName }, { it.instanceId }))
            targets.forEachIndexed { n, target -> gather.remaining += "${req.id}#$n" }
            targets.forEachIndexed { n, target ->
                val targetRequest = req.toBuilder().setId("${req.id}#$n").setMode(RequestMode.UNARY).setMaxResults(0).build()
                if (gathers[req.id] !== gather) return // every target so far failed and ended the gather
                pending[targetRequest.id] = Pending(sender, target, timer = null, gather = gather)
                val permits = target.permits
                when {
                    permits == null || permits > 0 -> deliver(target, targetRequest)
                    target.queue.size >= queueCapacity -> {
                        pending.remove(targetRequest.id)
                        val code = if (sender.flowControl) ErrorCode.RESOURCE_EXHAUSTED else ErrorCode.HANDLER_ERROR
                        val text = if (sender.flowControl) "queue of ${target.instanceId} is full" else "handler busy"
                        answered(gather, targetRequest.id, target, null, Error.newBuilder().setCode(code).setMessage(text).build())
                    }
                    else -> {
                        target.queue += Queued(targetRequest, seq++)
                        target.queue.sortWith(compareByDescending<Queued> { it.request.priority }.thenBy { it.seq })
                    }
                }
            }
        }
    }

    /**
     * Target [targetId] of [gather] answered (or failed on the hub's side): one `ReplyPart` to the sender, and the
     * `ReplyEnd` when it was the last one needed. Caller holds [lock].
     */
    private fun answered(gather: Gather, targetId: String, target: Session, reply: Reply?, error: Error?) {
        if (gathers[gather.id] !== gather || !gather.remaining.remove(targetId)) return // the gather already ended
        val part = ReplyPart.newBuilder()
            .setRequestId(gather.id)
            .setResponderService(target.serviceName)
            .setResponderInstance(target.instanceId)
            .setSeq(gather.seq++)
        val failure = error ?: reply?.takeIf { it.hasError() }?.error
        if (failure != null) {
            part.setError(failure.toBuilder().setRequestId(gather.id))
            gather.failed++
        } else {
            part.setPayload(reply!!.payload)
            gather.ok++
        }
        gather.parts++
        gather.sender.out.trySend(ServerMessage.newBuilder().setPart(part).build())
        if (gather.remaining.isEmpty() || (gather.maxResults > 0 && gather.ok >= gather.maxResults)) closeGather(gather, timedOut = 0)
    }

    /** Ends [gather]: its unanswered targets leave their queues and the map, then its `ReplyEnd`. Caller holds [lock]. */
    private fun closeGather(gather: Gather, timedOut: Int) {
        if (gathers.remove(gather.id) !== gather) return
        gather.timer.cancel()
        for (targetId in gather.remaining) {
            val entry = pending.remove(targetId) ?: continue
            entry.handler.queue.removeIf { it.request.id == targetId }
        }
        gather.remaining.clear()
        val end = ReplyEnd.newBuilder().setRequestId(gather.id).setParts(gather.parts).setFailed(gather.failed).setTimedOut(timedOut)
        gather.sender.out.trySend(ServerMessage.newBuilder().setEnd(end).build())
    }

    /** Forwards [req] to [handler], spending a permit when it is permit-managed. Caller holds [lock]. */
    private fun deliver(handler: Session, req: Request) {
        handler.permits = handler.permits?.minus(1)
        forwardedOrder += req.id
        handler.out.trySend(ServerMessage.newBuilder().setRequest(req).build())
    }

    private fun grant(session: Session, count: Int, grant: Int) {
        permitMessages.merge(session.instanceId, 1, Int::plus)
        permitsGranted.merge(session.instanceId, count, Int::plus)
        synchronized(lock) {
            val permits = session.permits ?: return // not permit-managed: ignored, like the real hub
            if (permitGrants) session.cap = maxOf(session.cap, grant)
            session.permits = if (permitCap) minOf(permits + count, session.cap) else permits + count
            while ((session.permits ?: 0) > 0 && session.queue.isNotEmpty()) {
                val next = session.queue.removeAt(0)
                if (next.request.id in pending) deliver(session, next.request) // skip entries that already timed out
            }
        }
    }

    private fun relay(handler: Session, msg: ClientMessage) {
        val reply = msg.reply
        val entry = synchronized(lock) {
            val p = pending[reply.requestId]
            val taken = if (p != null && p.handler === handler) pending.remove(reply.requestId) else null
            taken?.gather?.let { gather -> answered(gather, reply.requestId, handler, reply, null) }
            taken
        } ?: return // late (after TIMEOUT or the gather's end) or unknown reply: dropped
        if (entry.gather != null) return
        entry.timer?.cancel()
        entry.sender.out.trySend(ServerMessage.newBuilder().setReply(reply).build())
    }

    private fun cleanup(session: Session) {
        val failed = synchronized(lock) {
            sessions.remove(session)
            session.queue.clear() // its queued requests are pending with this handler: failed below
            // Gathers of this sender end silently (nobody is left to tell); their targets' answers will be dropped.
            gathers.values.filter { it.sender === session }.forEach { gather ->
                gathers.remove(gather.id)
                gather.timer.cancel()
                gather.remaining.forEach { pending.remove(it) }
            }
            val mine = pending.filterValues { it.handler === session || it.sender === session }
            mine.keys.forEach { pending.remove(it) }
            // A target of another sender's gather fails as a part.
            mine.forEach { (id, p) ->
                p.gather?.let { answered(it, id, session, null, Error.newBuilder().setCode(ErrorCode.HANDLER_DISCONNECTED).setMessage("handler ${session.instanceId} disconnected").build()) }
            }
            mine.filterValues { it.gather == null }
        }
        failed.forEach { (id, p) ->
            p.timer?.cancel()
            if (p.handler === session && p.sender !== session) {
                p.sender.fail(id, ErrorCode.HANDLER_DISCONNECTED, "handler ${session.instanceId} disconnected")
            }
        }
    }

    private companion object {
        const val FLOW_CONTROL = "flow-control"
        const val INSTANCE_STATUS = "instance-status"
        const val SCATTER_GATHER = "scatter-gather"
    }

    private fun Session.fail(requestId: String, code: ErrorCode, message: String) {
        out.trySend(
            ServerMessage.newBuilder()
                .setError(Error.newBuilder().setRequestId(requestId).setCode(code).setMessage(message)).build(),
        )
    }
}
