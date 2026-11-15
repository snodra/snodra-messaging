package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.ProcessorStatus
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.ReplyPart
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.ServerMessage
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.HubUnavailableException
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.merge
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ThreadLocalRandom
import java.util.concurrent.atomic.AtomicInteger

/**
 * This instance's connections to every hub (multi-hub HA, ADR 0016): one [HubConnection] (one `Connect` stream, with its
 * own handshake, heartbeat and reconnect backoff) per hub target, all sending the identical
 * registration. With a single target it behaves like that one connection.
 *
 * - **Targets**: [HubTargets.Static] connects to each listed target; [HubTargets.Dns] resolves the name every
 *   [dnsRefresh] (one address family, [HubTargets.pickAddresses]), connects to new addresses, and closes a vanished one
 *   once idle (it first announces an empty registration, then waits up to [dnsRefresh] for its requests and handlers).
 *   An address resolved again while its connection is still draining revives that connection (its registration is
 *   announced again) instead of opening a second stream. A failed or empty resolution keeps the current connections.
 * - **Dedupe**: a hub is its `RegisterAck.hub_id`. The first target to acknowledge an id
 *   owns it for as long as that target is kept, live or reconnecting: the hub keeps one session per instance id, so a
 *   second target of the same hub (an alias) replaces the owner's session with its `Register`, and the owner's stream
 *   ends before the alias's ack arrives. A target that acknowledges an owned id is therefore closed and parked
 *   (logged once), never made the owner; the owner reconnects. A parked target is retried only once its owner is gone
 *   (no longer resolved, or answering another id); with DNS it is dropped when no longer resolved.
 * - **Live hubs**: the kept connections whose stream completed its handshake ([liveHubIds]).
 * - **Routing**: a request with a routing key goes to the live hub chosen by [Rendezvous] over (key, hub id); any other
 *   round-robin over the live hubs. Replies to incoming requests go on the stream they arrived on ([HubConnection]).
 * - **Failover**: a request that its hub could not take before writing it (the stream closed, or broke with the
 *   request still queued) goes to another live hub; when none is left the call waits for the set of live hubs to change.
 *   The whole call has one deadline: the request's `timeout_ms` when set (the caller's `SendOptions.timeout` or
 *   `request-timeout`), else `requestTimeout`, else 5 s. Waiting ends there with [HubUnavailableException], and a request
 *   sent again carries what is left of its `timeout_ms`. A request already written fails with
 *   `HubConnectionLostException`.
 * - **Scatter-gather**: one gather goes to one live hub, round-robin like an unkeyed request. A gather not yet written
 *   fails over like a request; once written, a broken stream fails it with `HubConnectionLostException` (the parts
 *   already received are not asked again).
 * - **Streaming** (P4b): a streaming query goes to one live hub, chosen and failed over exactly like a gather. Its
 *   `timeout_ms` is the idle timeout and is sent whole, never reduced by a hub wait or a failover (the deadline of the
 *   call bounds only those); a collector exception (`StreamCollectorException`) is never a reason to fail over.
 * - **Subscriptions** (P4c): a subscription goes to one live hub, chosen and failed over like a gather. `timeout_ms`
 *   bounds the initial request (as for a unary request: what is left of the call's deadline after a hub wait is sent). Instances
 *   serve subscriptions from every hub they are connected to through one shared [SubscriptionRegistry].
 * - **Permits**: a stream announces `ceil(maxConcurrentHandlers / N)` initial permits, N = 1 (itself) + the other kept
 *   hubs that are live or still on their first connect attempt. Whenever that set changes, every live hub's stream is
 *   topped up to `ceil(maxConcurrentHandlers / N)` with N = the kept hubs that are live or on their first attempt:
 *   `Permits{count = max(0, ceil(max / N) - offered), grant = ceil(max / N)}`, offered = what the stream announced plus
 *   its earlier top-ups; the grant raises the hub's permit cap on the session (otherwise `initial_permits`). So the
 *   survivors of an outage (or of a hub that failed its first attempt) serve at full capacity, flaps never add up, a
 *   hub that joins or returns gets `ceil(max / N)` on its new stream, and nothing is ever revoked (rule shared with
 *   the Rust library). One [HandlerGate] keeps the local bound at [maxConcurrentHandlers] over all hubs.
 * - **Client certificate rotation** (P5c, ADR 0020): with [HubSettings.clientCertificate], every [certificateCheckInterval] the files are
 *   read ([HubClientCertificate.refresh]); when a renewed, valid pair was adopted, the instance first waits a random delay (uniform in
 *   [0, min([rolloverMaxDelay], 10 % of what is left of the pair its streams present)], once per pair, so replicas that see the same renewed
 *   file do not all deregister at once), reads the files again, and rolls the connections whose stream presents an older pair over
 *   **one hub at a time** ([HubConnection.rollOver]): the hub is told this instance is stopping, the work in flight on its stream is awaited
 *   ([rolloverDrainTimeout]; the streams and subscriptions this instance itself consumes are not: they have no end of their own and are cut by
 *   the roll), the stream is closed gracefully and a new one registers with the new pair ([rolloverReconnectTimeout]); only then the next hub.
 *   A hub with another live hub leaves routing at once; the only hub leaves it after its work in flight finished (from then until its new stream
 *   registers, up to the drain plus the reconnect, a sender of this instance waits within its call's deadline and other services get
 *   `NO_HANDLER`). Open subscriptions of the rolled stream end (the hub tells their subscribers `HANDLER_DISCONNECTED`). A roll that does not
 *   complete stops the rollover for that pair (one ERROR); the remaining hubs keep the old pair until the files change again. An unexpected
 *   exception in a check is logged once and the next tick checks again.
 * - **Status**: every [statusInterval], and whenever the live set changes, each live hub gets an `InstanceStatus` with the
 *   ids of the live hubs, the handlers in flight and the event processors ([processors], P6a).
 */
class HubConnections(
    private val targets: HubTargets,
    private val channelFactory: HubChannelFactory,
    private val serviceName: String,
    val instanceId: String,
    private val settings: HubSettings,
    private val inbound: suspend (Request) -> Reply,
    private val scope: CoroutineScope,
    private val handlerDispatcher: CoroutineDispatcher = Dispatchers.IO,
    private val maxConcurrentHandlers: Int = 256,
    private val loadFactor: Int = 100,
    private val tags: Map<String, String> = emptyMap(),
    private val statusInterval: Duration = HubProtocol.STATUS_INTERVAL,
    private val dnsRefresh: Duration = HubProtocol.DNS_REFRESH,
    private val resolver: suspend (String) -> List<InetAddress> = { host ->
        withContext(Dispatchers.IO) { InetAddress.getAllByName(host).toList() }
    },
    /** Serves `mode = STREAMING` requests (P4b); see [HubConnection]. */
    private val inboundStream: StreamingInbound = NO_STREAMING_HANDLERS,
    /** The subscriptions this instance serves over every hub (P4c); see [HubConnection]. */
    private val subscriptions: SubscriptionRegistry = SubscriptionRegistry(),
    /** Admits `mode = SUBSCRIPTION` requests (P4c); see [HubConnection]. */
    private val inboundSubscription: SubscriptionInbound = NO_SUBSCRIPTION_HANDLERS,
    /** How often the client certificate files are read for a renewed pair (P5c): the hub's heartbeat period, the tick of the other file checks. */
    private val certificateCheckInterval: Duration = HubProtocol.CERTIFICATE_CHECK_INTERVAL,
    /** The longest a rollover waits, per hub, for the work in flight on the old stream before it closes it anyway (P5c). */
    private val rolloverDrainTimeout: Duration = HubProtocol.ROLLOVER_DRAIN_TIMEOUT,
    /** The longest a rollover waits, per hub, for the new stream to register before it leaves the other hubs on the old pair until the next tick (P5c). */
    private val rolloverReconnectTimeout: Duration = HubProtocol.ROLLOVER_RECONNECT_TIMEOUT,
    /**
     * The longest this instance waits after it first sees a renewed pair before it starts rolling the hubs onto it (P5c): the wait is uniform
     * in [0, min([rolloverMaxDelay], 10 % of the remaining validity of the pair the streams present)], so replicas that see the same renewed
     * file within one tick do not all deregister at once. Zero for no wait (tests).
     */
    private val rolloverMaxDelay: Duration = HubProtocol.ROLLOVER_MAX_DELAY,
    /** The random source of the wait before a rollover: a value in [0, 1). Replaced in tests. */
    private val rolloverRandom: () -> Double = { ThreadLocalRandom.current().nextDouble() },
    /**
     * The status of this instance's event processors (P6a, ADR 0022), read once per report and sent to every live hub. Must
     * return at once from in-memory state; one that throws sends the report without processors.
     */
    private val processors: () -> List<ProcessorStatus> = { emptyList() },
    /** Serves the `ControlRequest`s of every hub (P6b, ADR 0023); see [HubConnection]. */
    private val control: ControlInbound = NO_PROCESSOR_CONTROL,
) : HubTransport {
    private val log = LoggerFactory.getLogger(HubConnections::class.java)

    private val gate = HandlerGate(maxConcurrentHandlers)

    init {
        require(!statusInterval.isZero && !statusInterval.isNegative) { "statusInterval must be positive, was $statusInterval" }
    }

    private class Member(val connection: HubConnection, val watcher: Job)

    private val lock = Any() // guards everything below except the flows
    private val active = LinkedHashMap<String, Member>() // target -> member, in configuration or resolution order
    private val draining = mutableListOf<Member>()
    private val owners = HashMap<String, HubConnection>() // hub id -> the kept connection that answered it first
    private val parked = HashMap<String, String>() // target closed as a duplicate -> the hub id it answered
    private var resolvedTargets: List<String> = emptyList() // latest DNS answer (DNS targets only)
    private class Types(val commands: Set<String>, val queries: Set<String>, val shared: Set<String>)
    private var types: Types? = null
    private var started = false
    private var stopped = false
    private val jobs = mutableListOf<Job>()

    private val live = MutableStateFlow<List<HubConnection>>(emptyList())
    private val anyLive = MutableStateFlow(false)
    private val roundRobin = AtomicInteger()

    /** True while at least one hub is live. */
    val connected: StateFlow<Boolean> = anyLive.asStateFlow()

    private val latest = MutableStateFlow<HubRejection?>(null)

    /**
     * The newest refusal of this instance's registration by any hub (`ALREADY_EXISTS`, `INVALID_ARGUMENT`, `UNAUTHENTICATED`,
     * `PERMISSION_DENIED`): set on every refusal, replaced only by a newer one, never cleared by a later acceptance (nor when its
     * hub leaves the targets); null while no hub ever refused. A failure to get an access token is not a hub's refusal and is not
     * kept here ([awaitRegistered] and [authStatus] report it).
     */
    val latestRejection: StateFlow<HubRejection?> = latest.asStateFlow()

    private fun keepNewer(rejection: HubRejection) =
        latest.update { current -> if (current == null || !rejection.at.isBefore(current.at)) rejection else current }

    /** Incoming requests of every hub currently being handled (or waiting for a handler permit). */
    val inFlightHandlers: StateFlow<Int> get() = gate.inFlightHandlers

    /** Test hook: runs after a hub is chosen for a request and before the request is handed to it. */
    internal var beforeWrite: (suspend (HubConnection) -> Unit)? = null

    /** Ids of the live hubs, in target order. */
    fun liveHubIds(): List<String> = live.value.mapNotNull { it.hubId }

    /**
     * How this instance authenticates to the hubs (P5a/P5b) and the health of its credential: the kind, a JWT's expiry, the last
     * token fetch error, fetch counts by result and refreshes sent. Read-only and cheap; safe to call from a health indicator.
     */
    fun authStatus(): HubAuthStatus {
        val provider = settings.tokenProvider
        val certificate = settings.clientCertificate
        val kind = when {
            certificate != null -> HubAuthKind.CERTIFICATE
            provider != null -> HubAuthKind.JWT
            settings.token.current() != null -> HubAuthKind.TOKEN
            else -> HubAuthKind.NONE
        }
        return HubAuthStatus(
            kind = kind,
            tokenExpiresAt = provider?.usable()?.expiresAt,
            lastTokenError = provider?.lastError?.value,
            tokenFetches = provider?.let { p -> TokenFetchResult.entries.associate { it.label to p.fetches(it) } }.orEmpty(),
            credentialsRefreshesSent = refreshesSent.get(),
            certificateFingerprint = certificate?.current?.fingerprint,
            certificateService = certificate?.current?.service,
            certificateExpiresAt = certificate?.current?.notAfter,
            certificateRotations = certificate?.rotations ?: 0,
        )
    }

    /**
     * `ClientMessage.credentials` written since this client started, over every connection it ever had. One shared counter, not a sum
     * over the live connections: a connection that goes away must not make the Prometheus counter drop.
     */
    private val refreshesSent = java.util.concurrent.atomic.AtomicLong()

    internal fun credentialsRefreshesSent(): Double = refreshesSent.get().toDouble()

    /** The kept connections (live or reconnecting), in target order; duplicates and drained targets excluded. */
    fun connections(): List<HubConnection> = synchronized(lock) { active.values.map { it.connection } }

    /** Connects to the configured hubs (resolving a DNS target first) and starts the status reports. */
    fun start() {
        synchronized(lock) {
            check(!started) { "HubConnections already started" }
            started = true
            when (targets) {
                is HubTargets.Static -> Unit
                is HubTargets.Dns -> jobs += scope.launch { resolveLoop(targets) }
            }
            jobs += scope.launch {
                while (true) {
                    delay(statusInterval.toMillis())
                    reportStatus()
                }
            }
            settings.clientCertificate?.let { certificate -> jobs += scope.launch { certificateLoop(certificate) } }
        }
        if (targets is HubTargets.Static) add(targets.targets)
    }

    /**
     * Replaces the announced handler types (see [HubConnection.register]) on every hub; re-sent on every reconnect and to
     * hubs added later.
     */
    fun register(commandTypes: Set<String>, queryTypes: Set<String>, sharedQueryTypes: Set<String> = emptySet()) {
        val connections = synchronized(lock) {
            types = Types(commandTypes, queryTypes, sharedQueryTypes)
            active.values.map { it.connection }
        }
        connections.forEach { it.register(commandTypes, queryTypes, sharedQueryTypes) }
    }

    /**
     * True once **every** kept hub connection confirmed the most recent [register] call, false if [timeout] passes first.
     * One hub being enough to serve is not enough here: while a configured hub is down this returns false at [timeout]
     * although the instance is already reachable through the others ([connected], [liveHubIds] tell that). Parked
     * duplicates and drained DNS targets are not kept, so they are not waited for.
     *
     * @throws HubRegistrationRejectedException if a hub rejects the registration while waiting
     */
    suspend fun awaitRegistered(timeout: Duration): Boolean = withTimeoutOrNull(timeout.toMillis()) {
        while (!allRegistered()) delay(20)
        true
    } ?: false

    private fun allRegistered(): Boolean {
        val connections = connections()
        val waiting = connections.filterNot { it.registered }
        for (c in waiting) {
            val status = c.lastRejection.value ?: continue
            throw HubRegistrationRejectedException(
                if (status.cause is HubTokenException) "no access token for $serviceName ($instanceId), not connecting to ${c.target}: ${status.cause?.message}"
                else "hub ${c.target} rejected the registration of $serviceName ($instanceId): " +
                    status.code.name + (status.description?.let { " ($it)" } ?: ""),
            )
        }
        return connections.isNotEmpty() && waiting.isEmpty()
    }

    /** True once no incoming request of any hub is being handled, false if [timeout] passes first. */
    suspend fun awaitHandlersIdle(timeout: Duration): Boolean =
        withTimeoutOrNull(timeout.toMillis()) { gate.inFlightHandlers.first { it == 0 } } != null

    /**
     * Sends [request] through one live hub (see the class comment for the choice and the failover) and returns its
     * `Reply` or `Error`, with [HubConnection.send]'s exceptions.
     */
    override suspend fun send(request: Request): ServerMessage =
        route(request, declined = { it.hasError() && it.error.code == ErrorCode.NO_HANDLER }) { connection, outgoing ->
            connection.send(outgoing, awaitStream = false)
        }

    /**
     * Sends [request] as a scatter-gather through one live hub (see the class comment), with [HubConnection.gather]'s parts,
     * result and exceptions.
     */
    override suspend fun gather(request: Request, onPart: suspend (ReplyPart) -> Unit): ServerMessage =
        route(request) { connection, outgoing ->
            connection.gather(outgoing, awaitStream = false, onPart)
        }

    /**
     * Sends [request] as a streaming query through one live hub (see the class comment), with [HubConnection.stream]'s
     * parts, result and exceptions.
     */
    override suspend fun stream(request: Request, onPart: suspend (ReplyPart) -> Unit): ServerMessage =
        route(request, keepTimeout = true) { connection, outgoing ->
            connection.stream(outgoing, awaitStream = false, onPart)
        }

    /**
     * Sends [request] as a subscription query through one live hub (see the class comment), with [HubConnection.subscribe]'s
     * result and exceptions. `timeout_ms` bounds the initial request: it is what is left of the call's deadline after a hub
     * wait or a hub that did not take the request, as for [send].
     */
    override suspend fun subscribe(request: Request): SubscribeResult =
        route(request) { connection, outgoing ->
            connection.subscribe(outgoing, awaitStream = false)
        }

    /**
     * The hub choice and failover of [send], [gather], [stream] and [subscribe]. A positive `timeout_ms` is one deadline for the whole call: a hub wait or a hub that did not take the
     * request uses some of it and the request is sent with what is left, unless [keepTimeout] (a stream: `timeout_ms`
     * is its idle timeout, sent whole; the deadline bounds only the hub wait and the retries). A hub whose answer is [declined]
     * (`NO_HANDLER` for a unary request) is skipped like one that could not take the request; when every live hub declined, the
     * last answer is returned at once.
     */
    private suspend fun <T> route(
        request: Request,
        keepTimeout: Boolean = false,
        declined: (T) -> Boolean = { false },
        call: suspend (HubConnection, Request) -> T,
    ): T {
        // One deadline for the whole call: the caller's timeout (timeout_ms), else request-timeout, else 5 s.
        val wait = if (request.timeoutMs > 0) Duration.ofMillis(request.timeoutMs.toLong())
        else settings.chosenRequestTimeout ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT
        val deadline = System.nanoTime() + wait.toNanos()
        val tried = HashSet<HubConnection>()
        var waited = false
        var lastDeclined: T? = null
        while (true) {
            val hubs = live.value
            val connection = choose(request, hubs.filter { it !in tried })
            // Every live hub answered that it has no handler (see [declined]): that is the answer, not a reason to wait for a hub to appear.
            if (connection == null && lastDeclined != null) return lastDeclined
            val leftMs = (deadline - System.nanoTime()) / 1_000_000
            if (connection == null || leftMs <= 0) {
                val changed = connection == null && leftMs > 0 && withTimeoutOrNull(leftMs) { live.first { it != hubs } } != null
                if (!changed) {
                    throw HubUnavailableException(
                        if (tried.isEmpty()) "no hub connected within $wait" else "no hub took the request within $wait",
                    )
                }
                tried.clear() // the live hubs changed: a hub that failed before may have a new stream
                waited = true
                continue
            }
            beforeWrite?.invoke(connection)
            // A caller's timeout is what is left of it once a wait or a hub that did not take the request used some.
            val outgoing = if (!keepTimeout && request.timeoutMs > 0 && (tried.isNotEmpty() || waited)) {
                val remaining = ((deadline - System.nanoTime()) / 1_000_000).coerceIn(1, request.timeoutMs.toLong())
                request.toBuilder().setTimeoutMs(remaining.toInt()).build()
            } else {
                request
            }
            try {
                val result = call(connection, outgoing)
                if (!declined(result)) return result
                // This hub has no owner of the type right now (the owner is connected to another hub only, or is being rolled over onto a
                // renewed client certificate here); nothing was delivered, so another hub may still have one.
                log.debug("Hub {} has no handler for request {}; trying another hub", connection.target, request.id)
                lastDeclined = result
                tried += connection
            } catch (e: HubUnavailableException) {
                // Only the hub not taking the request: a collector's HubUnavailableException arrives wrapped in a
                // StreamCollectorException (the request was written; it is never re-sent).

                log.debug("Request {} not taken by hub {}: {}; trying another hub", request.id, connection.target, e.message)
                tried += connection
            }
        }
    }

    /** The deadline rules of the first live hub (the hubs of a deployment are configured alike). */
    override fun hubTimeoutFor(type: String): Duration =
        live.value.firstOrNull()?.hubTimeoutFor(type) ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT

    /** The longest deadline any live hub may give a request (a request goes to one of them); see [HubConnection.hubDeadlineBound]. */
    override fun hubDeadlineBound(type: String, timeoutMs: Int): Duration =
        live.value.maxOfOrNull { it.hubDeadlineBound(type, timeoutMs) }
            ?: Duration.ofMillis(HubProtocol.hubDeadlineBoundMs(timeoutMs, type, io.damoreira.kraken.hub.v1.RegisterAck.getDefaultInstance()))

    /** Closes every stream, stops reconnecting and resolving, and fails every request in flight. */
    suspend fun stop() {
        val members = synchronized(lock) {
            stopped = true
            jobs.forEach { it.cancel() }
            val all = active.values.toList() + draining
            active.clear()
            draining.clear()
            refreshLive()
            all
        }
        for (member in members) {
            member.watcher.cancel()
            member.connection.stop()
        }
    }

    private fun choose(request: Request, candidates: List<HubConnection>): HubConnection? = when {
        candidates.isEmpty() -> null
        request.routingKey.isNotEmpty() -> Rendezvous.choose(request.routingKey, candidates) { it.hubId ?: it.target }
        else -> candidates[Math.floorMod(roundRobin.getAndIncrement(), candidates.size)]
    }

    private fun add(newTargets: List<String>) {
        var revived: List<HubConnection> = emptyList()
        val added = synchronized(lock) {
            if (stopped) return
            // A target still draining is revived, never dialed again: a second stream with this instance id would make
            // the hub end the draining one, failing the requests it still serves.
            revived = newTargets.filter { it !in active && it !in parked }.mapNotNull { target ->
                draining.firstOrNull { it.connection.target == target }?.also { member ->
                    draining.remove(member)
                    active[target] = member
                }?.connection
            }
            newTargets.filter { it !in active && it !in parked }.map { target ->
                lateinit var self: HubConnection
                val connection = HubConnection(
                    channelFactory = { channelFactory.channel(target) },
                    serviceName = serviceName,
                    instanceId = instanceId,
                    settings = settings,
                    inbound = inbound,
                    scope = scope,
                    handlerDispatcher = handlerDispatcher,
                    maxConcurrentHandlers = maxConcurrentHandlers,
                    loadFactor = loadFactor,
                    tags = tags,
                    target = target,
                    gate = gate,
                    initialPermits = { HubProtocol.initialPermits(maxConcurrentHandlers, hubsFor(self)) },
                    inboundStream = inboundStream,
                    subscriptions = subscriptions,
                    inboundSubscription = inboundSubscription,
                    onCredentialsRefreshSent = { refreshesSent.incrementAndGet() },
                    onRejected = ::keepNewer,
                    control = control,
                )
                self = connection
                types?.let { connection.register(it.commands, it.queries, it.shared) }
                val watcher = scope.launch(start = CoroutineStart.LAZY) {
                    merge(connection.ack, connection.firstAttemptOver).collect { onHandshake(connection) }
                }
                active[target] = Member(connection, watcher)
                connection to watcher
            }
        }
        for (connection in revived) {
            log.info("Hub target {} is resolved again; keeping its connection", connection.target)
            val announced = synchronized(lock) { types }
            // Replaces the empty registration it announced while draining.
            connection.register(announced?.commands.orEmpty(), announced?.queries.orEmpty(), announced?.shared.orEmpty())
            onHandshake(connection) // dedupe and the live hubs, as for a fresh handshake
        }
        // Every new target is counted before any of them opens a stream, so each gets its share of the permits.
        for ((connection, watcher) in added) {
            watcher.start()
            connection.start()
        }
    }

    /**
     * N for the permit split of a stream [connection] opens (same rule as the Rust library): the other kept hubs that
     * are live or still on their first connect attempt, plus [connection] itself. A hub that is down after having
     * failed is not counted, so the hubs that are up are not starved by it.
     */
    private fun hubsFor(connection: HubConnection): Int = synchronized(lock) {
        val live = live.value
        1 + active.values.count { m -> m.connection !== connection && (m.connection in live || m.connection.onFirstAttempt) }
    }

    /** A connection's handshake changed (a stream completed it, or ended): dedupe, then refresh the live hubs. */
    private fun onHandshake(connection: HubConnection) {
        var duplicateOf: HubConnection? = null
        var member: Member? = null
        var retry: List<String> = emptyList()
        synchronized(lock) {
            member = active[connection.target]?.takeIf { it.connection === connection } ?: return
            val id = connection.hubId
            if (id != null) {
                // A connection now answering another id (its target reached another hub) gives up the id it owned.
                owners.entries.removeIf { (owned, c) -> c === connection && owned != id }
                // The owner keeps the id while it is kept, even while its stream is down: that stream may just have been
                // replaced by this one's Register (one session per instance id on the hub), so taking the id over here
                // would make the owner's reconnect replace this one in turn, back and forth.
                val owner = owners[id]
                if (owner != null && owner !== connection && active[owner.target]?.connection === owner) {
                    duplicateOf = owner
                    active.remove(connection.target)
                    parked[connection.target] = id
                } else {
                    owners[id] = connection
                }
            }
            retry = refreshLive()
        }
        duplicateOf?.let { owner ->
            log.warn(
                "Hub targets {} and {} answer the same hub id {}: one hub; keeping {}, closing {}",
                owner.target, connection.target, connection.hubId, owner.target, connection.target,
            )
            val closing = member!!
            scope.launch {
                closing.connection.stop()
                closing.watcher.cancel()
            }
        }
        if (retry.isNotEmpty()) add(retry)
        reportStatus()
    }

    /**
     * Recomputes the live hubs. Returns the parked targets whose owner is gone (to connect again: a static target, or a
     * DNS target still resolved). Caller holds [lock].
     */
    private fun refreshLive(): List<String> {
        owners.values.retainAll { c -> active[c.target]?.connection === c }
        val now = active.values.map { it.connection }.filter { c -> !c.rolling && c.hubId?.let { owners[it] === c } == true }
        live.value = now
        anyLive.value = now.isNotEmpty()
        topUp(now)
        val orphaned = parked.filterValues { id -> id !in owners }.keys.toList()
        orphaned.forEach { parked.remove(it) }
        return if (stopped) emptyList() else when (targets) {
            is HubTargets.Static -> orphaned
            is HubTargets.Dns -> orphaned.filter { it in resolvedTargets }
        }
    }

    /** Tops every live hub's stream up to its current share of the permits (see the class comment). Caller holds [lock]. */
    private fun topUp(live: List<HubConnection>) {
        val n = active.values.count { m -> m.connection in live || m.connection.onFirstAttempt }
        if (n < 1) return
        val share = HubProtocol.initialPermits(maxConcurrentHandlers, n)
        for (connection in live) {
            val extra = connection.topUpPermits(share)
            if (extra > 0) log.debug("Hub {}: {} more permits (share {} of {} over {} hubs)", connection.target, extra, share, maxConcurrentHandlers, n)
        }
    }

    private fun reportStatus() {
        val hubs = live.value
        if (hubs.isEmpty()) return
        val ids = hubs.mapNotNull { it.hubId }.distinct()
        val handlers = gate.inFlightHandlers.value
        val processors = try {
            processors()
        } catch (e: Exception) {
            log.debug("Could not build the processor status: {}", e.toString())
            emptyList()
        }
        hubs.forEach { it.reportStatus(ids, handlers, processors) }
    }

    private suspend fun resolveLoop(dns: HubTargets.Dns) {
        while (true) {
            val resolved = try {
                HubTargets.pickAddresses(resolver(dns.host)).map { HubTargets.target(it, dns.port) }.distinct()
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                log.warn("Resolving hub address {} failed: {}", dns.host, e.toString())
                emptyList()
            }
            if (resolved.isEmpty()) {
                log.warn("Hub address {} resolved to no address; keeping the current hub connections", dns.host)
            } else {
                reconcile(resolved)
            }
            delay(dnsRefresh.toMillis())
        }
    }

    private fun reconcile(resolved: List<String>) {
        val gone = synchronized(lock) {
            if (stopped) return
            resolvedTargets = resolved
            // A parked duplicate is forgotten once its address is no longer resolved (refreshLive frees it when its owner goes).
            parked.keys.retainAll(resolved.toSet())
            val vanished = active.keys.filter { it !in resolved }.map { active.remove(it)!! }
            draining += vanished
            refreshLive()
            vanished
        }
        for (member in gone) {
            log.info("Hub target {} is no longer resolved; closing it once idle", member.connection.target)
            scope.launch { drain(member) }
        }
        add(resolved)
    }

    /** The generation of the pair this instance already waited its random delay for (once per pair, not per hub). */
    private var delayedGeneration = -1L

    /**
     * The wait before the roll to a newly seen pair starts: uniform in [0, min([rolloverMaxDelay], 10 % of what is left of the shortest-lived
     * pair the [stale] streams present)], zero when that pair has ended (the hub closes such a session anyway).
     */
    private fun rolloverDelay(certificate: HubClientCertificate, stale: List<HubConnection>): Duration {
        val now = certificate.now()
        val left = stale.mapNotNull { it.certificatePresented?.let { presented -> Duration.between(now, presented.notAfter) } }.minOrNull()
        val bound = if (left == null) rolloverMaxDelay else minOf(rolloverMaxDelay, left.dividedBy(10))
        if (bound.isNegative || bound.isZero) return Duration.ZERO
        return Duration.ofMillis((bound.toMillis() * rolloverRandom().coerceIn(0.0, 1.0)).toLong())
    }

    /** The generation of the pair whose roll did not complete: not tried again until the files change (ERROR logged once). */
    private var abandonedGeneration = -1L

    /**
     * The check tick of a client certificate (P5c): read the files, then roll over the hubs whose stream presents an older pair, one at a
     * time in target order. A hub with no live stream is skipped (its next attempt reads the files anyway). A hub with another live hub leaves
     * routing at once; the only hub leaves it after its work in flight finished (a sender then waits for the new stream, within its deadline).
     * A roll that does not complete (the new stream does not register in time) ends the rollover for that pair with one ERROR: the other hubs
     * keep the old pair until the files change again.
     */
    private suspend fun certificateLoop(certificate: HubClientCertificate) {
        var reported: String? = null // the unexpected problem logged at ERROR and not seen to clear since
        while (true) {
            delay(certificateCheckInterval.toMillis())
            try {
                if (!checkCertificate(certificate)) return
                reported = null
            } catch (e: CancellationException) {
                throw e
            } catch (e: Exception) {
                // Anything unexpected (a JCA provider exception on a key it cannot parse, say) must not end the rotation for the life of the
                // process: log it once per distinct problem and check again on the next tick.
                val problem = "${e.javaClass.name}: ${e.message}"
                if (problem != reported) {
                    reported = problem
                    log.error("The client certificate check failed, rotation continues with the next check: {}", problem, e)
                }
            }
        }
    }

    /** One tick of [certificateLoop]; false when this instance was stopped meanwhile. */
    private suspend fun checkCertificate(certificate: HubClientCertificate): Boolean {
        certificate.refresh()
        if (certificate.generation == abandonedGeneration) return true
        if (certificate.generation != delayedGeneration) {
            val stale = connections().filter { it.connected.value && it.certificateGeneration < certificate.generation }
            if (stale.isNotEmpty()) {
                val wait = rolloverDelay(certificate, stale)
                if (!wait.isZero) {
                    log.info("Renewed client certificate {} seen: starting the roll of {} hub(s) in {} ms (spreads the replicas of this service)", certificate.current.fingerprint, stale.size, wait.toMillis())
                    delay(wait.toMillis())
                    // The files may have changed again meanwhile: roll to the pair that is current now, with no second wait.
                    certificate.refresh()
                    if (certificate.generation == abandonedGeneration) return true
                }
                delayedGeneration = certificate.generation
            }
        }
        for (connection in connections()) {
            if (stopped) return false
            if (!connection.connected.value || connection.certificateGeneration >= certificate.generation) continue
            if (synchronized(lock) { live.value.any { it !== connection } }) takeOutOfRouting(connection)
            val rolled = try {
                connection.rollOver(rolloverDrainTimeout, rolloverReconnectTimeout, onDrained = { takeOutOfRouting(connection) })
            } finally {
                connection.rolling = false
            }
            onHandshake(connection) // back in, if its new stream is registered
            if (!rolled && !stopped) {
                abandonedGeneration = certificate.generation
                log.error(
                    "The roll of hub {} onto the renewed client certificate {} did not complete within {}; the other hubs keep the old pair until the files change again",
                    connection.target, certificate.current.fingerprint, rolloverReconnectTimeout,
                )
                break
            }
        }
        return true
    }

    /** A hub being rolled takes no new request from this instance's senders: out of the live hubs (a sender picks another hub, or waits for this one's new stream). */
    private fun takeOutOfRouting(connection: HubConnection) {
        if (connection.rolling) return
        connection.rolling = true
        onHandshake(connection)
    }

    private suspend fun drain(member: Member) {
        val connection = member.connection
        connection.register(emptySet(), emptySet()) // the hub stops routing to this instance
        withTimeoutOrNull(dnsRefresh.toMillis()) {
            while (!connection.idle && synchronized(lock) { member in draining }) delay(50)
        }
        // Resolved again meanwhile: add() revived it, it stays open.
        if (!synchronized(lock) { draining.remove(member) }) return
        member.watcher.cancel()
        connection.stop()
    }
}

/**
 * A hub's refusal of this instance's registration ([HubConnections.latestRejection]): the [hub] (its id when it registered this
 * instance before, else the target dialled, `host:port`: a refused `Register` carries no hub id), the gRPC status [code]'s name
 * (`PERMISSION_DENIED`), the hub's [description] (null when it gave none) and when it was refused ([at]).
 */
data class HubRejection(val hub: String, val code: String, val description: String?, val at: Instant)

/** The credential a service presents to the hub: nothing, a static token (P5a), a Keycloak access token (P5b) or a client certificate (P5c). */
enum class HubAuthKind { NONE, TOKEN, JWT, CERTIFICATE }

/**
 * [HubConnections.authStatus]: [kind]; for a JWT the [tokenExpiresAt] of the current access token (null before the first fetch
 * and while a hub refused it until it is renewed), the
 * [lastTokenError] (null when the last fetch worked), [tokenFetches] by result (`ok`, `http_error`, `oauth_error`, `io_error`,
 * `bad_response`: the `result` label of `kraken_hub_client_token_fetches_total`) and [credentialsRefreshesSent]
 * (`kraken_hub_client_credentials_refreshes_sent_total`). For a client certificate (P5c): the [certificateService] it names, the [certificateFingerprint] (first 8 hex characters of
 * the leaf's SHA-256, what the hub shows too), its end ([certificateExpiresAt], `notAfter`) and the pairs adopted since start
 * ([certificateRotations], `kraken_hub_client_certificate_rotations_total`). Holds no token and no key.
 */
data class HubAuthStatus(
    val kind: HubAuthKind,
    val tokenExpiresAt: java.time.Instant?,
    val lastTokenError: String?,
    val tokenFetches: Map<String, Long>,
    val credentialsRefreshesSent: Long,
    val certificateService: String? = null,
    val certificateFingerprint: String? = null,
    val certificateExpiresAt: java.time.Instant? = null,
    val certificateRotations: Long = 0,
)
