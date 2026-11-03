package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.ReplyPart
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.ServerMessage
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.UnsupportedByHubException
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
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.slf4j.LoggerFactory
import java.net.InetAddress
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

/**
 * This instance's connections to every hub (multi-hub HA, ADR 0016): one [HubConnection] (one `Connect` stream, with its
 * own handshake, heartbeat, reconnect backoff and legacy detection) per hub target, all sending the identical
 * registration. With a single target it behaves like that one connection.
 *
 * - **Targets**: [HubTargets.Static] connects to each listed target; [HubTargets.Dns] resolves the name every
 *   [dnsRefresh] (one address family, [HubTargets.pickAddresses]), connects to new addresses, and closes a vanished one
 *   once idle (it first announces an empty registration, then waits up to [dnsRefresh] for its requests and handlers).
 *   An address resolved again while its connection is still draining revives that connection (its registration is
 *   announced again) instead of opening a second stream. A failed or empty resolution keeps the current connections.
 * - **Dedupe**: a hub is its `RegisterAck.hub_id` (a legacy hub: its target). The first target to acknowledge an id
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
 *   `HubConnectionLostException` as before.
 * - **Scatter-gather**: one gather goes to one live hub, round-robin like an unkeyed request, chosen among the hubs whose
 *   ack lists `scatter-gather` (during a rolling hub upgrade the others are skipped); with live hubs and none of them
 *   capable it fails at once with [UnsupportedByHubException]. A gather not yet written fails over like a request; once
 *   written, a broken stream fails it with `HubConnectionLostException` (the parts already received are not asked again).
 * - **Streaming** (P4b): a streaming query goes to one live hub whose ack lists `streaming`, chosen and failed over
 *   exactly like a gather; with live hubs and none capable it fails at once with [UnsupportedByHubException]. Its
 *   `timeout_ms` is the idle timeout and is sent whole, never reduced by a hub wait or a failover (the deadline of the
 *   call bounds only those); a collector exception (`StreamCollectorException`) is never a reason to fail over.
 * - **Subscriptions** (P4c): a subscription goes to one live hub whose ack lists `subscription`, chosen and failed over
 *   like a gather; with live hubs and none capable it fails at once with [UnsupportedByHubException]. `timeout_ms` bounds
 *   the initial request (as for a unary request: what is left of the call's deadline after a hub wait is sent). Instances
 *   serve subscriptions from every hub they are connected to through one shared [SubscriptionRegistry].
 * - **Permits**: a stream announces `ceil(maxConcurrentHandlers / N)` initial permits, N = 1 (itself) + the other kept
 *   hubs that are live or still on their first connect attempt. Whenever that set changes, every live hub's stream is
 *   topped up to `ceil(maxConcurrentHandlers / N)` with N = the kept hubs that are live or on their first attempt:
 *   `Permits{count = max(0, ceil(max / N) - offered), grant = ceil(max / N)}`, offered = what the stream announced plus
 *   its earlier top-ups; the grant raises the hub's permit cap on the session (otherwise `initial_permits`). So the
 *   survivors of an outage (or of a hub that failed its first attempt) serve at full capacity, flaps never add up, a
 *   hub that joins or returns gets `ceil(max / N)` on its new stream, and nothing is ever revoked (rule shared with
 *   the Rust library). One [HandlerGate] keeps the local bound at [maxConcurrentHandlers] over all hubs.
 * - **Status**: every [statusInterval], and whenever the live set changes, each live hub whose ack lists
 *   `instance-status` gets an `InstanceStatus` with the ids of the acknowledged (protocol v2) live hubs and the handlers
 *   in flight. Hubs without the capability never get one.
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

    /** Incoming requests of every hub currently being handled (or waiting for a handler permit). */
    val inFlightHandlers: StateFlow<Int> get() = gate.inFlightHandlers

    /** Test hook: runs after a hub is chosen for a request and before the request is handed to it. */
    internal var beforeWrite: (suspend (HubConnection) -> Unit)? = null

    /** Ids of the live hubs, in target order. */
    fun liveHubIds(): List<String> = live.value.mapNotNull { it.hubId }

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
        route(request, eligible = { true }) { connection, outgoing -> connection.send(outgoing, awaitStream = false) }

    /**
     * Sends [request] as a scatter-gather through one live hub that supports it (see the class comment), with
     * [HubConnection.gather]'s parts, result and exceptions.
     *
     * @throws UnsupportedByHubException when hubs are live but none lists `scatter-gather`
     */
    override suspend fun gather(request: Request, onPart: suspend (ReplyPart) -> Unit): ServerMessage =
        route(request, eligible = { it.supportsScatterGather }, feature = HubProtocol.SCATTER_GATHER) { connection, outgoing ->
            connection.gather(outgoing, awaitStream = false, onPart)
        }

    /**
     * Sends [request] as a streaming query through one live hub that supports it (see the class comment), with
     * [HubConnection.stream]'s parts, result and exceptions.
     *
     * @throws UnsupportedByHubException when hubs are live but none lists `streaming`
     */
    override suspend fun stream(request: Request, onPart: suspend (ReplyPart) -> Unit): ServerMessage =
        route(request, eligible = { it.supportsStreaming }, feature = HubProtocol.STREAMING, keepTimeout = true) { connection, outgoing ->
            connection.stream(outgoing, awaitStream = false, onPart)
        }

    /**
     * Sends [request] as a subscription query through one live hub that supports it (see the class comment), with
     * [HubConnection.subscribe]'s result and exceptions. `timeout_ms` bounds the initial request: it is what is left of the
     * call's deadline after a hub wait or a hub that did not take the request, as for [send].
     *
     * @throws UnsupportedByHubException when hubs are live but none lists `subscription`
     */
    override suspend fun subscribe(request: Request): SubscribeResult =
        route(request, eligible = { it.supportsSubscription }, feature = HubProtocol.SUBSCRIPTION) { connection, outgoing ->
            connection.subscribe(outgoing, awaitStream = false)
        }

    /**
     * The hub choice and failover of [send], [gather], [stream] and [subscribe], restricted to the hubs that are [eligible] for
     * [feature]. A positive `timeout_ms` is one deadline for the whole call: a hub wait or a hub that did not take the
     * request uses some of it and the request is sent with what is left, unless [keepTimeout] (a stream: `timeout_ms`
     * is its idle timeout, sent whole; the deadline bounds only the hub wait and the retries).
     */
    private suspend fun <T> route(
        request: Request,
        eligible: (HubConnection) -> Boolean,
        feature: String = HubProtocol.SCATTER_GATHER,
        keepTimeout: Boolean = false,
        call: suspend (HubConnection, Request) -> T,
    ): T {
        // One deadline for the whole call: the caller's timeout (timeout_ms), else request-timeout, else 5 s.
        val wait = if (request.timeoutMs > 0) Duration.ofMillis(request.timeoutMs.toLong())
        else settings.chosenRequestTimeout ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT
        val deadline = System.nanoTime() + wait.toNanos()
        val tried = HashSet<HubConnection>()
        var waited = false
        while (true) {
            val hubs = live.value
            val capable = hubs.filter(eligible)
            if (hubs.isNotEmpty() && capable.isEmpty()) {
                throw UnsupportedByHubException(
                    feature,
                    "no connected hub supports $feature (hubs: ${hubs.joinToString { it.hubId ?: it.target }})",
                )
            }
            val connection = choose(request, capable.filter { it !in tried })
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
                return call(connection, outgoing)
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
                )
                self = connection
                types?.let { connection.register(it.commands, it.queries, it.shared) }
                val watcher = scope.launch(start = CoroutineStart.LAZY) {
                    merge(connection.handshake, connection.firstAttemptOver).collect { onHandshake(connection) }
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
        val now = active.values.map { it.connection }.filter { c -> c.hubId?.let { owners[it] === c } == true }
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
        val ids = hubs.filter { it.handshake.value is HubHandshake.V2 }.mapNotNull { it.hubId }.distinct()
        val handlers = gate.inFlightHandlers.value
        hubs.forEach { it.reportStatus(ids, handlers) }
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
