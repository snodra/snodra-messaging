package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.Reply
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
 *   [dnsRefresh], connects to new addresses, and closes a vanished one once idle (it first announces an empty
 *   registration, then waits up to [dnsRefresh] for its requests and handlers). A failed or empty resolution keeps the
 *   current connections.
 * - **Dedupe**: a hub is its `RegisterAck.hub_id` (a legacy hub: its target). When a target acknowledges an id another
 *   target already answers on a live stream, the later one is closed (logged once); with DNS it stays closed while the
 *   one kept is resolved.
 * - **Live hubs**: the kept connections whose stream completed its handshake ([liveHubIds]).
 * - **Routing**: a request with a routing key goes to the live hub chosen by [Rendezvous] over (key, hub id); any other
 *   round-robin over the live hubs. Replies to incoming requests go on the stream they arrived on ([HubConnection]).
 * - **Failover**: a request that its hub could not take before writing it (the stream closed, or broke with the
 *   request still queued) goes to another live hub; when none is left the call waits for the set of live hubs to change,
 *   up to `requestTimeout` (default 5 s) in total, then fails with [HubUnavailableException]. A request already written
 *   fails with `HubConnectionLostException` as before.
 * - **Permits**: a stream announces `ceil(maxConcurrentHandlers / N)` initial permits, N = 1 (itself) + the other kept
 *   hubs that are live or still on their first connect attempt; never revoked. One [HandlerGate] keeps the local bound
 *   at [maxConcurrentHandlers] over all hubs.
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
    private val owners = HashMap<String, HubConnection>() // hub id -> the connection that answered it first
    private val duplicates = HashMap<String, String>() // target closed as a duplicate -> target kept
    private var types: Pair<Set<String>, Set<String>>? = null
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

    /** Replaces the announced handler types on every hub; re-sent on every reconnect and to hubs added later. */
    fun register(commandTypes: Set<String>, queryTypes: Set<String>) {
        val connections = synchronized(lock) {
            types = commandTypes to queryTypes
            active.values.map { it.connection }
        }
        connections.forEach { it.register(commandTypes, queryTypes) }
    }

    /**
     * True once every kept hub connection confirmed the most recent [register] call, false if [timeout] passes first
     * (for example while a configured hub is down).
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
                "hub ${c.target} rejected the registration of $serviceName ($instanceId): " +
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
    override suspend fun send(request: Request): ServerMessage {
        val wait = settings.chosenRequestTimeout ?: HubProtocol.DEFAULT_REQUEST_TIMEOUT
        val deadline = System.nanoTime() + wait.toNanos()
        val tried = HashSet<HubConnection>()
        while (true) {
            val hubs = live.value
            val connection = choose(request, hubs.filter { it !in tried })
            if (connection == null) {
                val leftMs = (deadline - System.nanoTime()) / 1_000_000
                val changed = leftMs > 0 && withTimeoutOrNull(leftMs) { live.first { it != hubs } } != null
                if (!changed) {
                    throw HubUnavailableException(
                        if (tried.isEmpty()) "no hub connected within $wait" else "no hub took the request within $wait",
                    )
                }
                tried.clear() // the live hubs changed: a hub that failed before may have a new stream
                continue
            }
            beforeWrite?.invoke(connection)
            try {
                return connection.send(request, awaitStream = false)
            } catch (e: HubUnavailableException) {
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
        val added = synchronized(lock) {
            if (stopped) return
            newTargets.filter { it !in active && it !in duplicates }.map { target ->
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
                )
                self = connection
                types?.let { (commands, queries) -> connection.register(commands, queries) }
                val watcher = scope.launch(start = CoroutineStart.LAZY) { connection.handshake.collect { onHandshake(connection) } }
                active[target] = Member(connection, watcher)
                connection to watcher
            }
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
        synchronized(lock) {
            member = active[connection.target]?.takeIf { it.connection === connection } ?: return
            val id = connection.hubId
            if (id != null) {
                val owner = owners[id]
                if (owner != null && owner !== connection && owner.hubId == id && active[owner.target]?.connection === owner) {
                    duplicateOf = owner
                    active.remove(connection.target)
                    duplicates[connection.target] = owner.target
                } else {
                    owners[id] = connection
                }
            }
            refreshLive()
        }
        duplicateOf?.let { owner ->
            log.warn(
                "Hub targets {} and {} answer the same hub id {}: one hub; keeping {}, closing {}",
                owner.target, connection.target, owner.hubId, owner.target, connection.target,
            )
            val closing = member!!
            scope.launch {
                closing.connection.stop()
                closing.watcher.cancel()
            }
        }
        reportStatus()
    }

    /** Caller holds [lock]. */
    private fun refreshLive() {
        owners.values.retainAll { c -> active[c.target]?.connection === c }
        val now = active.values.map { it.connection }.filter { c -> c.hubId?.let { owners[it] === c } == true }
        live.value = now
        anyLive.value = now.isNotEmpty()
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
                resolver(dns.host).map { HubTargets.target(it, dns.port) }.distinct()
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
            // A duplicate stays closed while its own address is resolved and the target kept for it is still connected.
            duplicates.entries.removeIf { (duplicate, kept) -> duplicate !in resolved || kept !in active }
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
        withTimeoutOrNull(dnsRefresh.toMillis()) { while (!connection.idle) delay(50) }
        member.watcher.cancel()
        connection.stop()
        synchronized(lock) { draining.remove(member) }
    }
}
