package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Heartbeat
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.Request
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
 * relayed by request id, `TIMEOUT` fires after `Request.timeoutMs` (or [replyTimeout]) and a late reply is dropped,
 * `HANDLER_DISCONNECTED` is sent for in-flight requests when the handler stream ends, heartbeats are echoed, empty
 * names are `INVALID_ARGUMENT` and malformed or duplicate in-flight requests are `INVALID_REQUEST`. It does not
 * implement consistent hashing on `routing_key`, metrics, or the real hub's owner grace period and heartbeat expiry.
 */
class InProcessHub(val name: String = "kraken-hub-" + UUID.randomUUID()) : AutoCloseable {

    /** Default reply timeout for requests with `timeoutMs == 0`. */
    @Volatile
    var replyTimeout: Duration = Duration.ofSeconds(5)

    private class Session(val out: SendChannel<ServerMessage>) {
        @Volatile var serviceName: String = ""
        @Volatile var instanceId: String = ""
        @Volatile var types: Set<String> = emptySet()
        @Volatile var registered = false
        lateinit var collector: Job
    }

    private class Pending(val sender: Session, val handler: Session, val timer: Job)

    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val lock = Any() // guards sessions, pending and rotation; never held across a suspension
    private val sessions = CopyOnWriteArrayList<Session>()
    private val pending = HashMap<String, Pending>()
    private val rotation = HashMap<String, Int>()
    private val heartbeats = ConcurrentHashMap<String, Int>()
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

    fun heartbeatsFrom(instanceId: String): Int = heartbeats[instanceId] ?: 0

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
                        handle(session, msg)
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
            msg.hasHeartbeat() -> {
                heartbeats.merge(session.instanceId, 1, Int::plus)
                session.out.trySend(
                    ServerMessage.newBuilder().setHeartbeat(Heartbeat.newBuilder().setSentAtMs(msg.heartbeat.sentAtMs)).build(),
                )
            }
        }
    }

    private fun register(session: Session, reg: Register) {
        if (reg.serviceName.isEmpty() || reg.instanceId.isEmpty()) {
            throw Status.INVALID_ARGUMENT.withDescription("service_name and instance_id must not be empty").asException()
        }
        val types = (reg.commandTypesList + reg.queryTypesList).toSet()
        synchronized(lock) {
            for (type in types) {
                val owner = sessions.firstOrNull { it !== session && type in it.types && it.serviceName != reg.serviceName }
                if (owner != null) {
                    throw Status.ALREADY_EXISTS.withDescription("type $type is owned by service ${owner.serviceName}").asException()
                }
            }
            session.serviceName = reg.serviceName
            session.instanceId = reg.instanceId
            session.types = types
            session.registered = true
        }
    }

    private fun forward(sender: Session, req: Request) {
        if (req.id.isEmpty() || req.type.isEmpty()) {
            sender.fail(req.id, ErrorCode.INVALID_REQUEST, "request id and type must not be empty")
            return
        }
        synchronized(lock) {
            if (req.id in pending) {
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
            val timeout = if (req.timeoutMs > 0) Duration.ofMillis(req.timeoutMs.toLong()) else replyTimeout
            val timer = scope.launch {
                delay(timeout.toMillis())
                val expired = synchronized(lock) { pending.remove(req.id) }
                expired?.sender?.fail(req.id, ErrorCode.TIMEOUT, "no reply for ${req.type} within ${timeout.toMillis()} ms")
            }
            pending[req.id] = Pending(sender, handler, timer)
            lastHeaders = req.headersMap.toMap()
            handler.out.trySend(ServerMessage.newBuilder().setRequest(req).build())
        }
    }

    private fun relay(handler: Session, msg: ClientMessage) {
        val reply = msg.reply
        val entry = synchronized(lock) {
            val p = pending[reply.requestId]
            if (p != null && p.handler === handler) pending.remove(reply.requestId) else null
        } ?: return // late (after TIMEOUT) or unknown reply: dropped
        entry.timer.cancel()
        entry.sender.out.trySend(ServerMessage.newBuilder().setReply(reply).build())
    }

    private fun cleanup(session: Session) {
        val failed = synchronized(lock) {
            sessions.remove(session)
            val mine = pending.filterValues { it.handler === session || it.sender === session }
            mine.keys.forEach { pending.remove(it) }
            mine
        }
        failed.forEach { (id, p) ->
            p.timer.cancel()
            if (p.handler === session && p.sender !== session) {
                p.sender.fail(id, ErrorCode.HANDLER_DISCONNECTED, "handler ${session.instanceId} disconnected")
            }
        }
    }

    private fun Session.fail(requestId: String, code: ErrorCode, message: String) {
        out.trySend(
            ServerMessage.newBuilder()
                .setError(Error.newBuilder().setRequestId(requestId).setCode(code).setMessage(message)).build(),
        )
    }
}
