package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.test.InProcessHub
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.net.InetAddress
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** Multi-hub client (P3, ADR 0016) against in-process hubs: one target name = one in-process hub. */
class HubConnectionsTest {
    private val hubs = mutableListOf<InProcessHub>()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clients = mutableListOf<HubConnections>()
    private val fast = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(200),
    )
    private val wait = Duration.ofSeconds(5)

    /** Target name -> in-process server name (aliases and fake DNS addresses map onto real hubs). */
    private val routes = java.util.concurrent.ConcurrentHashMap<String, String>()
    private val channels = HubChannelFactory { target -> InProcessChannelBuilder.forName(routes[target] ?: target).build() }

    @AfterEach fun tearDown() = runBlocking {
        clients.forEach { it.stop() }
        scope.cancel()
        hubs.forEach { it.close() }
    }

    private fun hub(configure: InProcessHub.() -> Unit = {}): InProcessHub = InProcessHub().apply(configure).start().also { hubs += it }

    private fun client(
        targets: List<String>,
        instanceId: String,
        maxConcurrentHandlers: Int = 256,
        statusInterval: Duration = Duration.ofMillis(200),
        settings: HubSettings = fast,
        resolver: (suspend (String) -> List<InetAddress>)? = null,
        dnsTarget: HubTargets? = null,
        inbound: suspend (Request) -> Reply = { req -> errorReply(req.id, "unexpected") },
    ) = HubConnections(
        targets = dnsTarget ?: HubTargets.Static(targets),
        channelFactory = channels,
        serviceName = "svc-$instanceId",
        instanceId = instanceId,
        settings = settings,
        inbound = inbound,
        scope = scope,
        maxConcurrentHandlers = maxConcurrentHandlers,
        statusInterval = statusInterval,
        dnsRefresh = Duration.ofMillis(100),
        resolver = resolver ?: { host -> InetAddress.getAllByName(host).toList() },
    ).also { clients += it }

    /** An instance of `svc-b` handling `test.Ping` on [targets]. */
    private suspend fun handler(targets: List<String>, instanceId: String = "b"): HubConnections =
        client(targets, instanceId) { req -> payloadReply(req.id, "{\"by\":\"$instanceId\"}") }.apply {
            register(setOf("test.Ping"), emptySet())
            start()
            assertTrue(awaitRegistered(wait))
        }

    // --- registration and status ---

    @Test fun `the identical registration goes to every hub, and both ids are reported to both`() = runBlocking {
        val a = hub()
        val b = hub()
        val conns = client(listOf(a.name, b.name), "x")
        conns.register(setOf("test.Ping"), setOf("test.Get"))
        conns.start()
        assertTrue(conns.awaitRegistered(wait))

        val onA = a.lastRegisterFrom("x")!!
        assertEquals(onA, b.lastRegisterFrom("x"))
        assertEquals(listOf("flow-control", "instance-status"), onA.capabilitiesList)
        assertEquals(setOf(a.hubId, b.hubId), conns.liveHubIds().toSet())
        for (hub in listOf(a, b)) {
            eventually(2_000) { hub.lastStatusFrom("x")?.connectedHubIdsList?.toSet() == setOf(a.hubId, b.hubId) }
            assertEquals(0, hub.lastStatusFrom("x")!!.handlersInFlight)
        }
    }

    @Test fun `status is never sent to a hub without the instance-status capability`() = runBlocking {
        val p3 = hub()
        val p2 = hub { instanceStatus = false }
        val conns = client(listOf(p3.name, p2.name), "x", statusInterval = Duration.ofMillis(100))
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        eventually(2_000) { (p3.statusMessagesFrom("x")) >= 3 }
        assertEquals(setOf(p3.hubId, p2.hubId), p3.lastStatusFrom("x")!!.connectedHubIdsList.toSet())
        assertEquals(0, p2.statusMessagesFrom("x"))
    }

    @Test fun `status reports the handlers in flight over all hubs`() = runBlocking {
        val a = hub()
        val b = hub()
        val release = kotlinx.coroutines.CompletableDeferred<Unit>()
        val busy = client(listOf(a.name, b.name), "b") { req -> release.await(); payloadReply(req.id, "{}") }
        busy.register(setOf("test.Ping"), emptySet())
        busy.start()
        assertTrue(busy.awaitRegistered(wait))
        val sender = client(listOf(a.name, b.name), "a")
        sender.start()
        assertTrue(sender.awaitRegistered(wait))

        val calls = (1..4).map { async { sender.send(request(timeoutMs = 5_000)) } }
        eventually(2_000) { busy.inFlightHandlers.value == 4 }
        eventually(2_000) { a.lastStatusFrom("b")?.handlersInFlight == 4 && b.lastStatusFrom("b")?.handlersInFlight == 4 }
        release.complete(Unit)
        calls.awaitAll()
        Unit
    }

    // --- permits ---

    @Test fun `each hub gets ceil(max over hubs) initial permits, one hub gets them all`() = runBlocking {
        val a = hub()
        val b = hub()
        val split = client(listOf(a.name, b.name), "x", maxConcurrentHandlers = 9)
        split.start()
        assertTrue(split.awaitRegistered(wait))
        assertEquals(5, a.lastRegisterFrom("x")!!.initialPermits)
        assertEquals(5, b.lastRegisterFrom("x")!!.initialPermits)
        assertEquals(5, a.permitsAvailable("x"))

        val single = client(listOf(a.name), "y", maxConcurrentHandlers = 9)
        single.start()
        assertTrue(single.awaitRegistered(wait))
        assertEquals(9, a.lastRegisterFrom("y")!!.initialPermits)
    }

    @Test fun `N counts the hubs that are live or still on their first attempt, plus the registering one`() = runBlocking {
        val a = hub()
        val b = hub()
        val dead = "no-such-hub-" + UUID.randomUUID()
        val conns = client(listOf(a.name, dead, b.name), "x", maxConcurrentHandlers = 9)
        conns.start()
        eventually(5_000) { conns.liveHubIds().size == 2 }
        // At start every target is on its first attempt: N = 3.
        assertEquals(3, a.lastRegisterFrom("x")!!.initialPermits)
        assertEquals(3, b.lastRegisterFrom("x")!!.initialPermits)
        // Hub A's new stream: B is live, the dead target has failed an attempt: N = 1 + 1.
        val before = a.registrationsFrom("x")
        a.dropInstance("x")
        eventually(5_000) { a.registrationsFrom("x") > before }
        assertEquals(5, a.lastRegisterFrom("x")!!.initialPermits)
    }

    @Test fun `a hub going down tops up the survivors' permits, and nothing is revoked when it returns`() = runBlocking {
        val a = hub()
        val b = hub()
        val x = client(listOf(a.name, b.name), "x", maxConcurrentHandlers = 8)
        x.start()
        assertTrue(x.awaitRegistered(wait))
        assertEquals(4, a.lastRegisterFrom("x")!!.initialPermits)
        assertEquals(4, b.lastRegisterFrom("x")!!.initialPermits)
        assertEquals(0, a.permitsGrantedBy("x"))

        // N 2 -> 1: the survivor gets ceil(8/1) - ceil(8/2) = 4 more.
        b.close()
        hubs.remove(b)
        eventually(1_000) { a.permitsGrantedBy("x") == 4 }
        assertEquals(1, a.permitMessagesFrom("x"))
        assertEquals(8, a.permitsAvailable("x")) // InProcessHub does not clamp at initial_permits (kraken-hub P3 does)

        // N 1 -> 2: the returning hub gets ceil(8/2) = 4 fresh; the survivor keeps its 8.
        val back = InProcessHub(b.name).start().also { hubs += it }
        eventually(5_000) { x.liveHubIds().size == 2 }
        assertEquals(4, back.lastRegisterFrom("x")!!.initialPermits)
        assertEquals(4, back.permitsAvailable("x"))
        delay(300)
        assertEquals(4, a.permitsGrantedBy("x"))
        assertEquals(8, a.permitsAvailable("x"))
        assertEquals(0, back.permitsGrantedBy("x"))

        // Another flap: the survivor's stream has already been offered ceil(8/1) = 8, so it gets nothing more.
        back.close()
        hubs.remove(back)
        eventually(5_000) { x.liveHubIds().size == 1 }
        delay(300)
        assertEquals(4, a.permitsGrantedBy("x"), "top-ups never add up over flaps")
    }

    @Test fun `a hub down at start leaves its share to the others once its first attempt fails`() = runBlocking {
        val a = hub()
        val dead = "no-such-hub-" + UUID.randomUUID()
        val x = client(listOf(a.name, dead), "x", maxConcurrentHandlers = 8)
        x.start()
        eventually(5_000) { x.liveHubIds().size == 1 }
        // Both on their first attempt at start: 4 each. Whatever the order of the dead attempt's failure and A's
        // handshake, A ends up offered ceil(8/1) = 8.
        assertEquals(4, a.lastRegisterFrom("x")!!.initialPermits)
        eventually(1_000) { a.permitsGrantedBy("x") == 4 }
    }

    @Test fun `the local guard stays max-concurrent-handlers in total over all hubs`() = runBlocking {
        // P0 hubs: unlimited forwarding from both, only the client's own guard holds handlers back.
        val a = hub { flowControl = false }
        val b = hub { flowControl = false }
        val running = java.util.concurrent.atomic.AtomicInteger()
        val peak = java.util.concurrent.atomic.AtomicInteger()
        val handler = client(listOf(a.name, b.name), "b", maxConcurrentHandlers = 2) { req ->
            peak.accumulateAndGet(running.incrementAndGet(), ::maxOf)
            delay(50)
            running.decrementAndGet()
            payloadReply(req.id, "{}")
        }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        assertTrue(handler.awaitRegistered(wait))
        val sender = client(listOf(a.name, b.name), "a")
        sender.start()
        assertTrue(sender.awaitRegistered(wait))
        val answers = (1..12).map { async { sender.send(request(timeoutMs = 5_000)) } }.awaitAll()
        assertTrue(answers.all { it.hasReply() && !it.reply.hasError() }, answers.toString())
        assertTrue(peak.get() <= 2, "peak ${peak.get()} handlers")
    }

    // --- routing ---

    @Test fun `unkeyed requests alternate over the live hubs and skip a dead one`() = runBlocking {
        val a = hub()
        val b = hub()
        val dead = "no-such-hub-" + UUID.randomUUID()
        handler(listOf(a.name, b.name))
        val sender = client(listOf(a.name, dead, b.name), "a")
        sender.start()
        eventually(5_000) { sender.liveHubIds().size == 2 }

        repeat(40) {
            val answer = sender.send(request())
            assertTrue(answer.hasReply() && !answer.reply.hasError(), answer.toString())
        }
        assertEquals(20, a.requestsFrom("a"))
        assertEquals(20, b.requestsFrom("a"))
    }

    @Test fun `replies go back on the stream the request arrived on`() = runBlocking {
        val a = hub()
        val b = hub()
        handler(listOf(a.name, b.name))
        val viaA = client(listOf(a.name), "a1").apply { start(); assertTrue(awaitRegistered(wait)) }
        val viaB = client(listOf(b.name), "a2").apply { start(); assertTrue(awaitRegistered(wait)) }
        // Each hub relays only a reply that comes back on the handler stream it forwarded on; any other is dropped.
        repeat(5) {
            assertTrue(viaA.send(request()).let { it.hasReply() && !it.reply.hasError() })
            assertTrue(viaB.send(request()).let { it.hasReply() && !it.reply.hasError() })
        }
    }

    @Test fun `keyed requests go to the rendezvous hub of their key`() = runBlocking {
        val a = hub()
        val b = hub()
        handler(listOf(a.name, b.name))
        val sender = client(listOf(a.name, b.name), "a")
        sender.start()
        assertTrue(sender.awaitRegistered(wait))
        eventually(5_000) { sender.liveHubIds().size == 2 }

        val owners = mutableSetOf<String>()
        for (key in (1..30).map { "order-$it" }) {
            val expected = if (Rendezvous.choose(key, listOf(a.hubId, b.hubId)) { id -> id } == a.hubId) a else b
            owners += expected.hubId
            val before = expected.requestsFrom("a")
            repeat(3) { assertTrue(sender.send(request(routingKey = key)).hasReply()) }
            assertEquals(before + 3, expected.requestsFrom("a"), "every request with key $key goes to ${expected.hubId}")
        }
        assertEquals(setOf(a.hubId, b.hubId), owners, "both hubs own some keys")
    }

    @Test fun `no live hub is HubUnavailable within the request timeout`() = runBlocking {
        val conns = client(listOf("no-such-hub-" + UUID.randomUUID()), "a", settings = fast.copy(requestTimeout = Duration.ofMillis(300)))
        conns.start()
        val started = System.nanoTime()
        // No timeout_ms of its own: request-timeout bounds the wait.
        assertFailsWith<HubUnavailableException> { withTimeout(5_000) { conns.send(request(timeoutMs = 0)) } }
        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofSeconds(2))
    }

    @Test fun `waiting for a live hub ends at the call's own timeout, not the default request timeout`() = runBlocking {
        val conns = client(listOf("no-such-hub-" + UUID.randomUUID()), "a") // request-timeout 2 s
        conns.start()
        val started = System.nanoTime()
        assertFailsWith<HubUnavailableException> { withTimeout(5_000) { conns.send(request(timeoutMs = 500)) } }
        val took = Duration.ofNanos(System.nanoTime() - started).toMillis()
        assertTrue(took in 400..1_000, "failed after $took ms")
    }

    @Test fun `a request moved to another hub carries what is left of its timeout`() = runBlocking {
        val a = hub()
        val b = hub()
        handler(listOf(a.name, b.name))
        val sender = client(listOf(a.name, b.name), "a")
        sender.start()
        assertTrue(sender.awaitRegistered(wait))
        eventually(5_000) { sender.liveHubIds().size == 2 }

        val dropped = AtomicBoolean()
        sender.beforeWrite = { conn ->
            if (conn.hubId == a.hubId && dropped.compareAndSet(false, true)) {
                delay(400)
                a.dropInstance("a")
                withTimeout(5_000) { conn.connected.first { !it } }
            }
        }
        var resent: Request? = null
        repeat(4) { // round-robin: hub A is picked within two calls
            if (resent == null) {
                val answer = sender.send(request(timeoutMs = 1_000))
                assertTrue(answer.hasReply() && !answer.reply.hasError(), answer.toString())
                if (dropped.get()) resent = b.lastRequest()
            }
        }
        assertTrue(resent!!.timeoutMs in 1..650, "resent with timeout_ms ${resent!!.timeoutMs}")
    }

    // --- failover ---

    @Test fun `a request whose hub drops before it is written goes to another hub`() = runBlocking {
        val a = hub()
        val b = hub()
        handler(listOf(a.name, b.name))
        val sender = client(listOf(a.name, b.name), "a")
        sender.start()
        assertTrue(sender.awaitRegistered(wait))
        eventually(5_000) { sender.liveHubIds().size == 2 }

        val dropped = AtomicBoolean()
        sender.beforeWrite = { conn ->
            // The first pick of hub A: its stream breaks between the choice and the write.
            if (conn.hubId == a.hubId && dropped.compareAndSet(false, true)) {
                a.dropInstance("a")
                withTimeout(5_000) { conn.connected.first { !it } }
            }
        }
        val answers = (1..4).map { sender.send(request()) }
        assertTrue(dropped.get())
        assertTrue(answers.all { it.hasReply() && !it.reply.hasError() }, answers.toString())
    }

    @Test fun `requests still queued for a hub whose stream breaks move to another hub`() = runBlocking {
        val a = hub()
        val b = hub()
        handler(listOf(a.name, b.name))
        val sender = client(listOf(a.name, b.name), "a")
        sender.start()
        assertTrue(sender.awaitRegistered(wait))
        eventually(5_000) { sender.liveHubIds().size == 2 }

        a.pauseReading = true // hub A stops reading its streams: what the client writes for it stays queued
        val calls = (1..20).map { async { runCatching { sender.send(request(timeoutMs = 5_000)) } } }
        delay(300)
        a.dropInstance("a")
        val results = withTimeout(10_000) { calls.awaitAll() }
        val failures = results.mapNotNull { it.exceptionOrNull() }
        failures.forEach { assertIs<HubConnectionLostException>(it, it.toString()) }
        // At most the request hub A had already taken from the stream is lost; the queued ones were never sent.
        assertTrue(failures.size <= 2, "failed: $failures")
        assertTrue(b.requestsFrom("a") >= 18, "hub B served ${b.requestsFrom("a")}")
    }

    // --- dedupe and DNS ---

    @Test fun `two targets answering the same hub id are one hub`() = runBlocking {
        val a = hub()
        val b = hub()
        routes["alias-of-a"] = a.name
        val conns = client(listOf(a.name, "alias-of-a", b.name), "x")
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        eventually(5_000) { conns.connections().size == 2 && a.sessionsOf("x") == 1 }
        assertEquals(listOf(a.hubId, b.hubId), conns.liveHubIds())
        // The target that acknowledged first is kept (the two race here); the other one stays closed.
        val kept = conns.connections().map { it.target }
        assertTrue(kept == listOf(a.name, b.name) || kept == listOf("alias-of-a", b.name), kept.toString())
        delay(500)
        assertEquals(1, a.sessionsOf("x"))
    }

    @Test fun `a duplicate target that evicted the kept one's session never takes the hub over`() = runBlocking {
        // The hub keeps one session per instance id (replaceSessions, the default): the alias's Register ends the
        // session of the target that acknowledged first. The first one stays the hub's connection; the alias is parked.
        // The ack delay (network latency, well below the reconnect backoff) lets the client see the eviction before the
        // alias's ack.
        val a = hub { ackDelay = Duration.ofMillis(30) }
        routes["alias-of-a"] = a.name
        val settings = fast.copy(backoffMin = Duration.ofMillis(150), backoffMax = Duration.ofMillis(300))
        val x = client(listOf(a.name, "alias-of-a"), "x", settings = settings) { req -> payloadReply(req.id, "{}") }
        x.register(setOf("test.Ping"), emptySet())
        x.start()
        eventually(5_000) { x.connections().size == 1 && x.liveHubIds() == listOf(a.hubId) && a.sessionsOf("x") == 1 }
        val kept = x.connections().single()
        val opened = a.streamsOpenedBy("x")
        val sender = client(listOf(a.name), "s").apply { start(); assertTrue(awaitRegistered(wait)) }

        val until = System.nanoTime() + 1_500_000_000L
        var calls = 0
        while (System.nanoTime() < until) {
            val answer = sender.send(request(timeoutMs = 1_000))
            assertTrue(answer.hasReply() && !answer.reply.hasError(), "call $calls: $answer")
            assertEquals(1, a.sessionsOf("x"))
            calls++
            delay(10)
        }
        assertEquals(opened, a.streamsOpenedBy("x"), "no stream of x was opened (and none replaced) once settled")
        assertTrue(x.connections().single() === kept)
        assertTrue(x.awaitRegistered(wait))
    }

    @Test fun `a legacy hub is identified by its target`() = runBlocking {
        val v1 = hub { legacy = true }
        val conns = client(listOf(v1.name), "x")
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        assertEquals(listOf(v1.name), conns.liveHubIds())
        assertIs<HubHandshake.Legacy>(conns.connections().single().handshake.value)
        delay(500)
        assertEquals(0, v1.statusMessagesFrom("x"))
    }

    @Test fun `dns targets are re-resolved - new addresses connect, vanished ones close`() = runBlocking {
        val a = hub()
        val b = hub()
        routes["10.0.0.1:7000"] = a.name
        routes["10.0.0.2:7000"] = b.name
        val answer = AtomicReference(listOf("10.0.0.1"))
        val conns = client(
            emptyList(), "x",
            dnsTarget = HubTargets.Dns("kraken-hub", 7000),
            resolver = { host -> check(host == "kraken-hub"); answer.get().map(InetAddress::getByName) },
        )
        conns.register(setOf("test.Ping"), emptySet())
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        assertEquals(listOf(a.hubId), conns.liveHubIds())

        answer.set(listOf("10.0.0.1", "10.0.0.2"))
        eventually(5_000) { conns.liveHubIds().toSet() == setOf(a.hubId, b.hubId) }
        assertEquals(setOf("test.Ping"), b.registeredTypes())

        answer.set(emptyList()) // a failed or empty answer keeps what is connected
        delay(400)
        assertEquals(2, conns.liveHubIds().size)

        answer.set(listOf("10.0.0.2"))
        eventually(5_000) { conns.liveHubIds() == listOf(b.hubId) && a.connectedInstances().isEmpty() }
        assertEquals(listOf("10.0.0.2:7000"), conns.connections().map { it.target })
    }

    @Test fun `a dns target that comes back while draining is revived, never dialed a second time`() = runBlocking {
        // The hub keeps one session per instance id: a second stream to a hub whose draining stream still serves a
        // request would end that stream and fail the request.
        val a = hub()
        val b = hub()
        routes["10.0.0.1:7000"] = a.name
        routes["10.0.0.2:7000"] = b.name
        val answer = AtomicReference(listOf("10.0.0.1", "10.0.0.2"))
        val release = kotlinx.coroutines.CompletableDeferred<Unit>()
        val x = client(
            emptyList(), "x",
            dnsTarget = HubTargets.Dns("kraken-hub", 7000),
            resolver = { answer.get().map(InetAddress::getByName) },
        ) { req -> release.await(); payloadReply(req.id, "{}") }
        x.register(setOf("test.Ping"), emptySet())
        x.start()
        eventually(5_000) { x.liveHubIds().toSet() == setOf(a.hubId, b.hubId) }
        assertTrue(x.awaitRegistered(wait))
        val viaA = x.connections().single { it.target == "10.0.0.1:7000" }
        val sender = client(listOf(a.name), "s").apply { start(); assertTrue(awaitRegistered(wait)) }
        val call = async { sender.send(request(timeoutMs = 5_000)) }
        eventually(2_000) { x.inFlightHandlers.value == 1 }

        answer.set(listOf("10.0.0.2")) // the busy stream to hub A drains
        eventually(2_000) { x.connections().map { it.target } == listOf("10.0.0.2:7000") }
        answer.set(listOf("10.0.0.1", "10.0.0.2")) // and comes back within the drain window
        eventually(2_000) { x.connections().size == 2 }
        assertTrue(x.connections().any { it === viaA }, "the draining connection is revived")
        assertEquals(1, a.streamsOpenedBy("x"))
        assertEquals(0, a.sessionsReplacedOf("x"))

        release.complete(Unit)
        val reply = call.await()
        assertTrue(reply.hasReply() && !reply.reply.hasError(), reply.toString())
        assertTrue(x.awaitRegistered(wait))
        eventually(2_000) { "test.Ping" in a.registeredTypes() }
        assertEquals(setOf(a.hubId, b.hubId), x.liveHubIds().toSet())
    }

    // --- helpers ---

    /** Suspends (never blocks: the calls started with `async` in the test must keep running) until [condition] holds. */
    private suspend fun eventually(timeoutMs: Long, condition: () -> Boolean) =
        withTimeout(timeoutMs) { while (!condition()) delay(10) }

    private fun request(type: String = "test.Ping", timeoutMs: Int = 2000, routingKey: String = "") = Request.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setKind(RequestKind.COMMAND)
        .setType(type)
        .setRoutingKey(routingKey)
        .setPayload(ByteString.copyFromUtf8("{\"text\":\"x\"}"))
        .setTimeoutMs(timeoutMs)
        .build()

    private fun payloadReply(id: String, json: String): Reply =
        Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun errorReply(id: String, message: String): Reply = Reply.newBuilder().setRequestId(id)
        .setError(Error.newBuilder().setRequestId(id).setCode(ErrorCode.HANDLER_ERROR).setMessage(message)).build()
}
