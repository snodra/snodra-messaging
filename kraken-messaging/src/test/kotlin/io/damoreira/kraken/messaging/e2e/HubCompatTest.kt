package io.damoreira.kraken.messaging.e2e

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.hub.v1.TopologyRequest
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.ExportRows
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Row
import io.damoreira.kraken.messaging.fixtures.WatchAccount
import io.damoreira.kraken.messaging.hub.NO_STREAMING_HANDLERS
import io.damoreira.kraken.messaging.hub.StreamingInbound
import io.damoreira.kraken.messaging.UnsupportedByHubException
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.awaitAll
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.HubTargets
import io.damoreira.kraken.messaging.hub.HubHandshake
import io.damoreira.kraken.messaging.hub.HubSettings
import io.damoreira.kraken.messaging.hub.HubToken
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import java.net.ServerSocket
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Compatibility of this client with real hub images (ADR 0013): the hub under test (`KRAKEN_HUB_IMAGE`, default
 * `kraken-hub:dev`) must speak protocol v2 with flow control to it, the merged P0 hub (`KRAKEN_HUB_P0_IMAGE`, default
 * `kraken-hub:p0`: v2 without flow control) must keep working as in P0, and the v1 release (`KRAKEN_HUB_BASELINE_IMAGE`,
 * default `kraken-hub:v1-baseline`) in legacy mode. Each test skips when its image is not built.
 * [HubConnection]s talk to the container over a real channel with the library's default timing.
 */
private const val PONG = "{\"echoed\":\"remote\",\"at\":\"1970-01-01T00:00:00Z\"}"

class HubCompatTest {
    private val log = LoggerFactory.getLogger(HubCompatTest::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val multi = mutableListOf<HubConnections>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val channels = mutableListOf<ManagedChannel>()
    private val defaults = HubSettings(
        requestTimeout = null,
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(500),
        backoffMax = Duration.ofSeconds(5),
    )
    private val wait = Duration.ofSeconds(15)

    @AfterEach fun tearDown() = runBlocking {
        connections.forEach { it.stop() }
        multi.forEach { it.stop() }
        scope.cancel()
        channels.forEach { it.shutdownNow() }
        containers.forEach { runCatching { it.stop() } }
    }

    // --- the hub under test (protocol v2) ---

    @Test fun `new hub acknowledges the registration with its configured values`() = runBlocking {
        val hub = start(newHub(mapOf("KRAKEN_HUB_ID" to "compat-hub", "KRAKEN_HUB_TIMEOUT_OVERRIDES" to "t.Slow=8000,t.Quick=300")))
        val conn = connection(hub, "a", "svc-a")
        conn.register(setOf("t.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        val ack = assertIs<HubHandshake.V2>(conn.handshake.value).ack
        assertEquals("compat-hub", ack.hubId)
        assertEquals(3_000, ack.heartbeatIntervalMs)
        assertEquals(10_000, ack.heartbeatTimeoutMs)
        assertEquals(5_000, ack.defaultTimeoutMs)
        assertEquals(60_000, ack.maxTimeoutMs)
        assertEquals("default", ack.context)
        assertEquals(mapOf("t.Slow" to 8_000, "t.Quick" to 300), ack.typeTimeoutsMsMap)

        // The hub records what the client announced.
        val topology = HubGrpcKt.HubCoroutineStub(channel(hub)).topology(TopologyRequest.getDefaultInstance())
        assertEquals("compat-hub", topology.hubId)
        val instance = topology.servicesList.single { it.serviceName == "svc-a" }.instancesList.single()
        assertTrue(instance.clientVersion.startsWith("kraken-messaging/"), instance.clientVersion)
    }

    @Test fun `new hub applies per-type overrides to requests sent without a timeout`() = runBlocking {
        val hub = start(newHub(mapOf("KRAKEN_HUB_TIMEOUT_OVERRIDES" to "t.Slow=8000,t.Quick=300")))
        val handler = connection(hub, "b", "svc-b") { req ->
            delay(if (req.type == "t.Slow") 6_000 else 2_000) // Slow: beyond the 5 s default, within its 8 s override
            payloadReply(req.id, "{}")
        }
        handler.register(setOf("t.Slow", "t.Quick"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val slow = async { sender.send(request("t.Slow")) }
        val quickStarted = System.nanoTime()
        val quick = sender.send(request("t.Quick"))
        val quickElapsed = Duration.ofNanos(System.nanoTime() - quickStarted)
        assertEquals(ErrorCode.TIMEOUT, quick.error.code, quick.toString())
        assertTrue(quickElapsed < Duration.ofMillis(1_500), "t.Quick timed out after $quickElapsed, not ~300 ms")
        val slowAnswer = withTimeout(10_000) { slow.await() }
        assertTrue(slowAnswer.hasReply() && !slowAnswer.reply.hasError(), slowAnswer.toString())
    }

    @Test fun `new hub - a request in flight when the hub dies is HubConnectionLost, a later one HubUnavailable`() = runBlocking {
        val hub = start(newHub())
        val received = CompletableDeferred<Unit>()
        val handler = connection(hub, "b", "svc-b") { received.complete(Unit); awaitCancellation() }
        handler.register(setOf("t.Ping"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a", defaults.copy(requestTimeout = Duration.ofSeconds(30)))
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val inFlight = async { runCatching { sender.send(request("t.Ping")) } }
        withTimeout(10_000) { received.await() }
        DockerClientFactory.instance().client().killContainerCmd(hub.containerId).exec()

        val lost = withTimeout(20_000) { inFlight.await() }.exceptionOrNull()
        assertIs<HubConnectionLostException>(lost, lost.toString())
        withTimeout(20_000) { sender.connected.first { !it } }
        val unavailable = connection(hub, "c", "svc-c", defaults.copy(requestTimeout = Duration.ofMillis(500)))
        unavailable.start()
        assertFailsWith<HubUnavailableException> { unavailable.send(request("t.Ping")) }
        Unit
    }

    @Test fun `new hub - clients are back within 5 s of a hub restart`() = runBlocking {
        reconnectsWithinFiveSeconds(newHub(), expectV2 = true)
    }

    // --- the hub under test: flow control (P1) ---

    @Test fun `new hub - a slow handler with one permit gives latency, not timeouts`() = runBlocking {
        val hub = start(newHub())
        val handler = connection(hub, "b", "svc-b", maxConcurrentHandlers = 1) { req -> delay(300); payloadReply(req.id, "{}") }
        handler.register(setOf("t.Slow"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val started = System.nanoTime()
        val calls = (1..5).map { async { sender.send(request("t.Slow")) } }
        // While they wait, the hub shows the instance as flow-controlled with its queue.
        val stub = HubGrpcKt.HubCoroutineStub(channel(hub))
        var sawQueue = false
        repeat(20) {
            val instance = stub.topology(TopologyRequest.getDefaultInstance()).servicesList
                .single { it.serviceName == "svc-b" }.instancesList.single()
            assertTrue(instance.permitsAvailable <= 1, instance.toString())
            if (instance.queued > 0) sawQueue = true
            delay(50)
        }
        val answers = calls.awaitAll()
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(answers.all { it.hasReply() && !it.reply.hasError() }, answers.toString())
        assertTrue(elapsed >= Duration.ofMillis(1_500), "5 x 300 ms handled one at a time, took $elapsed")
        assertTrue(sawQueue, "the hub never queued a request")
        log.info("5 calls to a 300 ms handler with one permit took {} ms", elapsed.toMillis())
    }

    @Test fun `new hub - a full queue is HandlerBusy, and retry gets the command through`() = runBlocking {
        val hub = start(newHub(mapOf("KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE" to "1")))
        val release = CompletableDeferred<Unit>()
        val seen = CopyOnWriteArrayList<String>()
        val handler = connection(hub, "b", "svc-b", maxConcurrentHandlers = 1) { req ->
            seen += req.type
            release.await()
            payloadReply(req.id, PONG)
        }
        handler.register(setOf("t.Block", "test.Ping"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val bus = bus(sender)

        val first = async { sender.send(request("t.Block")) } // takes the only permit
        awaitUntil { seen.size == 1 }
        val second = async { sender.send(request("t.Block")) } // fills the queue (capacity 1)
        awaitQueued(hub, "svc-b", 1)

        assertFailsWith<HandlerBusyException> { bus.send(Ping("x"), SendOptions(retry = RetryPolicy.NONE)) }
        val retried = async {
            bus.send(Ping("x"), SendOptions(timeout = Duration.ofSeconds(10), retry = RetryPolicy(maxAttempts = 10, initialBackoff = Duration.ofMillis(200))))
        }
        delay(300)
        release.complete(Unit) // the queue drains; a later attempt finds room
        assertEquals("remote", withTimeout(15_000) { retried.await() }.echoed)
        assertTrue(first.await().hasReply() && second.await().hasReply())
        assertEquals(1, seen.count { it == "test.Ping" }, "rejected attempts never reached the handler")
    }

    @Test fun `new hub - retry never repeats a non-retryable failure or a timed-out command`() = runBlocking {
        val hub = start(newHub())
        val seen = CopyOnWriteArrayList<String>()
        val handler = connection(hub, "b", "svc-b") { req ->
            seen += req.payload.toStringUtf8()
            if (req.payload.toStringUtf8().contains("slow")) { delay(1_500); payloadReply(req.id, PONG) }
            else errorReply(req.id, "boom")
        }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val bus = bus(sender, retry = RetryPolicy(maxAttempts = 5, initialBackoff = Duration.ofMillis(50)))

        assertFailsWith<RemoteHandlerException> { bus.send(Ping("boom")) }
        assertFailsWith<MessageTimeoutException> { bus.send(Ping("slow"), SendOptions(timeout = Duration.ofMillis(500))) }
        delay(1_500)
        assertEquals(1, seen.count { "boom" in it }, seen.toString())
        assertEquals(1, seen.count { "slow" in it }, seen.toString())
    }

    @Test fun `new hub - queued requests are served by priority`() = runBlocking {
        val hub = start(newHub())
        val release = CompletableDeferred<Unit>()
        val order = CopyOnWriteArrayList<String>()
        val handler = connection(hub, "b", "svc-b", maxConcurrentHandlers = 1) { req ->
            order += req.payload.toStringUtf8()
            release.await()
            payloadReply(req.id, "{}")
        }
        handler.register(setOf("t.Work"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val blocker = async { sender.send(request("t.Work", payload = "blocker")) }
        awaitUntil { order.size == 1 }
        val low = async { sender.send(request("t.Work", payload = "low", priority = 0)) }
        awaitQueued(hub, "svc-b", 1)
        val high = async { sender.send(request("t.Work", payload = "high", priority = 5)) }
        awaitQueued(hub, "svc-b", 2)
        release.complete(Unit)
        listOf(blocker, low, high).awaitAll()
        assertEquals(listOf("blocker", "high", "low"), order)
    }

    // --- the merged P0 hub: protocol v2 without flow control ---

    @Test fun `P0 hub acknowledges without flow-control and works as before`() = runBlocking {
        val hub = start(p0Hub())
        val handler = connection(hub, "b", "svc-b", maxConcurrentHandlers = 1) { req -> delay(100); payloadReply(req.id, "{}") }
        handler.register(setOf("t.Ping"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val ack = assertIs<HubHandshake.V2>(handler.handshake.value).ack

        // No permits: the hub forwards everything at once and the client's own semaphore serialises the handlers.
        val answers = (1..4).map { async { sender.send(request("t.Ping", priority = 3)) } }.awaitAll()
        assertTrue(answers.all { it.hasReply() && !it.reply.hasError() }, answers.toString())
        assertEquals(ErrorCode.NO_HANDLER, sender.send(request("t.Nobody")).error.code)
        handler.register(setOf("t.Ping", "t.More"), emptySet())
        assertTrue(handler.awaitRegistered(wait))
    }

    @Test fun `P0 hub - clients are back within 5 s of a hub restart`() = runBlocking {
        reconnectsWithinFiveSeconds(p0Hub(), expectV2 = true)
    }

    // --- the merged P2 hub: flow control without instance-status (P3 clients against it) ---

    @Test fun `P2 hub - one target works as before and gets no InstanceStatus`() = runBlocking {
        val hub = start(p2Hub())
        val handler = hubConnections(hub, "svc-b") { req -> payloadReply(req.id, "{}") }
        handler.register(setOf("t.Ping"), emptySet())
        handler.start()
        val sender = hubConnections(hub, "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val ack = assertIs<HubHandshake.V2>(handler.connections().single().handshake.value).ack
        assertEquals(listOf(ack.hubId), sender.liveHubIds())
        assertEquals(256, permitsAnnounced(hub, "svc-b")) // one hub: the whole budget

        repeat(5) { assertTrue(sender.send(request("t.Ping")).let { it.hasReply() && !it.reply.hasError() }) }
        delay(600) // three status periods of 200 ms
        assertEquals(0, handler.connections().single().statusReportsWritten + sender.connections().single().statusReportsWritten)
    }

    // --- the P3 hub: no scatter-gather (P4a clients against it) ---

    @Test fun `P3 hub - scatter-gather is UnsupportedByHub without sending anything, unary queries still work`() = runBlocking {
        val hub = start(p3Hub())
        val owner = connection(hub, "b", "svc-b") { req -> payloadReply(req.id, "{\"value\":7}") }
        owner.register(emptySet(), setOf("test.GetCount"))
        owner.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val ack = assertIs<HubHandshake.V2>(sender.handshake.value).ack

        val bus = bus(sender)
        val e = assertFailsWith<UnsupportedByHubException> { bus.scatterGather(GetCount("k")) }
        assertEquals("scatter-gather", e.feature)
        assertEquals(Count(7), bus.send(GetCount("k")))
        assertTrue(sender.connected.value) // the refusal is local: the stream is untouched
    }

    // --- the P4a hub: no streaming (P4b clients against it) ---

    @Test fun `P4a hub - streaming is UnsupportedByHub without sending anything, unary queries and gathers still work`() = runBlocking {
        val hub = start(p4aHub())
        val owner = connection(hub, "b", "svc-b") { req -> payloadReply(req.id, "{\"value\":7}") }
        owner.register(emptySet(), setOf("test.ExportRows"), setOf("test.GetCount"))
        owner.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val ack = assertIs<HubHandshake.V2>(sender.handshake.value).ack
        assertEquals(false, sender.supportsStreaming)

        val bus = bus(sender)
        val e = assertFailsWith<UnsupportedByHubException> { bus.stream(ExportRows(3)).toList() }
        assertEquals("streaming", e.feature)
        assertEquals(Count(7), bus.send(GetCount("k")))
        assertEquals(listOf(Count(7)), bus.scatterGather(GetCount("k")).results)
        assertTrue(sender.connected.value) // the refusal is local: the stream is untouched
    }

    // --- the P4b hub: no subscription (P4c clients against it) ---

    @Test fun `P4b hub - subscribe is UnsupportedByHub without sending anything, unary queries, gathers and streams still work`() = runBlocking {
        val hub = start(p4bHub())
        val owner = connection(
            hub, "b", "svc-b",
            inbound = { req -> payloadReply(req.id, "{\"value\":7}") },
            inboundStream = { _, emit ->
                emit("{\"n\":1}".toByteArray())
                emit("{\"n\":2}".toByteArray())
                null
            },
        )
        owner.register(emptySet(), setOf("test.ExportRows", "test.WatchAccount"), setOf("test.GetCount"))
        owner.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val ack = assertIs<HubHandshake.V2>(sender.handshake.value).ack
        assertEquals(false, sender.supportsSubscription)
        assertTrue(owner.supportsStreaming)

        val bus = bus(sender)
        val e = assertFailsWith<UnsupportedByHubException> { bus.subscribe(WatchAccount(7)) }
        assertEquals("subscription", e.feature)
        // Nothing reached the hub: the owner never heard of a subscription, the stream is untouched.
        assertTrue(sender.connected.value)
        val topology = HubGrpcKt.HubCoroutineStub(channel(hub)).topology(TopologyRequest.getDefaultInstance())
        assertEquals(0, topology.servicesList.flatMap { it.instancesList }.sumOf { it.inFlightAsSender + it.inFlightAsHandler })

        assertEquals(Count(7), bus.send(GetCount("k")))
        assertEquals(listOf(Count(7)), bus.scatterGather(GetCount("k")).results)
        assertEquals(listOf(Row(1), Row(2)), bus.stream(ExportRows(2)).toList())
    }

    // --- the P4c hub: it predates access control and ignores Register.credentials (P5a clients against it) ---

    @Test fun `P4c hub - a configured token is harmless, the hub ignores it and everything works`() = runBlocking {
        val hub = start(p4cHub())
        val token = "tok-" + UUID.randomUUID()
        val withToken = defaults.copy(token = HubToken.of(token, null))
        val owner = connection(hub, "b", "svc-b", settings = withToken, inbound = { req -> payloadReply(req.id, "{\"value\":7}") })
        owner.register(emptySet(), setOf("test.GetCount"))
        owner.start()
        val sender = connection(hub, "a", "svc-a", settings = withToken)
        sender.start()
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        assertEquals(null, sender.lastRejection.value)
        assertEquals(Count(7), bus(sender).send(GetCount("k")))
        // The topology shows no trace of the credential: it is not stored, and the hub of this release never read it.
        val topology = HubGrpcKt.HubCoroutineStub(channel(hub)).topology(TopologyRequest.getDefaultInstance())
        assertFalse(token in topology.toString())
        assertFalse(token in hub.logs)
    }

    // --- the previous hub release (protocol v1): legacy mode ---

    @Test fun `baseline hub - one target through HubConnections runs in legacy mode`() = runBlocking {
        val hub = start(baselineHub())
        val handler = hubConnections(hub, "svc-b") { req -> payloadReply(req.id, "{}") }
        handler.register(setOf("t.Ping"), emptySet())
        handler.start()
        val sender = hubConnections(hub, "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        assertEquals(HubHandshake.Legacy, sender.connections().single().handshake.value)
        assertEquals(listOf("${hub.host}:${hub.getMappedPort(7000)}"), sender.liveHubIds()) // a v1 hub has no id: its target

        repeat(5) { assertTrue(sender.send(request("t.Ping")).let { it.hasReply() && !it.reply.hasError() }) }
        assertEquals(ErrorCode.NO_HANDLER, sender.send(request("t.Nobody")).error.code)
        delay(600)
        assertEquals(0, handler.connections().single().statusReportsWritten + sender.connections().single().statusReportsWritten)
    }

    @Test fun `baseline hub runs the client in legacy mode without regression`() = runBlocking {
        val hub = start(baselineHub())
        val handler = connection(hub, "b", "svc-b") { req ->
            if (req.type == "t.Slow") delay(3_000)
            payloadReply(req.id, "{\"echoed\":\"${req.payload.toStringUtf8().length}\"}")
        }
        handler.register(setOf("t.Ping", "t.Slow"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a", defaults.copy(requestTimeout = Duration.ofSeconds(1)))
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        assertEquals(HubHandshake.Legacy, sender.handshake.value)
        assertEquals(HubHandshake.Legacy, handler.handshake.value)

        val answer = sender.send(request("t.Ping"))
        assertTrue(answer.hasReply() && !answer.reply.hasError(), answer.toString())
        assertEquals(ErrorCode.NO_HANDLER, sender.send(request("t.Nobody")).error.code)
        val started = System.nanoTime()
        // Legacy: the configured 1 s is sent; the hub's TIMEOUT and the local guard (also 1 s, as before P0) race, and
        // both reach a bus caller as MessageTimeoutException.
        val timedOut = runCatching { sender.send(request("t.Slow")) }
        assertTrue(
            timedOut.exceptionOrNull() is MessageTimeoutException || timedOut.getOrNull()?.error?.code == ErrorCode.TIMEOUT,
            timedOut.toString(),
        )
        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofMillis(2_500))

        // Re-registration is still confirmed by the heartbeat echo.
        handler.register(setOf("t.Ping", "t.Slow", "t.Extra"), emptySet())
        assertTrue(handler.awaitRegistered(wait))
    }

    @Test fun `baseline hub - clients are back within 5 s of a hub restart`() = runBlocking {
        reconnectsWithinFiveSeconds(baselineHub(), expectV2 = false)
    }

    // --- helpers ---

    private suspend fun reconnectsWithinFiveSeconds(container: GenericContainer<*>, expectV2: Boolean) {
        val port = ServerSocket(0).use { it.localPort }
        // A fixed host port, so the address the client dials stays valid across the restart.
        container.withCreateContainerCmdModifier { cmd ->
            cmd.hostConfig!!.withPortBindings(Ports(PortBinding(Ports.Binding.bindPort(port), ExposedPort.tcp(7000))))
        }
        val hub = start(container)
        val conn = connection(hub, "a", "svc-a")
        conn.register(setOf("t.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        if (expectV2) assertIs<HubHandshake.V2>(conn.handshake.value) else assertEquals(HubHandshake.Legacy, conn.handshake.value)

        DockerClientFactory.instance().client().restartContainerCmd(hub.containerId).withTimeout(5).exec()
        val ready = awaitLogCount(hub, "hub listening", 2)
        withTimeout(10_000) { conn.connected.first { it } }
        assertTrue(conn.awaitRegistered(wait))
        val elapsed = Duration.ofNanos(System.nanoTime() - ready)
        log.info("Re-registered {} ms after the {} hub was listening again", elapsed.toMillis(), if (expectV2) "v2" else "baseline")
        // The backoff never waits more than 5 s; allow 1 s for the connect and the handshake.
        assertTrue(elapsed <= Duration.ofSeconds(6), "re-registered only after $elapsed")
    }

    /** `System.nanoTime()` when [hub]'s log shows [line] for the [count]th time. */
    private suspend fun awaitLogCount(hub: GenericContainer<*>, line: String, count: Int): Long = withTimeout(30_000) {
        while (hub.logs.split(line).size - 1 < count) delay(25)
        System.nanoTime()
    }

    private fun newHub(env: Map<String, String> = emptyMap()): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubImageAvailable(), "hub image under test not built")
        return KrakenContainers.hub().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "").withEnv(env)
    }

    private fun p0Hub(): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubP0ImageAvailable(), "P0 hub image not available")
        return KrakenContainers.hubP0().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
    }

    private fun p2Hub(): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubP2ImageAvailable(), "P2 hub image not available")
        return KrakenContainers.hubP2().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
    }

    private fun p3Hub(): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubP3ImageAvailable(), "P3 hub image not available")
        return KrakenContainers.hubP3().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
    }

    private fun p4aHub(): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubP4aImageAvailable(), "P4a hub image not available")
        return KrakenContainers.hubP4a().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
    }

    private fun p4bHub(): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubP4bImageAvailable(), "P4b hub image not available")
        return KrakenContainers.hubP4b().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
    }

    private fun p4cHub(): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubP4cImageAvailable(), "P4c hub image not available")
        return KrakenContainers.hubP4c().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
    }

    private fun baselineHub(): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubBaselineImageAvailable(), "baseline hub image not available")
        return KrakenContainers.hubBaseline().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
    }

    private fun start(container: GenericContainer<*>): GenericContainer<*> = container.also {
        containers += it
        it.start()
    }

    private fun channel(hub: GenericContainer<*>): ManagedChannel =
        ManagedChannelBuilder.forAddress(hub.host, hub.getMappedPort(7000)).usePlaintext()
            .keepAliveTime(10, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS).keepAliveWithoutCalls(true)
            .build().also { channels += it }

    private fun connection(
        hub: GenericContainer<*>,
        instanceId: String,
        service: String,
        settings: HubSettings = defaults,
        maxConcurrentHandlers: Int = 256,
        inboundStream: StreamingInbound = NO_STREAMING_HANDLERS,
        inbound: suspend (Request) -> Reply = { req -> errorReply(req.id, "unexpected") },
    ): HubConnection {
        val host = hub.host
        val port = hub.getMappedPort(7000)
        return HubConnection(
            channelFactory = { ManagedChannelBuilder.forAddress(host, port).usePlaintext()
                .keepAliveTime(10, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build() },
            serviceName = service,
            instanceId = "$instanceId-" + UUID.randomUUID(),
            settings = settings,
            inbound = inbound,
            scope = scope,
            maxConcurrentHandlers = maxConcurrentHandlers,
            inboundStream = inboundStream,
        ).also { connections += it }
    }

    /** A multi-hub client with one target, [hub]; status reports every 200 ms. */
    private fun hubConnections(
        hub: GenericContainer<*>,
        service: String,
        inbound: suspend (Request) -> Reply = { req -> errorReply(req.id, "unexpected") },
    ): HubConnections = HubConnections(
        targets = HubTargets.parse("static://${hub.host}:${hub.getMappedPort(7000)}"),
        channelFactory = HubChannelFactory { target ->
            ManagedChannelBuilder.forTarget(target).usePlaintext()
                .keepAliveTime(10, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build()
        },
        serviceName = service,
        instanceId = "$service-" + UUID.randomUUID(),
        settings = defaults,
        inbound = inbound,
        scope = scope,
        statusInterval = Duration.ofMillis(200),
    ).also { multi += it }

    /** The `initial_permits` [service]'s instance announced, as the hub's topology shows them before any request. */
    private suspend fun permitsAnnounced(hub: GenericContainer<*>, service: String): Int =
        HubGrpcKt.HubCoroutineStub(channel(hub)).topology(TopologyRequest.getDefaultInstance()).servicesList
            .single { it.serviceName == service }.instancesList.single().permitsAvailable

    private fun bus(connection: HubConnection, retry: RetryPolicy? = null): HubBus {
        val resolver = MessageTypeResolver("test")
        val codec = JsonCodec()
        val registry = HandlerRegistry(resolver)
        return HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), retry)
    }

    private suspend fun awaitUntil(condition: () -> Boolean) = withTimeout(10_000) { while (!condition()) delay(20) }

    /** Waits until the hub's topology shows [count] requests queued for [service]'s instance. */
    private suspend fun awaitQueued(hub: GenericContainer<*>, service: String, count: Int) {
        val stub = HubGrpcKt.HubCoroutineStub(channel(hub))
        withTimeout(10_000) {
            while (stub.topology(TopologyRequest.getDefaultInstance()).servicesList
                    .single { it.serviceName == service }.instancesList.single().queued < count
            ) delay(20)
        }
    }

    private fun request(type: String, payload: String = "{}", priority: Int = 0) = Request.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setKind(RequestKind.COMMAND)
        .setType(type)
        .setPayload(ByteString.copyFromUtf8(payload))
        .setPriority(priority)
        .build()

    private fun payloadReply(id: String, json: String): Reply =
        Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun errorReply(id: String, message: String): Reply = Reply.newBuilder().setRequestId(id)
        .setError(Error.newBuilder().setRequestId(id).setCode(ErrorCode.HANDLER_ERROR).setMessage(message)).build()
}
