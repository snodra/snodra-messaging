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
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import java.util.concurrent.CopyOnWriteArrayList
import kotlinx.coroutines.awaitAll
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.hub.HubSettings
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
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * This client against the real hub image under test (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`): the handshake and the
 * ack's values, timeouts, the error split when the hub goes away, reconnecting, flow control, retry and priorities. Skips
 * when the image is not built. [HubConnection]s talk to the container over a real channel with the library's default timing.
 */
private const val PONG = "{\"echoed\":\"remote\",\"at\":\"1970-01-01T00:00:00Z\"}"

class HubEndToEndTest {
    private val log = LoggerFactory.getLogger(HubEndToEndTest::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val channels = mutableListOf<ManagedChannel>()
    private val defaults = HubSettings(
        requestTimeout = null,
        backoffMin = Duration.ofMillis(500),
        backoffMax = Duration.ofSeconds(5),
    )
    private val wait = Duration.ofSeconds(15)

    @AfterEach fun tearDown() = runBlocking {
        connections.forEach { it.stop() }
        scope.cancel()
        channels.forEach { it.shutdownNow() }
        containers.forEach { runCatching { it.stop() } }
    }

    // --- handshake, timeouts, reconnect ---

    @Test fun `the hub acknowledges the registration with its configured values`() = runBlocking {
        val hub = start(newHub(mapOf("KRAKEN_HUB_ID" to "e2e-hub", "KRAKEN_HUB_TIMEOUT_OVERRIDES" to "t.Slow=8000,t.Quick=300")))
        val conn = connection(hub, "a", "svc-a")
        conn.register(setOf("t.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        val ack = assertNotNull(conn.ack.value)
        assertEquals("e2e-hub", ack.hubId)
        assertEquals("e2e-hub", conn.hubId)
        assertEquals(3_000, ack.heartbeatIntervalMs)
        assertEquals(10_000, ack.heartbeatTimeoutMs)
        assertEquals(5_000, ack.defaultTimeoutMs)
        assertEquals(60_000, ack.maxTimeoutMs)
        assertEquals("default", ack.context)
        assertEquals(mapOf("t.Slow" to 8_000, "t.Quick" to 300), ack.typeTimeoutsMsMap)

        // The hub records what the client announced.
        val topology = HubGrpcKt.HubCoroutineStub(channel(hub)).topology(TopologyRequest.getDefaultInstance())
        assertEquals("e2e-hub", topology.hubId)
        val instance = topology.servicesList.single { it.serviceName == "svc-a" }.instancesList.single()
        assertTrue(instance.clientVersion.startsWith("kraken-messaging/"), instance.clientVersion)
    }

    @Test fun `the hub applies per-type overrides to requests sent without a timeout`() = runBlocking {
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

    @Test fun `a request in flight when the hub dies is HubConnectionLost, a later one HubUnavailable`() = runBlocking {
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

    @Test fun `clients are back within 5 s of a hub restart`() = runBlocking {
        val port = ServerSocket(0).use { it.localPort }
        // A fixed host port, so the address the client dials stays valid across the restart (gRPC only: no HTTP port).
        val container = newHub().withExposedPorts(7000).withCreateContainerCmdModifier { cmd ->
            cmd.hostConfig!!.withPortBindings(Ports(PortBinding(Ports.Binding.bindPort(port), ExposedPort.tcp(7000))))
        }
        val hub = start(container)
        val conn = connection(hub, "a", "svc-a")
        conn.register(setOf("t.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        DockerClientFactory.instance().client().restartContainerCmd(hub.containerId).withTimeout(5).exec()
        val ready = awaitLogCount(hub, "hub listening", 2)
        withTimeout(10_000) { conn.connected.first { it } }
        assertTrue(conn.awaitRegistered(wait))
        val elapsed = Duration.ofNanos(System.nanoTime() - ready)
        log.info("Re-registered {} ms after the hub was listening again", elapsed.toMillis())
        // The backoff never waits more than 5 s; allow 1 s for the connect and the handshake.
        assertTrue(elapsed <= Duration.ofSeconds(6), "re-registered only after $elapsed")
    }

    // --- flow control (P1) ---

    @Test fun `a slow handler with one permit gives latency, not timeouts`() = runBlocking {
        val hub = start(newHub())
        val handler = connection(hub, "b", "svc-b", maxConcurrentHandlers = 1) { req -> delay(300); payloadReply(req.id, "{}") }
        handler.register(setOf("t.Slow"), emptySet())
        handler.start()
        val sender = connection(hub, "a", "svc-a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val started = System.nanoTime()
        val calls = (1..5).map { async { sender.send(request("t.Slow")) } }
        // While they wait, the hub shows the instance with its permits and its queue.
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

    @Test fun `a full queue is HandlerBusy, and retry gets the command through`() = runBlocking {
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

    @Test fun `retry never repeats a non-retryable failure or a timed-out command`() = runBlocking {
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

    @Test fun `queued requests are served by priority`() = runBlocking {
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

    // --- helpers ---

    /** `System.nanoTime()` when [hub]'s log shows [line] for the [count]th time. */
    private suspend fun awaitLogCount(hub: GenericContainer<*>, line: String, count: Int): Long = withTimeout(30_000) {
        while (hub.logs.split(line).size - 1 < count) delay(25)
        System.nanoTime()
    }

    private fun newHub(env: Map<String, String> = emptyMap()): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubImageAvailable(), "hub image under test not built")
        return KrakenContainers.hub().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "").withEnv(env)
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
        ).also { connections += it }
    }

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
