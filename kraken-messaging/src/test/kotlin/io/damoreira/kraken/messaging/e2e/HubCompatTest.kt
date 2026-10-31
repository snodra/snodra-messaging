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
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.hub.HubHandshake
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
import kotlin.test.assertTrue

/**
 * Compatibility of this client with real hub images (ADR 0013): the hub under test (`KRAKEN_HUB_IMAGE`, default
 * `kraken-hub:dev`) must speak protocol v2 to it, and the previous release (`KRAKEN_HUB_BASELINE_IMAGE`, default
 * `kraken-hub:v1-baseline`) must keep working in legacy mode. Each test skips when its image is not built.
 * [HubConnection]s talk to the container over a real channel with the library's default timing.
 */
class HubCompatTest {
    private val log = LoggerFactory.getLogger(HubCompatTest::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
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
        assertEquals(2, ack.protocolVersion)
        assertEquals(emptyList(), ack.capabilitiesList)
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
        assertEquals(2, instance.protocolVersion)
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

    // --- the previous hub release (protocol v1): legacy mode ---

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
        ).also { connections += it }
    }

    private fun request(type: String) = Request.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setKind(RequestKind.COMMAND)
        .setType(type)
        .setPayload(ByteString.copyFromUtf8("{}"))
        .build()

    private fun payloadReply(id: String, json: String): Reply =
        Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun errorReply(id: String, message: String): Reply = Reply.newBuilder().setRequestId(id)
        .setError(Error.newBuilder().setRequestId(id).setCode(ErrorCode.HANDLER_ERROR).setMessage(message)).build()
}
