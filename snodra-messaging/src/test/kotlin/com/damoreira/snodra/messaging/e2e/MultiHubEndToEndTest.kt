package com.damoreira.snodra.messaging.e2e

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.PortBinding
import com.github.dockerjava.api.model.Ports
import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.HubGrpcKt
import com.damoreira.snodra.server.v1.TopologyRequest
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.messaging.HubConnectionLostException
import com.damoreira.snodra.messaging.hub.HubChannelFactory
import com.damoreira.snodra.messaging.hub.HubConnections
import com.damoreira.snodra.messaging.hub.HubSettings
import com.damoreira.snodra.messaging.hub.HubTargets
import com.damoreira.snodra.messaging.test.SnodraContainers
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.net.ServerSocket
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.Collections
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Multi-hub HA against two containers of the hub image under test (`SNODRA_SERVER_IMAGE`): every instance
 * registers on both hubs, reports both ids, spreads unkeyed requests over both and survives losing one. Each hub gets
 * fixed host ports so a stopped container comes back on the same address. Skips when the image is not built.
 */
class MultiHubEndToEndTest {
    private val log = LoggerFactory.getLogger(MultiHubEndToEndTest::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clients = mutableListOf<HubConnections>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()
    private val json = JsonMapper.builder().build()
    private val defaults = HubSettings(
        requestTimeout = null,
        backoffMin = Duration.ofMillis(500),
        backoffMax = Duration.ofSeconds(5),
    )
    private val wait = Duration.ofSeconds(15)

    private class Hub(val container: GenericContainer<*>, val id: String, val grpcPort: Int, val httpPort: Int) {
        val target get() = "localhost:$grpcPort"
    }

    @AfterEach fun tearDown() = runBlocking {
        clients.forEach { it.stop() }
        scope.cancel()
        containers.forEach { runCatching { it.stop() } }
    }

    @Test fun `both hubs list the instance with both hub ids within 6 s`() = runBlocking {
        val (one, two) = startHubs()
        val started = System.nanoTime()
        val b = client(listOf(one, two), "svc-b") { req -> reply(req) }
        b.register(setOf("t.Ping"), emptySet())
        b.start()

        withTimeout(10_000) {
            while (listOf(one, two).any { reportedHubIds(it, "svc-b", b.instanceId) != setOf(one.id, two.id) }) delay(100)
        }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        log.info("Both hubs list the instance with both hub ids {} ms after start", elapsed.toMillis())
        assertTrue(elapsed <= Duration.ofSeconds(6), "took $elapsed")
        assertEquals(listOf(one.id, two.id), b.liveHubIds())
        assertTrue(b.connections().all { it.statusReportsWritten > 0 })
        for (hub in listOf(one, two)) assertTrue(instance(hub, "svc-b", b.instanceId)!!["statusAgeMs"].asLong() >= 0)
    }

    @Test fun `50 unkeyed queries spread over both hubs`() = runBlocking {
        val (one, two) = startHubs()
        val b = client(listOf(one, two), "svc-b") { req -> reply(req) }
        b.register(setOf("t.Ping"), emptySet())
        b.start()
        val a = client(listOf(one, two), "svc-a")
        a.start()
        assertTrue(b.awaitRegistered(wait) && a.awaitRegistered(wait))

        repeat(50) {
            val answer = a.send(request())
            assertTrue(answer.hasReply() && !answer.reply.hasError(), answer.toString())
        }
        val counts = withTimeout(10_000) {
            var c: List<Long>
            do {
                c = listOf(one, two).map { graphRequests(it) }
                if (c.sum() < 50) delay(100)
            } while (c.sum() < 50)
            c
        }
        log.info("50 queries: {} on {}, {} on {}", counts[0], one.id, counts[1], two.id)
        assertEquals(listOf(25L, 25L), counts)
    }

    @Test fun `a hub stopped mid-traffic - the other one takes over, and the stopped one is back within 5 s`() = runBlocking {
        val (one, two) = startHubs()
        val b = client(listOf(one, two), "svc-b") { req -> delay(20); reply(req) }
        b.register(setOf("t.Ping"), emptySet())
        b.start()
        val a = client(listOf(one, two), "svc-a")
        a.start()
        assertTrue(b.awaitRegistered(wait) && a.awaitRegistered(wait))

        class Outcome(val error: Throwable?, val tookMs: Long, val afterStop: Boolean)
        val outcomes = Collections.synchronizedList(mutableListOf<Outcome>())
        val running = AtomicBoolean(true)
        val stopped = AtomicBoolean(false)
        val workers = (1..8).map {
            scope.launch {
                while (running.get()) {
                    val start = System.nanoTime()
                    val result = runCatching { a.send(request(timeoutMs = 3_000)) }
                    val error = result.exceptionOrNull()
                        ?: result.getOrNull()!!.takeIf { !it.hasReply() || it.reply.hasError() }?.let { IllegalStateException(it.toString()) }
                    outcomes += Outcome(error, (System.nanoTime() - start) / 1_000_000, stopped.get())
                }
            }
        }
        delay(1_000)
        withContext(Dispatchers.IO) {
            DockerClientFactory.instance().client().stopContainerCmd(two.container.containerId).withTimeout(0).exec()
        }
        stopped.set(true)
        delay(2_000)
        running.set(false)
        workers.forEach { it.join() }

        val failures = outcomes.mapNotNull { it.error }
        log.info("{} calls, {} failed: {}", outcomes.size, failures.size, failures.map { it.message })
        // Only calls already written to the stopped hub fail: HubConnectionLost when its stream to us broke first, or the
        // hub's own HANDLER_DISCONNECTED when its shutdown closed the handler's stream first. Both: outcome unknown.
        failures.forEach {
            assertTrue(it is HubConnectionLostException || "HANDLER_DISCONNECTED" in it.message.orEmpty(), it.toString())
        }
        assertTrue(failures.size <= 8, "at most the 8 calls in flight are lost, lost ${failures.size}")
        assertTrue(outcomes.count { it.afterStop && it.error == null } >= 20, "the other hub kept serving")
        val slowest = outcomes.maxOf { it.tookMs }
        assertTrue(slowest < 4_500, "a call took $slowest ms (request timeout 3 s + 1 s local guard margin)")
        assertEquals(listOf(one.id), a.liveHubIds())

        withContext(Dispatchers.IO) { DockerClientFactory.instance().client().startContainerCmd(two.container.containerId).exec() }
        val listening = withTimeout(30_000) {
            while (two.container.logs.split("hub listening").size - 1 < 2) delay(25)
            System.nanoTime()
        }
        withTimeout(15_000) {
            while (a.liveHubIds() != listOf(one.id, two.id) || b.liveHubIds() != listOf(one.id, two.id) || !b.awaitRegistered(Duration.ofMillis(50))) {
                delay(25)
            }
        }
        val back = Duration.ofNanos(System.nanoTime() - listening)
        log.info("Both instances re-registered on {} {} ms after it was listening again", two.id, back.toMillis())
        // The backoff never waits more than 5 s; allow 1 s for the connect and the handshake (as the single-hub test).
        assertTrue(back <= Duration.ofSeconds(6), "re-registered only after $back")
        assertTrue(a.send(request()).hasReply())
    }

    @Test fun `a stopped hub's permits move to the other one within 2 s, and nothing is revoked when it returns`() = runBlocking {
        val (one, two) = startHubs()
        val b = client(listOf(one, two), "svc-b", maxConcurrentHandlers = 8) { req -> reply(req) }
        b.register(setOf("t.Ping"), emptySet())
        b.start()
        // max 8 over N = 2 hubs: ceil(8 / 2) = 4 initial permits on each.
        withTimeout(15_000) { while (listOf(one, two).any { permitsAvailable(it, "svc-b", b.instanceId) != 4 }) delay(50) }

        withContext(Dispatchers.IO) {
            DockerClientFactory.instance().client().stopContainerCmd(two.container.containerId).withTimeout(0).exec()
        }
        val stopped = System.nanoTime()
        // N 2 -> 1: Permits{count = 4, grant = 8} raises hub-1's cap to 8 and fills it.
        withTimeout(10_000) { while (permitsAvailable(one, "svc-b", b.instanceId) != 8) delay(25) }
        val toppedUp = Duration.ofNanos(System.nanoTime() - stopped)
        log.info("{} shows 8 permits {} ms after {} was stopped", one.id, toppedUp.toMillis(), two.id)
        assertTrue(toppedUp <= Duration.ofMillis(2_500), "topped up only after $toppedUp")

        withContext(Dispatchers.IO) { DockerClientFactory.instance().client().startContainerCmd(two.container.containerId).exec() }
        withTimeout(30_000) { while (two.container.logs.split("hub listening").size - 1 < 2) delay(25) }
        // N 1 -> 2: the returning hub gets ceil(8 / 2) = 4 on its new stream; hub-1 keeps its 8.
        withTimeout(15_000) { while (permitsAvailable(two, "svc-b", b.instanceId) != 4) delay(50) }
        delay(500)
        assertEquals(8, permitsAvailable(one, "svc-b", b.instanceId), "nothing revoked")
        assertEquals(listOf(one.id, two.id), b.liveHubIds())
    }

    @Test fun `two targets of one hub - the instance stays registered and served without interruption`() = runBlocking {
        // The real hub keeps one session per instance id: a second stream with the same id replaces the first. Two
        // targets of one hub (an alias, or the A and AAAA records of one name) must settle on one stream and stay there.
        Assumptions.assumeTrue(SnodraContainers.hubImageAvailable(), "hub image under test not built")
        val hub = start("e2e-hub-alias-" + UUID.randomUUID().toString().take(8))
        val aliasDialed = AtomicBoolean()
        val b = HubConnections(
            targets = HubTargets.parse("127.0.0.1:${hub.grpcPort},localhost:${hub.grpcPort}"),
            channelFactory = HubChannelFactory { target ->
                // The alias dials once the first stream is up, so its Register replaces an established session.
                if (target.startsWith("localhost") && aliasDialed.compareAndSet(false, true)) Thread.sleep(500)
                ManagedChannelBuilder.forTarget(target).usePlaintext().build()
            },
            serviceName = "svc-b",
            instanceId = "svc-b-" + UUID.randomUUID(),
            settings = defaults.copy(backoffMin = Duration.ofMillis(100)),
            inbound = { req -> reply(req) },
            scope = scope,
        ).also { clients += it }
        b.register(setOf("t.Ping"), emptySet())
        b.start()
        val a = client(listOf(hub), "svc-a")
        a.start()
        assertTrue(a.awaitRegistered(wait))
        withTimeout(15_000) { while (b.connections().size != 1 || !b.awaitRegistered(Duration.ofMillis(50))) delay(50) }
        delay(1_000) // the parked duplicate's close reaches the hub; from here on nothing may change

        val absent = mutableListOf<Long>()
        var calls = 0
        val failures = mutableListOf<String>()
        val until = System.nanoTime() + Duration.ofSeconds(10).toNanos()
        while (System.nanoTime() < until) {
            if (instance(hub, "svc-b", b.instanceId) == null) absent += System.currentTimeMillis()
            val answer = runCatching { a.send(request(timeoutMs = 2_000)) }
            val failure = answer.exceptionOrNull()?.toString()
                ?: answer.getOrNull()!!.takeIf { !it.hasReply() || it.reply.hasError() }?.toString()
            failure?.let { failures += it }
            calls++
            delay(50)
        }
        log.info("Alias test: {} polls/calls, absent {} times, {} failures", calls, absent.size, failures.size)
        assertTrue(absent.isEmpty(), "svc-b missing from /api/v1/topology at ${absent.size} of $calls polls")
        assertTrue(failures.isEmpty(), "failed calls: $failures")
        assertEquals(1, b.connections().size)
        assertEquals(listOf(hub.id), b.liveHubIds())
    }

    // --- helpers ---

    private fun startHubs(): Pair<Hub, Hub> {
        Assumptions.assumeTrue(SnodraContainers.hubImageAvailable(), "hub image under test not built")
        val suffix = UUID.randomUUID().toString().take(8)
        return start("e2e-hub-1-$suffix") to start("e2e-hub-2-$suffix")
    }

    private fun start(id: String): Hub {
        val grpc = ServerSocket(0).use { it.localPort }
        val web = ServerSocket(0).use { it.localPort }
        val container = SnodraContainers.hub()
            .withEnv("SNODRA_SERVER_OTLP_ENDPOINT", "")
            .withEnv("SNODRA_SERVER_ID", id)
            .withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withPortBindings(
                    Ports().apply {
                        bind(ExposedPort.tcp(7000), Ports.Binding.bindPort(grpc))
                        bind(ExposedPort.tcp(7001), Ports.Binding.bindPort(web))
                    },
                )
            }
        containers += container
        container.start()
        return Hub(container, id, grpc, web).also(::awaitReachable)
    }

    /**
     * Waits until both fixed host ports reach the hub: the hub logs "listening" before the Docker host's port forwarding
     * of a fixed binding is always in place, and a client that dials too early backs off for seconds.
     */
    private fun awaitReachable(hub: Hub) = runBlocking {
        val channel = ManagedChannelBuilder.forTarget(hub.target).usePlaintext().build()
        try {
            withTimeout(30_000) {
                while (true) {
                    val ok = runCatching {
                        get(hub, "/api/v1/topology")
                        HubGrpcKt.HubCoroutineStub(channel).withDeadlineAfter(1, TimeUnit.SECONDS)
                            .topology(TopologyRequest.getDefaultInstance())
                    }.isSuccess
                    if (ok) break
                    delay(100)
                }
            }
        } finally {
            channel.shutdownNow()
        }
    }

    private fun client(
        hubs: List<Hub>,
        service: String,
        maxConcurrentHandlers: Int = 256,
        inbound: suspend (Request) -> Reply = { req -> errorReply(req.id) },
    ) =
        HubConnections(
            targets = HubTargets.parse(hubs.joinToString(",") { it.target }),
            channelFactory = HubChannelFactory { target ->
                ManagedChannelBuilder.forTarget(target).usePlaintext()
                    .keepAliveTime(10, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build()
            },
            serviceName = service,
            instanceId = "$service-" + UUID.randomUUID(),
            settings = defaults,
            inbound = inbound,
            scope = scope,
            maxConcurrentHandlers = maxConcurrentHandlers,
        ).also { clients += it }

    private suspend fun get(hub: Hub, path: String): JsonNode = withContext(Dispatchers.IO) {
        val response = http.send(
            HttpRequest.newBuilder(URI.create("http://localhost:${hub.httpPort}$path")).timeout(Duration.ofSeconds(3)).build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        check(response.statusCode() == 200) { "GET $path on ${hub.id}: ${response.statusCode()}" }
        json.readTree(response.body())
    }

    private suspend fun instance(hub: Hub, service: String, instanceId: String): JsonNode? =
        get(hub, "/api/v1/topology")["services"]
            .firstOrNull { it["serviceName"].asString() == service }
            ?.get("instances")?.firstOrNull { it["instanceId"].asString() == instanceId }

    /** `permitsAvailable` of [instanceId] on [hub]; null while it is not listed (or the HTTP port is not up yet). */
    private suspend fun permitsAvailable(hub: Hub, service: String, instanceId: String): Int? =
        runCatching { instance(hub, service, instanceId) }.getOrNull()?.get("permitsAvailable")?.asInt()

    /** The ids [instanceId] last reported to [hub]; null while it is not listed (or the HTTP port is not up yet). */
    private suspend fun reportedHubIds(hub: Hub, service: String, instanceId: String): Set<String>? =
        runCatching { instance(hub, service, instanceId) }.getOrNull()?.get("connectedHubIds")?.let { ids -> (0 until ids.size()).mapTo(HashSet()) { ids[it].asString() } }

    private suspend fun graphRequests(hub: Hub): Long = get(hub, "/api/v1/graph")["totals"]["requests"].asLong()

    private fun request(timeoutMs: Int = 0) = Request.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setKind(RequestKind.QUERY)
        .setType("t.Ping")
        .setPayload(ByteString.copyFromUtf8("{}"))
        .setTimeoutMs(timeoutMs)
        .build()

    private fun reply(req: Request): Reply =
        Reply.newBuilder().setRequestId(req.id).setPayload(ByteString.copyFromUtf8("{\"ok\":true}")).build()

    private fun errorReply(id: String): Reply = Reply.newBuilder().setRequestId(id)
        .setError(Error.newBuilder().setRequestId(id).setCode(ErrorCode.HANDLER_ERROR).setMessage("unexpected")).build()
}
