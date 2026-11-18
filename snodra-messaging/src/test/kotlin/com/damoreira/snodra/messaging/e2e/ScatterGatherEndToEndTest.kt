package com.damoreira.snodra.messaging.e2e

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.Ports
import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.HubGrpcKt
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.TopologyRequest
import com.damoreira.snodra.messaging.HubConnectionLostException
import com.damoreira.snodra.messaging.ScatterOptions
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.GetCount
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.hub.HubBus
import com.damoreira.snodra.messaging.hub.HubChannelFactory
import com.damoreira.snodra.messaging.hub.HubConnections
import com.damoreira.snodra.messaging.hub.HubSettings
import com.damoreira.snodra.messaging.hub.HubTargets
import com.damoreira.snodra.messaging.test.SnodraContainers
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * Scatter-gather and shared query types (P4a, ADR 0017) against two containers of the hub image under test
 * (`SNODRA_SERVER_IMAGE`) in the multi-hub configuration: every instance is connected to both hubs. Skips when the
 * image is not built.
 */
class ScatterGatherEndToEndTest {
    private val log = LoggerFactory.getLogger(ScatterGatherEndToEndTest::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clients = mutableListOf<HubConnections>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val defaults = HubSettings(
        requestTimeout = null,
        backoffMin = Duration.ofMillis(500),
        backoffMax = Duration.ofSeconds(5),
    )
    private val wait = Duration.ofSeconds(15)
    private val served = ConcurrentHashMap<String, Int>()

    private class Hub(val container: GenericContainer<*>, val id: String, val grpcPort: Int) {
        val target get() = "localhost:$grpcPort"
    }

    @AfterEach fun tearDown() = runBlocking {
        clients.forEach { it.stop() }
        scope.cancel()
        containers.forEach { runCatching { it.stop() } }
    }

    @Test fun `scatter over three instances of two owner services gets three parts`() = runBlocking {
        val hubs = startHubs()
        owners(hubs)
        val bus = caller(hubs)

        repeat(2) { // one gather per hub (round-robin)
            val result = bus.scatterGather(GetCount("k"))
            log.info("Gather: {}", result)
            assertEquals(
                setOf("svc-x" to 1, "svc-x" to 2, "svc-y" to 3),
                result.parts.map { it.service to it.result!!.value }.toSet(),
            )
            assertEquals(3, result.parts.map { it.instance }.toSet().size)
            assertEquals(0, result.failed)
            assertEquals(0, result.timedOut)
        }
        val two = bus.scatterGather(GetCount("k"), ScatterOptions(maxResults = 2))
        assertEquals(2, two.parts.size)
    }

    @Test fun `a unary query on the shared type round-robins over every owner`() = runBlocking {
        val hubs = startHubs()
        val instances = owners(hubs)
        val bus = caller(hubs)
        served.clear()
        val answers = (1..6).map { bus.send(GetCount("k")).value }
        log.info("Unary answers {}, served {}", answers, served)
        assertEquals(instances.toSet(), served.keys, "every owner instance served some")
        assertEquals(6, served.values.sum())
    }

    @Test fun `the hub carrying a gather is killed mid-flight - HubConnectionLost, the next gather succeeds on the other`() = runBlocking {
        val hubs = startHubs()
        val slow = AtomicBoolean(true)
        val started = CompletableDeferred<Unit>()
        owners(hubs) { if (slow.get()) { started.complete(Unit); delay(5_000) } }
        val conns = callerConnections(hubs)
        val carrying = CompletableDeferred<String>()
        conns.beforeWrite = { connection -> carrying.complete(connection.target) }
        val bus = bus(conns)

        val gather = async { runCatching { bus.scatterGather(GetCount("k"), ScatterOptions(timeout = Duration.ofSeconds(10))) } }
        val target = withTimeout(5_000) { carrying.await() }
        withTimeout(5_000) { started.await() } // the targets have it: the gather is in flight
        conns.beforeWrite = null
        val victim = hubs.single { it.target == target }
        // Killed, not stopped: a graceful stop may still send HANDLER_DISCONNECTED parts and an end first.
        withContext(Dispatchers.IO) { DockerClientFactory.instance().client().killContainerCmd(victim.container.containerId).exec() }
        val outcome = withTimeout(5_000) { gather.await() }
        log.info("Gather on the killed hub {}: {}", victim.id, outcome)
        assertIs<HubConnectionLostException>(outcome.exceptionOrNull())

        slow.set(false)
        val survivor = hubs.single { it !== victim }
        withTimeout(10_000) { while (conns.liveHubIds() != listOf(survivor.id)) delay(25) }
        val next = bus.scatterGather(GetCount("k"))
        assertEquals(3, next.parts.size)
        assertEquals(0, next.failed)
    }

    // --- helpers ---

    private fun startHubs(): List<Hub> {
        Assumptions.assumeTrue(SnodraContainers.hubImageAvailable(), "hub image under test not built")
        val suffix = UUID.randomUUID().toString().take(8)
        return listOf(start("e2e-p4a-1-$suffix"), start("e2e-p4a-2-$suffix"))
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
        return Hub(container, id, grpc).also(::awaitReachable)
    }

    /** Waits until the fixed host port reaches the hub (port forwarding may lag the "listening" log line). */
    private fun awaitReachable(hub: Hub) = runBlocking {
        val channel = ManagedChannelBuilder.forTarget(hub.target).usePlaintext().build()
        try {
            withTimeout(30_000) {
                while (true) {
                    val ok = runCatching {
                        HubGrpcKt.HubCoroutineStub(channel).withDeadlineAfter(1, TimeUnit.SECONDS).topology(TopologyRequest.getDefaultInstance())
                    }.isSuccess
                    if (ok) break
                    delay(100)
                }
            }
        } finally {
            channel.shutdownNow()
        }
    }

    private fun connections(hubs: List<Hub>, service: String, inbound: suspend (Request) -> Reply) = HubConnections(
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
    ).also { clients += it }

    /**
     * svc-x (two instances, answering 1 and 2) and svc-y (one instance, answering 3), all owning `test.GetCount` as a
     * shared type on both hubs; [before] runs before each answer. Returns the instance ids.
     */
    private suspend fun owners(hubs: List<Hub>, before: suspend () -> Unit = {}): List<String> =
        listOf("svc-x" to 1, "svc-x" to 2, "svc-y" to 3).map { (service, value) ->
            lateinit var self: HubConnections
            val conns = connections(hubs, service) { req ->
                before()
                served.merge(self.instanceId, 1, Int::plus)
                Reply.newBuilder().setRequestId(req.id).setPayload(ByteString.copyFromUtf8("{\"value\":$value}")).build()
            }
            self = conns
            conns.register(emptySet(), emptySet(), setOf("test.GetCount"))
            conns.start()
            assertTrue(conns.awaitRegistered(wait))
            conns.instanceId
        }

    private suspend fun callerConnections(hubs: List<Hub>): HubConnections {
        val conns = connections(hubs, "svc-caller") { req ->
            Reply.newBuilder().setRequestId(req.id)
                .setError(Error.newBuilder().setRequestId(req.id).setCode(ErrorCode.HANDLER_ERROR).setMessage("unexpected")).build()
        }
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        withTimeout(10_000) { while (conns.liveHubIds().size < hubs.size) delay(25) }
        return conns
    }

    private suspend fun caller(hubs: List<Hub>): HubBus = bus(callerConnections(hubs))

    private fun bus(conns: HubConnections): HubBus {
        val resolver = MessageTypeResolver("test")
        val codec = JsonCodec()
        val registry = HandlerRegistry(resolver)
        return HubBus(conns, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), null)
    }
}
