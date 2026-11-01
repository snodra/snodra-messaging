package io.damoreira.kraken.messaging.e2e

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.Ports
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.TopologyRequest
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.ExportRows
import io.damoreira.kraken.messaging.fixtures.Row
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.HubSettings
import io.damoreira.kraken.messaging.hub.HubTargets
import io.damoreira.kraken.messaging.hub.InboundRequestDispatcher
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import java.net.ServerSocket
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** The exporting service's handler: `Row(1..count)`, failing before row `failAt` when set; records how its flow ended. */
class ExportHandlers {
    val emitted = AtomicInteger()
    val ended = CompletableDeferred<String>() // completed, cancelled, failed

    @QueryHandler fun handle(query: ExportRows): Flow<Row> = flow {
        try {
            repeat(query.count) { n ->
                if (n == query.failAt) throw IllegalStateException("boom at $n")
                emit(Row(n + 1))
                emitted.incrementAndGet()
            }
            ended.complete("completed")
        } catch (e: CancellationException) {
            ended.complete("cancelled")
            throw e
        } catch (e: Throwable) {
            ended.complete("failed")
            throw e
        }
    }
}

/**
 * Streaming queries (P4b, ADR 0017) against two real `kraken-hub:p4b-hub` containers (`KRAKEN_HUB_P4B_IMAGE`) in the
 * P3 multi-hub configuration: every instance is connected to both hubs. One service streams through the real handler
 * stack (registry, dispatcher, credits), the other collects through `QueryBus.stream`. Skips when the image is not built.
 */
class StreamingQueryEndToEndTest {
    private val log = LoggerFactory.getLogger(StreamingQueryEndToEndTest::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clients = mutableListOf<HubConnections>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val defaults = HubSettings(
        requestTimeout = null,
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(500),
        backoffMax = Duration.ofSeconds(5),
    )
    private val wait = Duration.ofSeconds(15)

    private class Hub(val container: GenericContainer<*>, val id: String, val grpcPort: Int) {
        val target get() = "localhost:$grpcPort"
    }

    @AfterEach fun tearDown() = runBlocking {
        clients.forEach { it.stop() }
        scope.cancel()
        containers.forEach { runCatching { it.stop() } }
    }

    @Test fun `one service streams 1000 rows with credit 8 and the other collects them all in order`() = runBlocking {
        val hubs = startHubs()
        val handlers = ExportHandlers()
        val exporter = exporter(hubs, handlers)
        val bus = caller(hubs)

        val started = System.nanoTime()
        val rows = withTimeout(60_000) { bus.stream(ExportRows(1_000), StreamOptions(initialCredit = 8)).toList() }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        log.info("Streamed {} rows under credit 8 in {} ms", rows.size, elapsed.toMillis())
        assertEquals((1..1_000).map { Row(it) }, rows)
        assertEquals("completed", withTimeout(5_000) { handlers.ended.await() })
        assertEquals(1_000, handlers.emitted.get())
        withTimeout(5_000) { while (exporter.inFlightHandlers.value != 0) delay(20) }
        // Second stream on the other hub (round-robin), same result: both hubs route it.
        assertEquals((1..10).map { Row(it) }, withTimeout(10_000) { bus.stream(ExportRows(10)).toList() })
    }

    @Test fun `a cancel mid-stream cancels the handler, and its permit comes back for the next stream`() = runBlocking {
        val hubs = startHubs()
        val handlers = ExportHandlers()
        val exporter = exporter(hubs, handlers, maxConcurrentHandlers = 1) // one permit: a leaked one would block the next stream
        val bus = caller(hubs)

        val some = withTimeout(30_000) { bus.stream(ExportRows(1_000_000), StreamOptions(initialCredit = 8)).take(50).toList() }
        assertEquals((1..50).map { Row(it) }, some)
        assertEquals("cancelled", withTimeout(10_000) { handlers.ended.await() })
        log.info("Handler emitted {} rows before the cancel reached it", handlers.emitted.get())
        assertTrue(handlers.emitted.get() < 100, "the handler stopped at its window, emitted ${handlers.emitted.get()}")
        withTimeout(5_000) { while (exporter.inFlightHandlers.value != 0) delay(20) }

        // The hub released the stream's permit: the next stream (same instance, one permit) is served.
        val next = withTimeout(10_000) { bus.stream(ExportRows(5)).toList() }
        assertEquals((1..5).map { Row(it) }, next)

        // A failing flow ends the stream with the handler's error after the rows it emitted.
        val seen = mutableListOf<Row>()
        val e = assertFailsWith<RemoteHandlerException> {
            withTimeout(10_000) { bus.stream(ExportRows(10, failAt = 3)).collect { seen += it } }
        }
        assertEquals("HANDLER_ERROR", e.code)
        assertEquals("IllegalStateException: boom at 3", e.message)
        assertEquals((1..3).map { Row(it) }, seen)
        // Nothing is left in flight on either hub.
        for (hub in hubs) {
            withTimeout(10_000) {
                while (topology(hub).servicesList.flatMap { it.instancesList }.any { it.inFlightAsHandler > 0 || it.inFlightAsSender > 0 }) delay(50)
            }
        }
    }

    // --- helpers ---

    private fun startHubs(): List<Hub> {
        Assumptions.assumeTrue(KrakenContainers.hubP4bImageAvailable(), "P4b hub image not available")
        val suffix = UUID.randomUUID().toString().take(8)
        return listOf(start("e2e-p4b-1-$suffix"), start("e2e-p4b-2-$suffix"))
    }

    private fun start(id: String): Hub {
        val grpc = ServerSocket(0).use { it.localPort }
        val web = ServerSocket(0).use { it.localPort }
        val container = KrakenContainers.hubP4b()
            .withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "")
            .withEnv("KRAKEN_HUB_ID", id)
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
        withTimeout(30_000) {
            while (runCatching { topology(hub) }.isFailure) delay(100)
        }
    }

    private suspend fun topology(hub: Hub) = ManagedChannelBuilder.forTarget(hub.target).usePlaintext().build().let { channel ->
        try {
            HubGrpcKt.HubCoroutineStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS).topology(TopologyRequest.getDefaultInstance())
        } finally {
            channel.shutdownNow()
        }
    }

    private fun connections(
        hubs: List<Hub>,
        service: String,
        maxConcurrentHandlers: Int = 256,
        dispatcher: InboundRequestDispatcher? = null,
    ) = HubConnections(
        targets = HubTargets.parse(hubs.joinToString(",") { it.target }),
        channelFactory = HubChannelFactory { target ->
            ManagedChannelBuilder.forTarget(target).usePlaintext()
                .keepAliveTime(10, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build()
        },
        serviceName = service,
        instanceId = "$service-" + UUID.randomUUID(),
        settings = defaults,
        inbound = dispatcher?.let { it::dispatch } ?: { req ->
            Reply.newBuilder().setRequestId(req.id)
                .setError(Error.newBuilder().setRequestId(req.id).setCode(ErrorCode.HANDLER_ERROR).setMessage("unexpected")).build()
        },
        scope = scope,
        maxConcurrentHandlers = maxConcurrentHandlers,
        inboundStream = dispatcher?.let { it::dispatchStream } ?: { req, _ ->
            Error.newBuilder().setRequestId(req.id).setCode(ErrorCode.HANDLER_ERROR).setMessage("unexpected").build()
        },
    ).also { clients += it }

    /** `svc-export`, owning `test.ExportRows` through the real registry and dispatcher, on both hubs. */
    private suspend fun exporter(hubs: List<Hub>, handlers: ExportHandlers, maxConcurrentHandlers: Int = 256): HubConnections {
        val registry = HandlerRegistry(resolver).apply { register(handlers) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(registry, codec), ForwardedIdentityResolver(null), codec)
        val conns = connections(hubs, "svc-export", maxConcurrentHandlers, dispatcher)
        conns.register(emptySet(), registry.exclusiveQueryTypes(), registry.sharedQueryTypes())
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        return conns
    }

    private suspend fun caller(hubs: List<Hub>): HubBus {
        val conns = connections(hubs, "svc-web")
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        withTimeout(10_000) { while (conns.liveHubIds().size < hubs.size) delay(25) }
        val registry = HandlerRegistry(resolver)
        return HubBus(conns, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), null)
    }
}
