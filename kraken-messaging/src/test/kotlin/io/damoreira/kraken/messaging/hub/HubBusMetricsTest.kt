package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.messaging.MessageKind
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.metrics.HubOutcome
import io.damoreira.kraken.messaging.metrics.RecordingMessagingMetrics
import io.damoreira.kraken.messaging.saga.RawOutcome
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.test.InProcessHub
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** `kraken_hub_client_request_duration_seconds` (P9): one observation per unary call through the hub, with its final outcome. */
class HubBusMetricsTest {
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val settings = HubSettings(Duration.ofSeconds(2), Duration.ofMillis(20), Duration.ofMillis(200))
    private val wait = Duration.ofSeconds(5)
    private val metrics = RecordingMessagingMetrics()

    @AfterEach fun tearDown() = runBlocking {
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun connection(instanceId: String, inbound: suspend (io.damoreira.kraken.hub.v1.Request) -> io.damoreira.kraken.hub.v1.Reply) =
        HubConnection({ InProcessChannelBuilder.forName(hub.name).build() }, "svc-$instanceId", instanceId, settings, inbound, scope).also { connections += it }

    private fun bus(connection: HubConnection, registry: HandlerRegistry) =
        HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), Duration.ofSeconds(2), metrics = metrics)

    /** A caller whose commands and queries go to an owner of [owners] through the hub. */
    private suspend fun caller(vararg owners: Any): HubBus {
        val ownerRegistry = HandlerRegistry(resolver).also { r -> owners.forEach { r.register(it) } }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(ownerRegistry, codec), ForwardedIdentityResolver(null), codec)
        val remote = connection("owner") { dispatcher.dispatch(it) }
        remote.register(ownerRegistry.commandTypes(), ownerRegistry.queryTypes())
        remote.start()
        assertTrue(remote.awaitRegistered(wait))
        val caller = connection("caller") { error("unexpected") }
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        return bus(caller, HandlerRegistry(resolver))
    }

    @Test fun `a command and a query through the hub are observed with kind, type and outcome`() = runBlocking {
        val bus = caller(ContextCapturingHandlers())

        bus.send(Ping("hi"))
        assertEquals(Count(1), bus.send(GetCount("abc")))

        assertEquals(
            listOf(MessageKind.COMMAND to "test.Ping", MessageKind.QUERY to "test.GetCount"),
            metrics.hubCalls.map { it.kind to it.type },
        )
        assertTrue(metrics.hubCalls.all { it.outcome == HubOutcome.OK && it.nanos > 0 }, "${metrics.hubCalls}")
    }

    @Test fun `a failed call is observed with the outcome of its failure`() = runBlocking {
        val bus = caller(FailingPingHandler())

        assertFailsWith<RemoteHandlerException> { bus.send(Ping("hi")) }
        assertFailsWith<NoHandlerException> { bus.send(GetCount("abc")) }

        assertEquals(listOf(HubOutcome.HANDLER_ERROR, HubOutcome.NO_HANDLER), metrics.hubCalls.map { it.outcome })
    }

    @Test fun `a local handler is not observed, and a saga's raw command is`() = runBlocking {
        val local = HandlerRegistry(resolver).also { it.register(ContextCapturingHandlers()) }
        val connection = connection("local") { error("unexpected") }
        connection.register(emptySet(), emptySet()) // its handler is answered locally, never through the hub
        connection.start()
        assertTrue(connection.awaitRegistered(wait))
        val localBus = bus(connection, local)
        localBus.send(Ping("here"))
        assertEquals(emptyList(), metrics.hubCalls.toList(), "answered in this instance: no hub call")

        val remoteBus = caller(FailingPingHandler())
        assertTrue(remoteBus.sendRaw("test.Ping", """{"text":"x"}""".toByteArray(), SendOptions()) !is RawOutcome.Success)
        assertEquals(listOf(Triple(MessageKind.COMMAND, "test.Ping", HubOutcome.HANDLER_ERROR)), metrics.hubCalls.map { Triple(it.kind, it.type, it.outcome) })
    }
}
