package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.test.InMemoryBuses
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
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** Remembers the [MessageContext] of every `test.Ping` and `test.GetCount` it handles. */
class ContextCapturingHandlers {
    val contexts = CopyOnWriteArrayList<MessageContext>()

    @CommandHandler fun handle(command: Ping, ctx: MessageContext): Pong {
        contexts += ctx
        return Pong("seen:${command.text}", Instant.EPOCH)
    }

    @QueryHandler fun handle(query: GetCount, ctx: MessageContext): Count {
        contexts += ctx
        return Count(1)
    }
}

class HubBusHeadersTest {
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val settings = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(200),
    )
    private val wait = Duration.ofSeconds(5)
    private val headers = mapOf("idempotency-key" to "saga-1:1001", "saga-id" to "saga-1")

    @AfterEach fun tearDown() = runBlocking {
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun connection(instanceId: String, hubName: String = hub.name, inbound: suspend (io.damoreira.kraken.hub.v1.Request) -> io.damoreira.kraken.hub.v1.Reply) =
        HubConnection({ InProcessChannelBuilder.forName(hubName).build() }, "svc-$instanceId", instanceId, settings, inbound, scope).also { connections += it }

    private fun bus(connection: HubConnection, registry: HandlerRegistry) =
        HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), Duration.ofSeconds(2))

    @Test fun `headers given in SendOptions reach a remote handler's MessageContext`() = runBlocking {
        val owner = ContextCapturingHandlers()
        val ownerRegistry = HandlerRegistry(resolver).also { it.register(owner) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(ownerRegistry, codec), ForwardedIdentityResolver(null), codec)
        val remote = connection("owner") { dispatcher.dispatch(it) }
        remote.register(ownerRegistry.commandTypes(), ownerRegistry.queryTypes())
        remote.start()
        assertTrue(remote.awaitRegistered(wait))
        val caller = connection("caller") { error("unexpected") }
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        val callerBus = bus(caller, HandlerRegistry(resolver))

        callerBus.send(Ping("x"), SendOptions(headers = headers))
        callerBus.send(GetCount("k"), SendOptions(headers = headers))
        callerBus.send(Ping("y"))

        val (command, query, plain) = owner.contexts.toList()
        for (ctx in listOf(command, query)) {
            assertEquals("saga-1:1001", ctx.headers["idempotency-key"])
            assertEquals("saga-1", ctx.headers["saga-id"])
            assertTrue("correlationId" in ctx.headers, "the library's own headers stay: ${ctx.headers}")
        }
        assertEquals(null, plain.headers["idempotency-key"])

        callerBus.send(Ping("z"), SendOptions(headers = headers))
        assertEquals("saga-1:1001", hub.lastRequest()!!.headersMap["idempotency-key"], "on the wire too")
    }

    @Test fun `headers given in SendOptions reach a local handler's MessageContext (short-circuit)`() = runBlocking {
        val owner = ContextCapturingHandlers()
        val registry = HandlerRegistry(resolver).also { it.register(owner) }
        val lonely = connection("lonely", hubName = "no-such-hub-" + UUID.randomUUID()) { error("unexpected") }
        val bus = bus(lonely, registry)

        bus.send(Ping("x"), SendOptions(headers = headers))
        bus.send(GetCount("k"), SendOptions(headers = headers))

        for (ctx in owner.contexts) {
            assertEquals("saga-1:1001", ctx.headers["idempotency-key"])
            assertEquals("saga-1", ctx.headers["saga-id"])
            assertTrue("correlationId" in ctx.headers, "the library's own headers stay: ${ctx.headers}")
        }
        assertEquals(2, owner.contexts.size)
    }

    @Test fun `the in-memory buses hand SendOptions headers to a handler's MessageContext`() = runBlocking {
        val owner = ContextCapturingHandlers()
        val buses = InMemoryBuses().register(owner)

        buses.send(Ping("x"), SendOptions(headers = headers))
        buses.send(GetCount("k"), SendOptions(headers = headers))
        buses.send(Ping("y"))

        assertEquals(listOf("saga-1", "saga-1", null), owner.contexts.map { it.headers["saga-id"] })
    }

    @Test fun `a reserved header name is refused`() {
        for (name in listOf("authorization", "Authorization", "traceparent", "tracestate", "correlation-id", "correlationId")) {
            val e = assertFailsWith<IllegalArgumentException>(name) { SendOptions(headers = mapOf(name to "x")) }
            assertTrue(name in e.message!!, e.message)
        }
        SendOptions(headers = mapOf("idempotency-key" to "k")) // others are fine
    }
}
