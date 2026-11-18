package com.damoreira.snodra.messaging.tracing

import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.messaging.QueryBus
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.events.OutboxEventBus
import com.damoreira.snodra.messaging.events.PublishThenFail
import com.damoreira.snodra.messaging.fixtures.Count
import com.damoreira.snodra.messaging.fixtures.GetCount
import com.damoreira.snodra.messaging.fixtures.Happened
import com.damoreira.snodra.messaging.fixtures.Ping
import com.damoreira.snodra.messaging.fixtures.Pong
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.outboxEntries
import com.damoreira.snodra.messaging.hub.HubBus
import com.damoreira.snodra.messaging.hub.HubConnection
import com.damoreira.snodra.messaging.hub.HubSettings
import com.damoreira.snodra.messaging.hub.InboundRequestDispatcher
import com.damoreira.snodra.messaging.security.ForwardedIdentityResolver
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import com.damoreira.snodra.messaging.test.InProcessHub
import io.grpc.inprocess.InProcessChannelBuilder
import io.micrometer.tracing.CurrentTraceContext
import io.micrometer.tracing.Tracer
import io.micrometer.tracing.propagation.Propagator
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Remembers the [MessageContext] of every `test.GetCount` it answers. */
class CountingHandlers {
    val contexts = CopyOnWriteArrayList<MessageContext>()

    @QueryHandler fun handle(query: GetCount, ctx: MessageContext): Count {
        contexts += ctx
        return Count(1)
    }
}

/** Handles `test.Ping` by asking `test.GetCount` through [bus]: a request sent while handling one. */
class Forwarder(private val bus: () -> QueryBus) {
    @CommandHandler suspend fun handle(command: Ping): Pong {
        bus().send(GetCount(command.text))
        return Pong(command.text, Instant.EPOCH)
    }
}

/**
 * The trace floor: without Micrometer Tracing, a request or an event sent while handling a message carries the handled
 * message's `traceparent`, unchanged, so a trace still follows a message across services. With tracing present the tracer's
 * propagator alone decides, as before.
 */
class TraceFloorTest {
    private val traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val settings = HubSettings(requestTimeout = Duration.ofSeconds(2), backoffMin = Duration.ofMillis(20), backoffMax = Duration.ofMillis(200))
    private val wait = Duration.ofSeconds(5)

    @AfterEach fun tearDown() = runBlocking {
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun connection(instanceId: String, inbound: suspend (Request) -> com.damoreira.snodra.server.v1.Reply) =
        HubConnection({ InProcessChannelBuilder.forName(hub.name).build() }, "svc-$instanceId", instanceId, settings, inbound, scope).also { connections += it }

    @Test fun `a request sent while handling a hub request carries its traceparent`() = runBlocking {
        val counter = CountingHandlers()
        val counterRegistry = HandlerRegistry(resolver).also { it.register(counter) }
        val counterDispatcher = InboundRequestDispatcher(HandlerInvoker(counterRegistry, codec), ForwardedIdentityResolver(null), codec)
        val owner = connection("counter") { counterDispatcher.dispatch(it) }
        owner.register(counterRegistry.commandTypes(), counterRegistry.queryTypes())
        owner.start()
        assertTrue(owner.awaitRegistered(wait))
        val caller = connection("forwarder") { error("unexpected") }
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        val bus = HubBus(caller, HandlerRegistry(resolver), HandlerInvoker(HandlerRegistry(resolver), codec), codec, resolver, true, TraceHeaders(null, null), Duration.ofSeconds(2))
        val forwarderRegistry = HandlerRegistry(resolver).also { it.register(Forwarder { bus }) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(forwarderRegistry, codec), ForwardedIdentityResolver(null), codec)

        dispatcher.dispatch(request("r-1", traceparent))
        assertEquals(traceparent, counter.contexts.single().headers["traceparent"], "forwarded unchanged")

        dispatcher.dispatch(request("r-2", null))
        assertNull(counter.contexts.last().headers["traceparent"], "none came in: none goes out")
    }

    @Test fun `an event published while handling an event carries its traceparent`() {
        val store = MemoryMessageStore()
        val handler = PublishThenFail(OutboxEventBus(store.outbox, codec, resolver, TraceHeaders(null, null))).also { it.failing = false }
        val registry = HandlerRegistry(resolver).also { it.register(handler) }
        val dispatcher = LocalEventDispatcher(registry, HandlerInvoker(registry, codec), store)
        val ctx = MessageContext("e-1", "test.Happened", "corr-1", null, emptySet(), null, mapOf("id" to "e-1", "type" to "test.Happened", "traceparent" to traceparent))
        dispatcher.dispatch("e-1", "test.Happened", codec.encode(Happened("e-1", Instant.now())), ctx)
        assertEquals(traceparent, store.outboxEntries().single().headers["traceparent"])
    }

    @Test fun `only a valid W3C traceparent is forwarded`() = runBlocking {
        val invalid = listOf(
            "00-4BF92F3577B34DA6A3CE929D0E0E4736-00F067AA0BA902B7-01", // upper case
            "00-00000000000000000000000000000000-00f067aa0ba902b7-01", // trace id all zeros
            "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01", // parent id all zeros
            "01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", // another version
            "00-4bf92f3577b34da6a3ce929d0e0e473-00f067aa0ba902b7-01", // short trace id
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",
            "garbage",
        )
        for (value in invalid) {
            val headers = mutableMapOf<String, String>()
            kotlinx.coroutines.withContext(CorrelationContext("corr-1", value)) { TraceHeaders(null, null).inject(headers, coroutineContext) }
            assertNull(headers["traceparent"], value)
        }
        val headers = mutableMapOf<String, String>()
        kotlinx.coroutines.withContext(CorrelationContext("corr-1", traceparent)) { TraceHeaders(null, null).inject(headers, coroutineContext) }
        assertEquals(traceparent, headers["traceparent"])
    }

    @Test fun `with tracing present the propagator decides, as before`() = runBlocking {
        val current = mockk<CurrentTraceContext> { every { context() } returns null }
        val tracer = mockk<Tracer> { every { currentTraceContext() } returns current }
        val headers = mutableMapOf<String, String>()
        kotlinx.coroutines.withContext(CorrelationContext("corr-1", traceparent)) {
            TraceHeaders(mockk<Propagator>(relaxed = true), tracer).inject(headers, coroutineContext)
        }
        assertNull(headers["traceparent"], "no span: the tracer's propagator writes nothing, and nothing is forwarded in its place")
    }

    private fun request(id: String, traceparent: String?): Request = Request.newBuilder()
        .setId(id).setKind(RequestKind.COMMAND).setType("test.Ping").setPayload(ByteString.copyFrom(codec.encode(Ping("k-$id"))))
        .putHeaders("correlationId", "corr-$id")
        .also { if (traceparent != null) it.putHeaders("traceparent", traceparent) }
        .build()
}
