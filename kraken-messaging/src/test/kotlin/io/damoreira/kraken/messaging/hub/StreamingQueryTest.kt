package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Flow as HubFlow
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.hub.v1.RequestMode
import io.damoreira.kraken.hub.v1.ServerMessage
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.ScatterOptions
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.UnsupportedByHubException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.ExportRows
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Row
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.test.InProcessHub
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** What a streaming handler of `test.ExportRows` did: parts handed to the hub, and how its flow ended. */
class StreamerState {
    val emitted = AtomicInteger()
    val started = CompletableDeferred<Unit>()
    val ended = CompletableDeferred<String>() // completed, cancelled, failed

    fun rows(query: ExportRows, hang: Boolean = false): Flow<Row> = flow {
        started.complete(Unit)
        try {
            repeat(query.count) { n ->
                if (n == query.failAt) throw IllegalStateException("boom at $n")
                emit(Row(n + 1)) // returns once the part went to the hub: the library suspends it while the credit is 0
                emitted.incrementAndGet()
            }
            if (hang) awaitCancellation()
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

class RowsStreamer(val state: StreamerState = StreamerState()) {
    @QueryHandler fun handle(query: ExportRows): Flow<Row> = state.rows(query)
}

/** Emits its rows, then never ends. */
class HangingRowsStreamer(val state: StreamerState = StreamerState()) {
    @QueryHandler fun handle(query: ExportRows): Flow<Row> = state.rows(query, hang = true)
}

class SharedRowsStreamer(val state: StreamerState = StreamerState()) {
    @QueryHandler(shared = true) fun handle(query: ExportRows): Flow<Row> = state.rows(query)
}

/** Streaming queries (P4b, ADR 0017) over in-process hubs: the handler side, the sender side and their meeting. */
class StreamingQueryTest {
    private val hubs = mutableListOf<InProcessHub>()
    private val hub = hub()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val multi = mutableListOf<HubConnections>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val registry = HandlerRegistry(resolver)
    private val invoker = HandlerInvoker(registry, codec)
    private val settings = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(200),
    )
    private val wait = Duration.ofSeconds(5)

    @AfterEach fun tearDown() = runBlocking {
        SecurityContextHolder.clearContext()
        connections.forEach { it.stop() }
        multi.forEach { it.stop() }
        scope.cancel()
        hubs.forEach { it.close() }
    }

    private fun hub(configure: InProcessHub.() -> Unit = {}) = InProcessHub().apply(configure).start().also { hubs += it }

    private fun connection(
        instanceId: String,
        service: String = "svc-$instanceId",
        hubName: String = hub.name,
        maxConcurrentHandlers: Int = 256,
        inboundStream: StreamingInbound? = null,
        inbound: suspend (Request) -> Reply = { req -> error(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
    ) = HubConnection(
        channelFactory = { InProcessChannelBuilder.forName(hubName).build() },
        serviceName = service,
        instanceId = instanceId,
        settings = settings,
        inbound = inbound,
        scope = scope,
        maxConcurrentHandlers = maxConcurrentHandlers,
        inboundStream = inboundStream ?: { req, _ -> Error.newBuilder().setRequestId(req.id).setCode(ErrorCode.HANDLER_ERROR).setMessage("unexpected").build() },
    ).also { connections += it }

    private fun bus(transport: HubTransport, retry: RetryPolicy? = null, requestTimeout: Duration? = null) =
        HubBus(transport, registry, invoker, codec, resolver, true, TraceHeaders(null, null), requestTimeout, emptyMap(), retry)

    /** An instance of [service] serving the handlers of [beans] through the real registry and dispatcher. */
    private suspend fun owner(
        service: String,
        instanceId: String,
        vararg beans: Any,
        hubName: String = hub.name,
        maxConcurrentHandlers: Int = 256,
    ): HubConnection {
        val ownerRegistry = HandlerRegistry(resolver)
        beans.forEach { ownerRegistry.register(it) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(ownerRegistry, codec), ForwardedIdentityResolver(null), codec)
        return connection(instanceId, service, hubName, maxConcurrentHandlers, inboundStream = dispatcher::dispatchStream, inbound = dispatcher::dispatch).apply {
            register(emptySet(), ownerRegistry.exclusiveQueryTypes(), ownerRegistry.sharedQueryTypes())
            start()
            assertTrue(awaitRegistered(wait))
        }
    }

    private suspend fun caller(hubName: String = hub.name, retry: RetryPolicy? = null): HubBus {
        val conn = connection("caller-" + UUID.randomUUID().toString().take(6), "svc-caller", hubName)
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        return bus(conn, retry)
    }

    private fun callerId() = connections.first { it.instanceId.startsWith("caller-") }.instanceId

    private suspend fun awaitUntil(ms: Long = 5_000, condition: () -> Boolean) = withTimeout(ms) { while (!condition()) delay(10) }

    // --- the handler side on the wire ---

    @Test fun `the handler streams under the sender's credit, stalls at zero and resumes on Flow, seq from 1, then ends`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state))
        val sender = RawSender("s-1")
        sender.next() // ack
        sender.send(streamRequest("s1", initialCredit = 2))
        val parts = mutableListOf<io.damoreira.kraken.hub.v1.ReplyPart>()
        repeat(2) { parts += sender.next().part }
        assertEquals(listOf(1L, 2L), parts.map { it.seq })
        assertEquals(listOf(Row(1), Row(2)), parts.map { codec.decode(it.payload.toByteArray(), Row::class) })
        assertEquals("x-1", parts.first().responderInstance)
        delay(200)
        assertEquals(2, state.emitted.get()) // no credit: the flow is suspended inside emit
        assertEquals(false, state.ended.isCompleted)

        sender.send(ClientMessage.newBuilder().setFlow(HubFlow.newBuilder().setRequestId("s1").setCredits(2)).build())
        repeat(2) { parts += sender.next().part }
        assertEquals(listOf(3L, 4L), parts.takeLast(2).map { it.seq })
        delay(100)
        assertEquals(4, state.emitted.get())
        sender.send(ClientMessage.newBuilder().setFlow(HubFlow.newBuilder().setRequestId("s1").setCredits(10)).build())
        assertEquals(5L, sender.next().part.seq)
        val end = sender.next().end
        assertEquals("s1", end.requestId)
        assertEquals(5, end.parts)
        assertEquals(false, end.hasError())
        assertEquals("completed", state.ended.await())
        assertNull(withTimeoutOrNull(200) { sender.inbox.receive() })
        assertEquals(0, hub.openStreams())
    }

    @Test fun `register declares streaming and the connection reports the capability`() = runBlocking {
        val conn = connection("a")
        conn.register(emptySet(), setOf("test.ExportRows"))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        assertEquals(listOf("flow-control", "instance-status", "scatter-gather", "streaming"), hub.lastRegisterFrom("a")!!.capabilitiesList)
        assertTrue(conn.supportsStreaming)
        hub.streaming = false
        val old = connection("b")
        old.start()
        assertTrue(old.awaitRegistered(wait))
        assertEquals(false, old.supportsStreaming)
    }

    // --- the sender side ---

    @Test fun `stream collects every part in order and refills the credit at half the window`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state))
        val bus = caller()
        val rows = withTimeout(10_000) { bus.stream(ExportRows(100), StreamOptions(initialCredit = 8)).toList() }
        assertEquals((1..100).map { Row(it) }, rows)
        assertEquals("completed", state.ended.await())
        awaitUntil { hub.flowsFrom(callerId()).size == 25 } // one Flow{4} per 4 parts consumed
        assertTrue(hub.flowsFrom(callerId()).all { it == 4 }, hub.flowsFrom(callerId()).toString())
        assertEquals(0, hub.openStreams())
        awaitUntil { hub.permitsAvailable("x-1") == 256 } // the stream's permit came back
    }

    @Test fun `a slow collector stalls the handler at the window and the refill lets it go on`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state))
        val bus = caller()
        val gate = CompletableDeferred<Unit>()
        val seen = CopyOnWriteArrayList<Row>()
        val collecting = async {
            bus.stream(ExportRows(20), StreamOptions(initialCredit = 4)).collect { row ->
                if (row.n == 3) gate.await() // consumed 2: one refill of 2 went out, the handler may send 2 more
                seen += row
            }
        }
        awaitUntil { state.emitted.get() == 6 }
        delay(300)
        assertEquals(6, state.emitted.get()) // 4 initial + 2 refilled, then no credit
        assertEquals(listOf(Row(1), Row(2)), seen.toList())
        gate.complete(Unit)
        withTimeout(5_000) { collecting.await() }
        assertEquals((1..20).map { Row(it) }, seen.toList())
        assertEquals("completed", state.ended.await())
    }

    @Test fun `StreamOptions become the request fields`() = runBlocking {
        owner("svc-x", "x-1", RowsStreamer())
        val bus = caller()
        bus.stream(ExportRows(1), StreamOptions(timeout = Duration.ofMillis(1_234), maxResults = 5, priority = 7, initialCredit = 3)).toList()
        val sent = hub.lastRequest()!!
        assertEquals(RequestMode.STREAMING, sent.mode)
        assertEquals(RequestKind.QUERY, sent.kind)
        assertEquals("test.ExportRows", sent.type)
        assertEquals(1_234, sent.timeoutMs)
        assertEquals(5, sent.maxResults)
        assertEquals(7, sent.priority)
        assertEquals(3, sent.initialCredit)
        assertEquals("", sent.routingKey)

        bus.stream(ExportRows(1)).toList()
        val defaults = hub.lastRequest()!!
        assertEquals(0, defaults.timeoutMs)
        assertEquals(0, defaults.maxResults)
        assertEquals(0, defaults.priority)
        assertEquals(64, defaults.initialCredit)
        assertFailsWith<IllegalArgumentException> { StreamOptions(initialCredit = 0) }
        assertFailsWith<IllegalArgumentException> { StreamOptions(maxResults = -1) }
        Unit
    }

    @Test fun `maxResults ends the stream after that many parts and cancels the handler`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state))
        val rows = withTimeout(5_000) { caller().stream(ExportRows(1_000), StreamOptions(maxResults = 3, initialCredit = 2)).toList() }
        assertEquals(listOf(Row(1), Row(2), Row(3)), rows)
        assertEquals("cancelled", withTimeout(2_000) { state.ended.await() })
        assertEquals(0, hub.openStreams())
    }

    @Test fun `the idle timeout is a RemoteHandlerException with code TIMEOUT and cancels the handler`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", HangingRowsStreamer(state))
        val seen = mutableListOf<Row>()
        val started = System.nanoTime()
        val e = assertFailsWith<RemoteHandlerException> {
            caller().stream(ExportRows(2), StreamOptions(timeout = Duration.ofMillis(400))).collect { seen += it }
        }
        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofSeconds(2))
        assertEquals("TIMEOUT", e.code)
        assertEquals("test.ExportRows", e.type)
        assertEquals(listOf(Row(1), Row(2)), seen)
        assertEquals("cancelled", withTimeout(2_000) { state.ended.await() })
    }

    @Test fun `an exception in the handler's flow fails the stream after the parts it emitted`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state))
        val seen = mutableListOf<Row>()
        val e = assertFailsWith<RemoteHandlerException> { caller().stream(ExportRows(5, failAt = 2)).collect { seen += it } }
        assertEquals("HANDLER_ERROR", e.code)
        assertEquals("IllegalStateException: boom at 2", e.message)
        assertEquals(listOf(Row(1), Row(2)), seen)
        assertEquals("failed", state.ended.await())
        assertEquals(0, hub.openStreams())
    }

    @Test fun `cancelling the collector sends Cancel and cancels the handler`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state))
        val bus = caller()
        val rows = withTimeout(5_000) { bus.stream(ExportRows(1_000), StreamOptions(initialCredit = 4)).take(3).toList() }
        assertEquals(listOf(Row(1), Row(2), Row(3)), rows)
        assertEquals("cancelled", withTimeout(2_000) { state.ended.await() })
        awaitUntil { hub.cancelsFrom(callerId()).size == 1 }
        assertEquals("collector cancelled", hub.cancelsFrom(callerId()).single().reason)
        assertEquals(0, hub.openStreams())
    }

    @Test fun `an exception in the collector sends Cancel and cancels the handler`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state))
        val bus = caller()
        val e = assertFailsWith<IllegalStateException> {
            bus.stream(ExportRows(1_000), StreamOptions(initialCredit = 4)).collect { row -> if (row.n == 2) throw IllegalStateException("stop") }
        }
        assertEquals("stop", e.message)
        assertEquals("cancelled", withTimeout(2_000) { state.ended.await() })
        awaitUntil { hub.cancelsFrom(callerId()).size == 1 }
        assertTrue(hub.cancelsFrom(callerId()).single().reason.contains("stop"), hub.cancelsFrom(callerId()).toString())
        assertEquals(0, hub.openStreams())
    }

    @Test fun `a hub Cancel for a unary request cancels its handler job and pays the permit back`() = runBlocking {
        val cancelled = CompletableDeferred<Boolean>()
        val slowStarted = CompletableDeferred<Unit>()
        val fast = connection("x-1", "svc-x") { req -> count(req.id, 1) }
        fast.register(emptySet(), emptySet(), setOf("test.GetCount"))
        fast.start()
        val slow = connection("y-1", "svc-y", maxConcurrentHandlers = 4) { req ->
            slowStarted.complete(Unit)
            try {
                delay(10_000)
            } catch (e: CancellationException) {
                cancelled.complete(true)
                throw e
            }
            count(req.id, 2)
        }
        slow.register(emptySet(), emptySet(), setOf("test.GetCount"))
        slow.start()
        assertTrue(fast.awaitRegistered(wait) && slow.awaitRegistered(wait))
        val result = caller().scatterGather(GetCount("k"), ScatterOptions(maxResults = 1))
        assertEquals(listOf(Count(1)), result.results)
        withTimeout(2_000) { slowStarted.await() }
        assertTrue(withTimeout(2_000) { cancelled.await() })
        awaitUntil { slow.inFlightHandlers.value == 0 }
        awaitUntil { hub.permitsAvailable("y-1") == 4 }
    }

    @Test fun `the handler permit is held for the whole stream and released after every ending`() = runBlocking {
        val endings = listOf("completed", "failed", "cancelled by the collector", "max results", "idle timeout")
        for (ending in endings) {
            val state = StreamerState()
            val hanging = ending == "idle timeout"
            val conn = owner("svc-x", "x-" + ending.replace(' ', '-'), if (hanging) HangingRowsStreamer(state) else RowsStreamer(state), maxConcurrentHandlers = 1)
            val bus = caller()
            val query = if (ending == "failed") ExportRows(5, failAt = 3) else ExportRows(5)
            // Credit 1: the handler is stalled on the collector's gate, so the stream is in progress while the second one is sent.
            val options = when (ending) {
                "max results" -> StreamOptions(maxResults = 2, initialCredit = 1)
                "idle timeout" -> StreamOptions(timeout = Duration.ofMillis(800), initialCredit = 1)
                else -> StreamOptions(initialCredit = 1)
            }
            val gate = CompletableDeferred<Unit>()
            val first = async {
                runCatching {
                    bus.stream(query, options).collect { row ->
                        if (row.n == 1) gate.await()
                        if (ending == "cancelled by the collector" && row.n == 2) throw CancellationException("enough")
                    }
                }
            }
            withTimeout(2_000) { state.started.await() }
            // The only permit is in use by the stream: a second stream waits in the hub's queue (capped: the hanging streamer never ends).
            val second = async { bus.stream(ExportRows(2), StreamOptions(maxResults = 2)).toList() }
            awaitUntil { hub.queuedFor(conn.instanceId) == 1 }
            assertEquals(1, conn.inFlightHandlers.value)
            gate.complete(Unit)
            withTimeout(5_000) { first.await() }
            assertEquals(listOf(Row(1), Row(2)), withTimeout(5_000) { second.await() }, ending)
            awaitUntil { hub.permitsAvailable(conn.instanceId) == 1 }
            assertEquals(0, hub.openStreams(), ending)
            awaitUntil { conn.inFlightHandlers.value == 0 }
            conn.stop()
            connections.remove(conn)
            awaitUntil { conn.instanceId !in hub.connectedInstances() }
        }
    }

    @Test fun `a hub without streaming is UnsupportedByHub and nothing is sent, unary and gathers keep working`() = runBlocking {
        hub.streaming = false
        owner("svc-x", "x-1", RowsStreamer(), OwnCountHandlers(7))
        val conn = connection("caller")
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val bus = bus(conn)
        val e = assertFailsWith<UnsupportedByHubException> { bus.stream(ExportRows(3)).toList() }
        assertEquals("streaming", e.feature)
        assertEquals(0, hub.requestsFrom("caller"))
        assertEquals(Count(7), bus.send(GetCount("k")))
        assertEquals(listOf(Count(7)), bus.scatterGather(GetCount("k")).results)
        assertTrue(conn.connected.value)

        hub.legacy = true
        val legacy = connection("legacy")
        legacy.start()
        assertTrue(legacy.awaitRegistered(wait))
        assertFailsWith<UnsupportedByHubException> { bus(legacy).stream(ExportRows(3)).toList() }
        assertEquals(0, hub.requestsFrom("legacy"))
    }

    @Test fun `a stream goes to one hub, and only to hubs with the capability`() = runBlocking {
        val old = hub { streaming = false }
        owner("svc-x", "x-new", RowsStreamer(), hubName = hub.name)
        owner("svc-x", "x-old", RowsStreamer(), hubName = old.name)
        val conns = HubConnections(
            targets = HubTargets.Static(listOf(hub.name, old.name)),
            channelFactory = { target -> InProcessChannelBuilder.forName(target).build() },
            serviceName = "svc-caller",
            instanceId = "caller",
            settings = settings,
            inbound = { req -> error(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
            scope = scope,
        ).also { multi += it }
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        val bus = bus(conns)
        repeat(4) { assertEquals(listOf(Row(1), Row(2)), bus.stream(ExportRows(2)).toList()) }
        assertEquals(4, hub.requestsFrom("caller"))
        assertEquals(0, old.requestsFrom("caller"))

        hub.close() // only the hub without the capability is left
        awaitUntil { conns.liveHubIds() == listOf(old.hubId) }
        val e = assertFailsWith<UnsupportedByHubException> { bus.stream(ExportRows(2)).toList() }
        assertEquals("streaming", e.feature)
        assertEquals(0, old.requestsFrom("caller"))
    }

    @Test fun `errors before any part map as for a unary query`() = runBlocking {
        val bus = caller()
        assertFailsWith<NoHandlerException> { bus.stream(ExportRows(1)).toList() }
        val command = connection("c-1", "svc-c") { req -> count(req.id, 1) }
        command.register(setOf("test.ExportRows"), emptySet())
        command.start()
        assertTrue(command.awaitRegistered(wait))
        assertFailsWith<InvalidMessageException> { bus.stream(ExportRows(1)).toList() }
        Unit
    }

    @Test fun `a stream is retried before its first part`() = runBlocking {
        val retry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(300), maxBackoff = Duration.ofMillis(300))
        val bus = caller(retry = retry)
        // NO_HANDLER is retried, and the owner that appeared during the backoff answers the second attempt.
        val early = async { bus.stream(ExportRows(2), StreamOptions(timeout = Duration.ofSeconds(4))).toList() }
        delay(100)
        owner("svc-x", "x-1", RowsStreamer())
        assertEquals(listOf(Row(1), Row(2)), withTimeout(5_000) { early.await() })
        assertEquals(2, hub.requestsFrom(callerId()))
    }

    @Test fun `after its first part a stream is never retried - a lost hub stream is HubConnectionLost`() = runBlocking {
        val retry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(300), maxBackoff = Duration.ofMillis(300))
        val bus = caller(retry = retry)
        val state = StreamerState()
        owner("svc-x", "x-1", HangingRowsStreamer(state))
        val seen = CopyOnWriteArrayList<Row>()
        val late = async {
            runCatching { bus.stream(ExportRows(2), StreamOptions(timeout = Duration.ofSeconds(4))).collect { seen += it } }
        }
        awaitUntil { seen.size == 2 }
        hub.dropInstance(callerId())
        assertIs<HubConnectionLostException>(withTimeout(3_000) { late.await() }.exceptionOrNull())
        assertEquals("cancelled", withTimeout(2_000) { state.ended.await() }) // the sender left: the hub cancelled the handler
        delay(700) // longer than the backoff: a retry would have sent another request
        awaitUntil { connections.first { it.instanceId == callerId() }.connected.value }
        assertEquals(1, hub.requestsFrom(callerId()))
        assertEquals(listOf(Row(1), Row(2)), seen)
    }

    @Test fun `a part that does not decode fails the stream and cancels the handler`() = runBlocking {
        val cancelled = CompletableDeferred<Boolean>()
        val raw = connection("x-1", "svc-x", inboundStream = { _, emit ->
            try {
                emit("{\"n\":1}".toByteArray())
                emit("{\"n\":\"not a number\"}".toByteArray())
                emit("{\"n\":3}".toByteArray())
                awaitCancellation()
            } catch (e: CancellationException) {
                cancelled.complete(true)
                throw e
            }
        })
        raw.register(emptySet(), setOf("test.ExportRows"))
        raw.start()
        assertTrue(raw.awaitRegistered(wait))
        val seen = mutableListOf<Row>()
        val e = assertFailsWith<InvalidMessageException> { caller().stream(ExportRows(3)).collect { seen += it } }
        assertTrue(e.message!!.contains("test.ExportRows"), e.message)
        assertEquals(listOf(Row(1)), seen)
        assertTrue(withTimeout(2_000) { cancelled.await() })
        awaitUntil { hub.cancelsFrom(callerId()).size == 1 }
        assertEquals(0, hub.openStreams())
    }

    @Test fun `the handler losing its hub stream cancels its running flows instead of leaving them waiting for credit`() = runBlocking {
        val state = StreamerState()
        owner("svc-x", "x-1", RowsStreamer(state), maxConcurrentHandlers = 2)
        val bus = caller()
        val gate = CompletableDeferred<Unit>()
        val seen = CopyOnWriteArrayList<Row>()
        val collecting = async { runCatching { bus.stream(ExportRows(1_000), StreamOptions(initialCredit = 4)).collect { seen += it; if (it.n == 2) gate.await() } } }
        awaitUntil { state.emitted.get() == 4 } // the window is full: the flow waits for credit
        hub.dropInstance("x-1")
        assertEquals("cancelled", withTimeout(2_000) { state.ended.await() })
        val owner = connections.first { it.instanceId == "x-1" }
        awaitUntil { owner.inFlightHandlers.value == 0 }
        gate.complete(Unit)
        val e = assertIs<RemoteHandlerException>(withTimeout(3_000) { collecting.await() }.exceptionOrNull())
        assertEquals("HANDLER_DISCONNECTED", e.code)
    }

    @Test fun `stream carries the bearer token of the thread that built it, not of the one collecting`() = runBlocking {
        owner("svc-x", "x-1", RowsStreamer())
        val bus = caller()
        val token = TestJwt.token("user-42", listOf("member"))
        SecurityContextHolder.getContext().authentication = JwtAuthenticationToken(TestJwt.decoder().decode(token))
        val rows = bus.stream(ExportRows(1)) // built on the request thread, as a controller returning a Flow
        SecurityContextHolder.clearContext()

        val collected = withContext(Dispatchers.IO) { rows.toList() }

        assertEquals(listOf(Row(1)), collected)
        val headers = assertNotNull(hub.lastRequestHeaders())
        assertEquals("Bearer $token", headers["authorization"])
    }

    @Test fun `a shared streaming type is announced as shared and streams round-robin over its owners`() = runBlocking {
        val x = StreamerState()
        val y = StreamerState()
        owner("svc-x", "x-1", SharedRowsStreamer(x))
        owner("svc-y", "y-1", SharedRowsStreamer(y))
        assertEquals(listOf("test.ExportRows"), hub.lastRegisterFrom("x-1")!!.sharedQueryTypesList)
        assertEquals(emptyList(), hub.lastRegisterFrom("x-1")!!.queryTypesList)
        val bus = caller()
        repeat(4) { assertEquals(listOf(Row(1), Row(2)), bus.stream(ExportRows(2)).toList()) }
        assertEquals(4, x.emitted.get())
        assertEquals(4, y.emitted.get())
    }

    // --- helpers ---

    /** A raw sender: the generated stub, so the test sees exactly what the Kotlin handler puts on the wire. */
    private inner class RawSender(instanceId: String) {
        val out = Channel<ClientMessage>(Channel.UNLIMITED)
        val inbox = Channel<ServerMessage>(Channel.UNLIMITED)

        init {
            scope.launch {
                runCatching { HubGrpcKt.HubCoroutineStub(hub.channel()).connect(out.consumeAsFlow()).collect { inbox.send(it) } }
            }
            send(
                ClientMessage.newBuilder().setRegister(
                    Register.newBuilder().setServiceName("svc-raw").setInstanceId(instanceId).setProtocolVersion(2).addCapabilities("streaming"),
                ).build(),
            )
        }

        fun send(message: ClientMessage) = out.trySend(message).getOrThrow()

        suspend fun next(): ServerMessage = withTimeout(5_000) { inbox.receive() }
    }

    private fun streamRequest(id: String, initialCredit: Int, maxResults: Int = 0, timeoutMs: Int = 5_000) = ClientMessage.newBuilder().setRequest(
        Request.newBuilder().setId(id).setKind(RequestKind.QUERY).setType("test.ExportRows").setMode(RequestMode.STREAMING)
            .setInitialCredit(initialCredit).setMaxResults(maxResults).setTimeoutMs(timeoutMs)
            .setPayload(ByteString.copyFromUtf8("{\"count\":5}")),
    ).build()

    private fun json(text: String) = ByteString.copyFromUtf8(text)

    private fun count(id: String, value: Int): Reply = Reply.newBuilder().setRequestId(id).setPayload(json("{\"value\":$value}")).build()

    private fun error(id: String, code: ErrorCode, message: String): Reply = Reply.newBuilder().setRequestId(id)
        .setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message)).build()
}
