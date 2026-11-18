package com.damoreira.snodra.messaging.hub

import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.ReplyEnd
import com.damoreira.snodra.server.v1.ReplyPart
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.server.v1.RequestMode
import com.damoreira.snodra.server.v1.ServerMessage
import com.damoreira.snodra.messaging.HubConnectionLostException
import com.damoreira.snodra.messaging.InvalidMessageException
import com.damoreira.snodra.messaging.NoHandlerException
import com.damoreira.snodra.messaging.RetryPolicy
import com.damoreira.snodra.messaging.ScatterOptions
import com.damoreira.snodra.messaging.ScatterPart
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Count
import com.damoreira.snodra.messaging.fixtures.GetCount
import com.damoreira.snodra.messaging.fixtures.TestJwt
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.test.InProcessHub
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** This service's own `test.GetCount` handler, shared ([shared]) or exclusive; answers [value]. */
class OwnCountHandlers(private val value: Int) {
    @QueryHandler(shared = true) fun handle(query: GetCount): Count = Count(value)
}

class OwnExclusiveCountHandlers(private val value: Int) {
    @QueryHandler fun handle(query: GetCount): Count = Count(value)
}

/** Scatter-gather queries and shared query types (P4a, ADR 0017) over in-process hubs. */
class ScatterGatherTest {
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
        inbound: suspend (Request) -> Reply = { req -> error(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
    ) = HubConnection({ InProcessChannelBuilder.forName(hubName).build() }, service, instanceId, settings, inbound, scope)
        .also { connections += it }

    private fun bus(transport: HubTransport, retry: RetryPolicy? = null, requestTimeout: Duration? = null) =
        HubBus(transport, registry, invoker, codec, resolver, true, TraceHeaders(null, null), requestTimeout, emptyMap(), retry)

    /** An instance of [service] owning `test.GetCount` as a shared type ([shared]) and answering with [answer]. */
    private suspend fun owner(
        service: String,
        instanceId: String,
        shared: Boolean = true,
        hubName: String = hub.name,
        answer: suspend (Request) -> Reply = { req -> count(req.id, instanceId.hashCode() and 0xff) },
    ): HubConnection = connection(instanceId, service, hubName, answer).apply {
        if (shared) register(emptySet(), emptySet(), setOf("test.GetCount")) else register(emptySet(), setOf("test.GetCount"))
        start()
        assertTrue(awaitRegistered(wait))
    }

    private suspend fun caller(hubName: String = hub.name, retry: RetryPolicy? = null): HubBus {
        val conn = connection("caller-" + UUID.randomUUID().toString().take(6), "svc-caller", hubName)
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        return bus(conn, retry)
    }

    /** svc-x with x-1 and x-2, svc-y with y-1, each answering `{"value": n}` after [delays] (by instance). */
    private suspend fun threeOwners(delays: Map<String, Long> = emptyMap(), values: Map<String, Int> = mapOf("x-1" to 1, "x-2" to 2, "y-1" to 3)) {
        for ((service, instance) in listOf("svc-x" to "x-1", "svc-x" to "x-2", "svc-y" to "y-1")) {
            owner(service, instance) { req ->
                delay(delays[instance] ?: 0)
                count(req.id, values.getValue(instance))
            }
        }
    }

    // --- registration ---

    @Test fun `register lists shared query types apart`() = runBlocking {
        val conn = connection("a")
        conn.register(setOf("test.Ping"), setOf("test.Get"), setOf("test.GetCount"))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val register = hub.lastRegisterFrom("a")!!
        assertEquals(listOf("test.Get"), register.queryTypesList)
        assertEquals(listOf("test.GetCount"), register.sharedQueryTypesList)
    }

    // --- the gather ---

    @Test fun `two owner services with three instances give three parts and the end`() = runBlocking {
        threeOwners()
        val result = caller().scatterGather(GetCount("k"))

        assertEquals(3, result.parts.size)
        assertEquals(0, result.failed)
        assertEquals(0, result.timedOut)
        assertEquals(
            setOf(Triple("svc-x", "x-1", Count(1)), Triple("svc-x", "x-2", Count(2)), Triple("svc-y", "y-1", Count(3))),
            result.parts.map { Triple(it.service, it.instance, it.result) }.toSet(),
        )
        assertTrue(result.parts.all { it.error == null })
        val sent = hub.lastRequest()!!
        assertEquals(RequestMode.SCATTER_GATHER, sent.mode)
        assertEquals(RequestKind.QUERY, sent.kind)
        assertEquals("test.GetCount", sent.type)
        assertEquals((0..2).map { "${sent.id}#$it" }.toSet(), hub.forwardedOrder().takeLast(3).toSet())
    }

    @Test fun `ScatterOptions become the request fields`() = runBlocking {
        threeOwners()
        caller().scatterGather(GetCount("k"), ScatterOptions(timeout = Duration.ofMillis(1_234), maxResults = 2, priority = 5))
        val sent = hub.lastRequest()!!
        assertEquals(1_234, sent.timeoutMs)
        assertEquals(2, sent.maxResults)
        assertEquals(5, sent.priority)
        assertEquals("", sent.routingKey)

        caller().scatterGather(GetCount("k"))
        val defaults = hub.lastRequest()!!
        assertEquals(0, defaults.timeoutMs) // unset: the hub decides
        assertEquals(0, defaults.maxResults)
        assertEquals(0, defaults.priority)
    }

    @Test fun `parts come in arrival order`() = runBlocking {
        threeOwners(delays = mapOf("x-1" to 600, "x-2" to 300, "y-1" to 0))
        val result = caller().scatterGather(GetCount("k"))
        assertEquals(listOf("y-1", "x-2", "x-1"), result.parts.map { it.instance })
    }

    @Test fun `max_results ends the gather after that many successful parts`() = runBlocking {
        threeOwners(delays = mapOf("x-1" to 3_000))
        val started = System.nanoTime()
        val result = caller().scatterGather(GetCount("k"), ScatterOptions(maxResults = 2))
        assertEquals(setOf("x-2", "y-1"), result.parts.map { it.instance }.toSet())
        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofMillis(2_000))
        assertEquals(0, result.timedOut)
    }

    @Test fun `a failing handler is a part with an error`() = runBlocking {
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        owner("svc-y", "y-1") { req -> error(req.id, ErrorCode.HANDLER_ERROR, "boom", details = "{\"why\":\"x\"}") }
        val result = caller().scatterGather(GetCount("k"))
        assertEquals(2, result.parts.size)
        assertEquals(1, result.failed)
        val failed = result.parts.single { it.instance == "y-1" }
        assertNull(failed.result)
        val e = assertNotNull(failed.error)
        assertEquals("boom", e.message)
        assertEquals("HANDLER_ERROR", e.code)
        assertEquals("{\"why\":\"x\"}", String(e.details))
        assertEquals(Count(1), result.parts.single { it.instance == "x-1" }.result)
    }

    @Test fun `a part that does not decode is a failed part, not a failed gather`() = runBlocking {
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        owner("svc-y", "y-1") { req -> Reply.newBuilder().setRequestId(req.id).setPayload(json("{\"value\":\"not a number\"}")).build() }
        val result = caller().scatterGather(GetCount("k"))
        assertEquals(2, result.parts.size)
        assertEquals(1, result.failed) // counted here although the hub's ReplyEnd.failed is 0
        val bad = assertNotNull(result.parts.single { it.instance == "y-1" }.error)
        assertEquals("ERROR_CODE_UNSPECIFIED", bad.code)
        assertTrue(bad.message!!.contains("test.GetCount"), bad.message)
        assertEquals(listOf(Count(1)), result.results)
    }

    @Test fun `a target that does not answer by the deadline is counted as timed out`() = runBlocking {
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        owner("svc-y", "y-1") { req -> delay(5_000); count(req.id, 2) }
        val started = System.nanoTime()
        val result = caller().scatterGather(GetCount("k"), ScatterOptions(timeout = Duration.ofMillis(500)))
        assertTrue(Duration.ofNanos(System.nanoTime() - started) < Duration.ofMillis(1_500))
        assertEquals(listOf("x-1"), result.parts.map { it.instance })
        assertEquals(1, result.timedOut)
        assertEquals(0, result.failed)
    }

    @Test fun `no owner is NoHandlerException`() = runBlocking {
        assertFailsWith<NoHandlerException> { caller().scatterGather(GetCount("k")) }
        Unit
    }

    @Test fun `a type owned as a command is InvalidMessageException`() = runBlocking {
        val owner = connection("x-1", "svc-x") { req -> count(req.id, 1) }
        owner.register(setOf("test.GetCount"), emptySet())
        owner.start()
        assertTrue(owner.awaitRegistered(wait))
        assertFailsWith<InvalidMessageException> { caller().scatterGather(GetCount("k")) }
        Unit
    }

    @Test fun `an exclusive type is gathered from every instance of its owner`() = runBlocking {
        owner("svc-x", "x-1", shared = false) { req -> count(req.id, 1) }
        owner("svc-x", "x-2", shared = false) { req -> count(req.id, 2) }
        val result = caller().scatterGather(GetCount("k"))
        assertEquals(setOf(Count(1), Count(2)), result.parts.mapNotNull { it.result }.toSet())
    }

    @Test fun `scatterGatherFlow emits each part as it arrives and completes on the end`() = runBlocking {
        val release = CompletableDeferred<Unit>()
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        owner("svc-y", "y-1") { req -> release.await(); count(req.id, 2) }
        val seen = CopyOnWriteArrayList<ScatterPart<Count>>()
        val bus = caller()
        val collected = async {
            bus.scatterGatherFlow(GetCount("k")).collect { part ->
                seen += part
                if (part.instance == "x-1") release.complete(Unit) // the second part is only sent after the first arrived
            }
        }
        withTimeout(3_000) { collected.await() }
        assertEquals(listOf("x-1", "y-1"), seen.map { it.instance })
        assertEquals(listOf(Count(1), Count(2)), seen.map { it.result })
    }

    @Test fun `scatterGatherFlow carries the bearer token of the thread that built it, not of the one collecting`() = runBlocking {
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        val bus = caller()
        val token = TestJwt.token("user-42", listOf("member"))
        SecurityContextHolder.getContext().authentication = JwtAuthenticationToken(TestJwt.decoder().decode(token))
        val flow = bus.scatterGatherFlow(GetCount("k")) // built on the request thread, as a controller returning a Flow
        SecurityContextHolder.clearContext()

        // Collected elsewhere, on a thread without the caller's SecurityContext.
        val parts = withContext(Dispatchers.IO) { flow.toList() }

        assertEquals(listOf(Count(1)), parts.map { it.result })
        val headers = assertNotNull(hub.lastRequestHeaders())
        assertEquals("Bearer $token", headers["authorization"])
    }

    @Test fun `a unary query on a shared type round-robins over every owner`() = runBlocking {
        val served = ConcurrentHashMap<String, Int>()
        for ((service, instance) in listOf("svc-x" to "x-1", "svc-x" to "x-2", "svc-y" to "y-1")) {
            owner(service, instance) { req -> served.merge(instance, 1, Int::plus); count(req.id, 1) }
        }
        val bus = caller()
        repeat(6) { assertEquals(Count(1), bus.send(GetCount("k"))) }
        assertEquals(mapOf("x-1" to 2, "x-2" to 2, "y-1" to 2), served.toMap())
    }

    @Test fun `an owner's own point query on a shared type goes through the hub, not to its local handler`() = runBlocking {
        val served = ConcurrentHashMap<String, Int>()
        registry.register(OwnCountHandlers(99)) // this service owns test.GetCount as a shared type
        val self = owner("svc-x", "x-1") { req -> served.merge("x-1", 1, Int::plus); count(req.id, 1) }
        owner("svc-y", "y-1") { req -> served.merge("y-1", 1, Int::plus); count(req.id, 1) }
        val bus = bus(self)

        repeat(4) { assertEquals(Count(1), bus.send(GetCount("k"))) } // never Count(99): the hub round-robins over both owners
        assertEquals(mapOf("x-1" to 2, "y-1" to 2), served.toMap())

        // A gather from the owner still asks every instance of every owner over the hub.
        val result = bus.scatterGather(GetCount("k"))
        assertEquals(setOf("x-1", "y-1"), result.parts.map { it.instance }.toSet())
        assertEquals(mapOf("x-1" to 3, "y-1" to 3), served.toMap())
    }

    @Test fun `an owner's own point query on an exclusive type is answered locally`() = runBlocking {
        val served = ConcurrentHashMap<String, Int>()
        registry.register(OwnExclusiveCountHandlers(99))
        val self = owner("svc-x", "x-1", shared = false) { req -> served.merge("x-1", 1, Int::plus); count(req.id, 1) }
        val bus = bus(self)

        repeat(2) { assertEquals(Count(99), bus.send(GetCount("k"))) }
        assertTrue(served.isEmpty(), served.toString())
        assertEquals(0, hub.requestsFrom("x-1"))
    }

    // --- transport ---

    @Test fun `parts and ends for unknown ids are ignored`() = runBlocking {
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        val conn = connection("caller")
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        hub.inject("caller", ServerMessage.newBuilder().setPart(ReplyPart.newBuilder().setRequestId("nobody").setPayload(json("{\"value\":9}"))).build())
        hub.inject("caller", ServerMessage.newBuilder().setEnd(ReplyEnd.newBuilder().setRequestId("nobody").setParts(1)).build())
        val result = bus(conn).scatterGather(GetCount("k"))
        assertEquals(listOf(Count(1)), result.parts.map { it.result })
        assertTrue(conn.connected.value)
    }

    @Test fun `the stream breaking mid-gather fails it with HubConnectionLost, and retry gathers again`() = runBlocking {
        val first = CompletableDeferred<Unit>()
        val calls = java.util.concurrent.atomic.AtomicInteger()
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        owner("svc-y", "y-1") { req ->
            if (calls.incrementAndGet() == 1) { first.complete(Unit); delay(10_000) }
            count(req.id, 2)
        }
        val conn = connection("caller")
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val lost = async { runCatching { bus(conn).scatterGather(GetCount("k")) } }
        first.await()
        delay(100) // x-1's part has arrived
        hub.dropInstance("caller")
        assertIs<HubConnectionLostException>(withTimeout(3_000) { lost.await() }.exceptionOrNull())

        assertTrue(conn.awaitRegistered(wait))
        calls.set(0)
        val retried = async {
            bus(conn, retry = RetryPolicy(maxAttempts = 2, initialBackoff = Duration.ofMillis(50), maxBackoff = Duration.ofMillis(100)))
                .scatterGather(GetCount("k"), ScatterOptions(timeout = Duration.ofSeconds(4)))
        }
        withTimeout(2_000) { while (calls.get() < 1) delay(10) }
        delay(100)
        hub.dropInstance("caller")
        val result = withTimeout(6_000) { retried.await() }
        // The second attempt is a new gather: only its own parts, none of the lost attempt.
        assertEquals(listOf(Count(1), Count(2)), result.parts.mapNotNull { it.result }.sortedBy { it.value })
    }

    @Test fun `the flow retries until its first part, never after it`() = runBlocking {
        val retry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(400), maxBackoff = Duration.ofMillis(400))
        val bus = caller(retry = retry)
        // Before any part: NO_HANDLER is retried, and the owner that appeared meanwhile answers.
        val early = async { bus.scatterGatherFlow(GetCount("k"), ScatterOptions(timeout = Duration.ofSeconds(4))).toList() }
        delay(100)
        owner("svc-x", "x-1") { req -> count(req.id, 1) }
        assertEquals(listOf(Count(1)), withTimeout(5_000) { early.await() }.map { it.result })

        // After a part: the broken stream ends the flow, nobody is asked again.
        val asked = java.util.concurrent.atomic.AtomicInteger()
        owner("svc-y", "y-1") { req -> asked.incrementAndGet(); delay(10_000); count(req.id, 2) }
        val callerId = connections.first { it.instanceId.startsWith("caller-") }.instanceId
        val seen = CopyOnWriteArrayList<ScatterPart<Count>>()
        val late = async {
            runCatching { bus.scatterGatherFlow(GetCount("k"), ScatterOptions(timeout = Duration.ofSeconds(4))).collect { seen += it } }
        }
        withTimeout(3_000) { while (seen.isEmpty()) delay(10) }
        hub.dropInstance(callerId)
        assertIs<HubConnectionLostException>(withTimeout(3_000) { late.await() }.exceptionOrNull())
        delay(600) // longer than the backoff: a retry would have asked y-1 again
        assertEquals(1, seen.size)
        assertEquals(1, asked.get())
    }

    // --- multi-hub (P3) ---

    @Test fun `one gather goes to one hub, round-robin over the live hubs`() = runBlocking {
        val other = hub()
        owner("svc-x", "x-1", hubName = hub.name) { req -> count(req.id, 1) }
        owner("svc-x", "x-2", hubName = other.name) { req -> count(req.id, 2) }
        val conns = HubConnections(
            targets = HubTargets.Static(listOf(hub.name, other.name)),
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
        repeat(4) { assertEquals(1, bus.scatterGather(GetCount("k")).parts.size) } // each hub knows only its own owner
        assertEquals(2, hub.requestsFrom("caller"))
        assertEquals(2, other.requestsFrom("caller"))
    }

    // --- helpers ---

    private fun json(text: String) = ByteString.copyFromUtf8(text)

    private fun count(id: String, value: Int): Reply = Reply.newBuilder().setRequestId(id).setPayload(json("{\"value\":$value}")).build()

    private fun error(id: String, code: ErrorCode, message: String, details: String = ""): Reply = Reply.newBuilder().setRequestId(id)
        .setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message).setDetails(json(details))).build()
}
