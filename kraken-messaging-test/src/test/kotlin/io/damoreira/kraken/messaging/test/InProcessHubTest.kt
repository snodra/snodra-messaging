package io.damoreira.kraken.messaging.test

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.hub.v1.RequestMode
import io.damoreira.kraken.hub.v1.ServerMessage
import io.grpc.Status
import io.grpc.StatusException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class InProcessHubTest {
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)

    @AfterEach
    fun tearDown() {
        scope.cancel()
        hub.close()
    }

    /** A raw hub client: no starter code, just the generated stub. */
    private inner class RawClient {
        val out = Channel<ClientMessage>(Channel.UNLIMITED)
        val inbox = Channel<ServerMessage>(Channel.UNLIMITED)
        val ended = CompletableDeferred<Throwable?>()

        init {
            scope.launch {
                try {
                    HubGrpcKt.HubCoroutineStub(hub.channel()).connect(out.consumeAsFlow()).collect { inbox.send(it) }
                    ended.complete(null)
                } catch (t: Throwable) {
                    ended.complete(t)
                }
            }
        }

        fun send(msg: ClientMessage) = out.trySend(msg).getOrThrow()

        /** A protocol v2 Register declaring `flow-control` with [permits] initial permits. */
        fun registerFlowControlled(service: String, instance: String, permits: Int, vararg commands: String) = send(
            ClientMessage.newBuilder().setRegister(
                Register.newBuilder().setServiceName(service).setInstanceId(instance).addAllCommandTypes(commands.toList())
                    .setProtocolVersion(2).addCapabilities("flow-control").setInitialPermits(permits),
            ).build(),
        )

        fun register(service: String, instance: String, vararg commands: String) = send(
            ClientMessage.newBuilder().setRegister(
                Register.newBuilder().setServiceName(service).setInstanceId(instance).addAllCommandTypes(commands.toList()),
            ).build(),
        )

        /** A protocol v2 Register declaring [capabilities], with exclusive [queries] and [shared] query types. */
        fun registerV2(
            service: String,
            instance: String,
            queries: List<String> = emptyList(),
            shared: List<String> = emptyList(),
            capabilities: List<String> = listOf("scatter-gather"),
        ) = send(
            ClientMessage.newBuilder().setRegister(
                Register.newBuilder().setServiceName(service).setInstanceId(instance).addAllQueryTypes(queries)
                    .addAllSharedQueryTypes(shared).setProtocolVersion(2).addAllCapabilities(capabilities),
            ).build(),
        )

        fun reply(id: String, json: String) = send(
            ClientMessage.newBuilder().setReply(Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json))).build(),
        )

        suspend fun next(): ServerMessage = withTimeout(TIMEOUT_MS) { inbox.receive() }
        suspend fun awaitEnd(): Throwable? = withTimeout(TIMEOUT_MS) { ended.await() }
    }

    private fun request(id: String, type: String, headers: Map<String, String> = emptyMap(), priority: Int = 0) =
        ClientMessage.newBuilder().setRequest(
            Request.newBuilder().setId(id).setKind(RequestKind.COMMAND).setType(type).setPriority(priority)
                .setPayload(ByteString.copyFromUtf8("{}")).putAllHeaders(headers),
        ).build()

    private fun permits(count: Int, grant: Int = 0) = ClientMessage.newBuilder()
        .setPermits(io.damoreira.kraken.hub.v1.Permits.newBuilder().setCount(count).setGrant(grant)).build()

    /** Sends [msg] and waits until the hub has handled it (counted as a `Permits` message from [instance]). */
    private suspend fun RawClient.sendPermits(instance: String, msg: ClientMessage) {
        val before = hub.permitMessagesFrom(instance)
        send(msg)
        withTimeout(TIMEOUT_MS) { while (hub.permitMessagesFrom(instance) == before) kotlinx.coroutines.delay(5) }
    }

    @Test
    fun `permits never exceed the cap, which only a grant above it raises`() = runBlocking {
        val handler = RawClient().also { it.registerFlowControlled("svc-a", "a-1", 2, "test.Ping") }
        handler.next() // ack
        awaitRegistered("test.Ping")
        handler.sendPermits("a-1", permits(5))
        assertEquals(2, hub.permitsAvailable("a-1"), "clamped at initial_permits")
        handler.sendPermits("a-1", permits(2, grant = 4))
        assertEquals(4, hub.permitsAvailable("a-1"), "the grant raised the cap first")
        handler.sendPermits("a-1", permits(3, grant = 1))
        assertEquals(4, hub.permitsAvailable("a-1"), "a grant never lowers the cap")
    }

    @Test
    fun `a hub that predates the grant keeps the cap at initial_permits`() = runBlocking {
        hub.permitGrants = false
        val handler = RawClient().also { it.registerFlowControlled("svc-a", "a-1", 2, "test.Ping") }
        handler.next() // ack
        awaitRegistered("test.Ping")
        handler.sendPermits("a-1", permits(2, grant = 4))
        assertEquals(2, hub.permitsAvailable("a-1"))
    }

    @Test
    fun `flow-controlled handler gets requests only with permits, highest priority first`() = runBlocking {
        val handler = RawClient().also { it.registerFlowControlled("svc-a", "a-1", 1, "test.Ping") }
        val ack = handler.next()
        assertEquals(listOf("flow-control"), ack.registerAck.capabilitiesList)
        awaitRegistered("test.Ping")
        val sender = RawClient().also { it.register("svc-b", "b-1") }

        sender.send(request("r1", "test.Ping"))
        assertEquals("r1", handler.next().request.id) // the one permit
        sender.send(request("low", "test.Ping", priority = 0))
        sender.send(request("high", "test.Ping", priority = 5))
        withTimeout(TIMEOUT_MS) { while (hub.queuedFor("a-1") < 2) kotlinx.coroutines.delay(10) }
        assertEquals(0, hub.permitsAvailable("a-1"))

        handler.send(permits(1)) // one permit back per finished request, never past initial_permits
        assertEquals("high", handler.next().request.id)
        handler.send(permits(1))
        assertEquals("low", handler.next().request.id)
        assertEquals(2, hub.permitsGrantedBy("a-1"))
    }

    @Test
    fun `full queue is RESOURCE_EXHAUSTED for flow-control senders and handler busy for others`() = runBlocking {
        hub.queueCapacity = 1
        val handler = RawClient().also { it.registerFlowControlled("svc-a", "a-1", 1, "test.Ping") }
        handler.next() // ack
        awaitRegistered("test.Ping")
        val v1 = RawClient().also { it.register("svc-b", "b-1") }
        val flow = RawClient().also { it.registerFlowControlled("svc-c", "c-1", 4) }
        flow.next() // ack
        v1.send(request("r1", "test.Ping")) // forwarded
        handler.next()
        v1.send(request("r2", "test.Ping")) // queued
        withTimeout(TIMEOUT_MS) { while (hub.queuedFor("a-1") < 1) kotlinx.coroutines.delay(10) }
        v1.send(request("r3", "test.Ping"))
        flow.send(request("r4", "test.Ping"))
        val busy = v1.next().error
        assertEquals(ErrorCode.HANDLER_ERROR, busy.code)
        assertEquals("handler busy", busy.message)
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED, flow.next().error.code)
    }

    @Test
    fun `P0 emulation acks without flow-control and never queues`() = runBlocking {
        hub.flowControl = false
        val handler = RawClient().also { it.registerFlowControlled("svc-a", "a-1", 1, "test.Ping") }
        assertEquals(emptyList(), handler.next().registerAck.capabilitiesList)
        awaitRegistered("test.Ping")
        val sender = RawClient().also { it.register("svc-b", "b-1") }
        sender.send(request("r1", "test.Ping"))
        sender.send(request("r2", "test.Ping"))
        assertEquals("r1", handler.next().request.id)
        assertEquals("r2", handler.next().request.id)
        assertEquals(null, hub.permitsAvailable("a-1"))
    }

    private suspend fun awaitRegistered(type: String) = withTimeout(TIMEOUT_MS) {
        while (type !in hub.registeredTypes()) kotlinx.coroutines.delay(10)
    }

    @Test
    fun `relays command between two raw clients`() = runBlocking {
        val handler = RawClient().also { it.register("svc-a", "a-1", "test.Ping") }
        awaitRegistered("test.Ping")
        assertEquals(setOf("a-1"), hub.connectedInstances())
        val sender = RawClient().also { it.register("svc-b", "b-1") }

        sender.send(request("r1", "test.Ping", mapOf("correlationId" to "c-1")))
        val forwarded = handler.next()
        assertTrue(forwarded.hasRequest())
        assertEquals("r1", forwarded.request.id)
        assertEquals("c-1", forwarded.request.headersMap["correlationId"])
        assertEquals(mapOf("correlationId" to "c-1"), hub.lastRequestHeaders())

        handler.send(
            ClientMessage.newBuilder().setReply(
                Reply.newBuilder().setRequestId("r1").setPayload(ByteString.copyFromUtf8("""{"ok":true}""")),
            ).build(),
        )
        val reply = sender.next()
        assertTrue(reply.hasReply())
        assertEquals("r1", reply.reply.requestId)
        assertEquals("""{"ok":true}""", reply.reply.payload.toStringUtf8())
    }

    @Test
    fun `unknown type is NO_HANDLER`() = runBlocking {
        val sender = RawClient().also { it.register("svc-b", "b-1") }
        sender.send(request("r1", "nobody.Owns"))
        val err = sender.next()
        assertTrue(err.hasError())
        assertEquals("r1", err.error.requestId)
        assertEquals(ErrorCode.NO_HANDLER, err.error.code)
    }

    @Test
    fun `dropped handler fails in flight with HANDLER_DISCONNECTED`() = runBlocking {
        val handler = RawClient().also { it.register("svc-a", "a-1", "test.Ping") }
        awaitRegistered("test.Ping")
        val sender = RawClient().also { it.register("svc-b", "b-1") }
        sender.send(request("r1", "test.Ping"))
        handler.next()

        hub.dropInstance("a-1")

        val err = sender.next()
        assertEquals(ErrorCode.HANDLER_DISCONNECTED, err.error.code)
        assertEquals("r1", err.error.requestId)
        assertNotNull(handler.awaitEnd())
        assertTrue("a-1" !in hub.connectedInstances())
    }

    @Test
    fun `a second stream of one instance id replaces the first, like the real hub`() = runBlocking {
        val first = RawClient().also { it.register("svc-a", "a-1", "test.Ping") }
        awaitRegistered("test.Ping")
        val sender = RawClient().also { it.register("svc-b", "b-1") }
        sender.send(request("r1", "test.Ping"))
        first.next()

        val second = RawClient().also { it.register("svc-a", "a-1", "test.Ping") }
        // The older stream ends and its in-flight request fails; the newer one is the instance's only session.
        assertEquals(null, first.awaitEnd())
        val err = sender.next()
        assertEquals(ErrorCode.HANDLER_DISCONNECTED, err.error.code)
        assertEquals(1, hub.sessionsOf("a-1"))
        assertEquals(1, hub.sessionsReplacedOf("a-1"))
        assertEquals(2, hub.streamsOpenedBy("a-1"))
        sender.send(request("r2", "test.Ping"))
        assertEquals("r2", second.next().request.id)
    }

    @Test
    fun `with replaceSessions off both streams of one instance id stay`() = runBlocking {
        hub.replaceSessions = false
        RawClient().also { it.register("svc-a", "a-1", "test.Ping") }
        awaitRegistered("test.Ping")
        RawClient().also { it.register("svc-a", "a-1", "test.Ping") }
        withTimeout(TIMEOUT_MS) { while (hub.sessionsOf("a-1") < 2) kotlinx.coroutines.delay(10) }
        assertEquals(0, hub.sessionsReplacedOf("a-1"))
    }

    @Test
    fun `request before register is FAILED_PRECONDITION`() = runBlocking {
        val client = RawClient()
        client.send(request("r1", "test.Ping"))
        val failure = client.awaitEnd()
        assertIs<StatusException>(failure)
        assertEquals(Status.Code.FAILED_PRECONDITION, failure.status.code)
    }

    @Test
    fun `second owner of a type is ALREADY_EXISTS`() = runBlocking {
        RawClient().also { it.register("svc-a", "a-1", "test.Ping") }
        awaitRegistered("test.Ping")
        val second = RawClient().also { it.register("svc-b", "b-1", "test.Ping") }
        val failure = second.awaitEnd()
        assertIs<StatusException>(failure)
        assertEquals(Status.Code.ALREADY_EXISTS, failure.status.code)
        assertEquals("type test.Ping is owned by service svc-a", failure.status.description)
    }

    // --- shared query types and scatter-gather (P4a) ---

    private fun scatter(id: String, type: String, timeoutMs: Int = 0, maxResults: Int = 0) = ClientMessage.newBuilder().setRequest(
        Request.newBuilder().setId(id).setKind(RequestKind.QUERY).setType(type).setMode(RequestMode.SCATTER_GATHER)
            .setTimeoutMs(timeoutMs).setMaxResults(maxResults).setPayload(ByteString.copyFromUtf8("{}")),
    ).build()

    @Test
    fun `a shared type may have many owners, registrants must agree`() = runBlocking {
        val x = RawClient().also { it.registerV2("svc-x", "x-1", shared = listOf("test.Search")) }
        assertEquals(listOf("scatter-gather"), x.next().registerAck.capabilitiesList)
        awaitRegistered("test.Search")
        val y = RawClient().also { it.registerV2("svc-y", "y-1", shared = listOf("test.Search")) }
        assertTrue(y.next().hasRegisterAck())

        val exclusive = RawClient().also { it.registerV2("svc-z", "z-1", queries = listOf("test.Search")) }
        val failure = assertIs<StatusException>(exclusive.awaitEnd())
        assertEquals(Status.Code.ALREADY_EXISTS, failure.status.code)

        // Even within one service: x-2 of svc-x registers the type exclusive.
        val sameService = RawClient().also { it.registerV2("svc-x", "x-2", queries = listOf("test.Search")) }
        assertEquals(Status.Code.ALREADY_EXISTS, assertIs<StatusException>(sameService.awaitEnd()).status.code)

        RawClient().also { it.registerV2("svc-e", "e-1", queries = listOf("test.Exclusive")) }.next()
        awaitRegistered("test.Exclusive")
        val sharedOverExclusive = RawClient().also { it.registerV2("svc-f", "f-1", shared = listOf("test.Exclusive")) }
        assertEquals(Status.Code.ALREADY_EXISTS, assertIs<StatusException>(sharedOverExclusive.awaitEnd()).status.code)

        val both = RawClient().also { it.registerV2("svc-g", "g-1", queries = listOf("test.Both"), shared = listOf("test.Both")) }
        assertEquals(Status.Code.INVALID_ARGUMENT, assertIs<StatusException>(both.awaitEnd()).status.code)
    }

    @Test
    fun `scatter-gather sends each target its own unary request and relays parts then the end`() = runBlocking {
        val x1 = RawClient().also { it.registerV2("svc-x", "x-1", shared = listOf("test.Search")) }.apply { next() }
        val x2 = RawClient().also { it.registerV2("svc-x", "x-2", shared = listOf("test.Search")) }.apply { next() }
        val y1 = RawClient().also { it.registerV2("svc-y", "y-1", shared = listOf("test.Search")) }.apply { next() }
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 3) kotlinx.coroutines.delay(10) }
        val sender = RawClient().also { it.registerV2("svc-s", "s-1") }.apply { next() }

        sender.send(scatter("g1", "test.Search"))
        val targets = listOf(x1, x2, y1).map { it.next().request }
        assertEquals(listOf("g1#0", "g1#1", "g1#2"), targets.map { it.id }) // target order: service, then instance
        assertTrue(targets.all { it.mode == RequestMode.UNARY })
        y1.reply("g1#2", "{\"by\":\"y-1\"}")
        x1.reply("g1#0", "{\"by\":\"x-1\"}")
        x2.send(
            ClientMessage.newBuilder().setReply(
                Reply.newBuilder().setRequestId("g1#1").setError(
                    io.damoreira.kraken.hub.v1.Error.newBuilder().setRequestId("g1#1").setCode(ErrorCode.HANDLER_ERROR).setMessage("boom"),
                ),
            ).build(),
        )
        val parts = (0 until 3).map { sender.next().part }
        assertEquals(listOf("y-1", "x-1", "x-2"), parts.map { it.responderInstance })
        assertEquals(listOf("svc-y", "svc-x", "svc-x"), parts.map { it.responderService })
        assertEquals(listOf(0L, 1L, 2L), parts.map { it.seq })
        assertTrue(parts.all { it.requestId == "g1" })
        assertEquals("boom", parts[2].error.message)
        val end = sender.next().end
        assertEquals("g1", end.requestId)
        assertEquals(3, end.parts)
        assertEquals(1, end.failed)
        assertEquals(0, end.timedOut)
    }

    @Test
    fun `max_results ends early and the deadline counts the missing targets`() = runBlocking {
        val x1 = RawClient().also { it.registerV2("svc-x", "x-1", shared = listOf("test.Search")) }.apply { next() }
        val y1 = RawClient().also { it.registerV2("svc-y", "y-1", shared = listOf("test.Search")) }.apply { next() }
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 2) kotlinx.coroutines.delay(10) }
        val sender = RawClient().also { it.registerV2("svc-s", "s-1") }.apply { next() }

        sender.send(scatter("g1", "test.Search", maxResults = 1))
        x1.next(); y1.next()
        x1.reply("g1#0", "{}")
        assertTrue(sender.next().hasPart())
        val early = sender.next().end
        assertEquals(1, early.parts)
        assertEquals(0, early.timedOut)
        y1.reply("g1#1", "{}") // after the end: dropped

        sender.send(scatter("g2", "test.Search", timeoutMs = 200))
        x1.next(); y1.next()
        x1.reply("g2#0", "{}")
        val part = sender.next()
        assertEquals("g2", part.part.requestId) // nothing of g1 followed its end
        val late = sender.next().end
        assertEquals(1, late.parts)
        assertEquals(1, late.timedOut)
    }

    @Test
    fun `scatter-gather refusals - no target, a command, senders without the capability, the P3 emulation`() = runBlocking {
        val sender = RawClient().also { it.registerV2("svc-s", "s-1") }.apply { next() }
        sender.send(scatter("g1", "test.Nobody"))
        assertEquals(ErrorCode.NO_HANDLER, sender.next().error.code)

        RawClient().also { it.register("svc-c", "c-1", "test.Cmd") }
        awaitRegistered("test.Cmd")
        sender.send(scatter("g2", "test.Cmd"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code)

        val undeclared = RawClient().also { it.registerV2("svc-u", "u-1", capabilities = emptyList()) }.apply { next() }
        undeclared.send(scatter("g3", "test.Cmd"))
        assertEquals(ErrorCode.UNSUPPORTED, undeclared.next().error.code)
        val v1 = RawClient().also { it.register("svc-v", "v-1") }
        v1.send(scatter("g4", "test.Cmd"))
        assertEquals(ErrorCode.INVALID_REQUEST, v1.next().error.code)

        hub.scatterGather = false // a P3 hub: no capability, shared types ignored
        val p3 = RawClient().also { it.registerV2("svc-p", "p-1", shared = listOf("test.Search")) }
        assertEquals(emptyList(), p3.next().registerAck.capabilitiesList)
        p3.send(scatter("g5", "test.Cmd"))
        assertEquals(ErrorCode.UNSUPPORTED, p3.next().error.code)
        kotlinx.coroutines.delay(50)
        assertTrue("test.Search" !in hub.registeredTypes())
    }

    private companion object {
        const val TIMEOUT_MS = 5_000L
    }
}
