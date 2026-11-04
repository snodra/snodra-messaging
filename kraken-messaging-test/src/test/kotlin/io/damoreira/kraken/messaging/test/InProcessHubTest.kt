package io.damoreira.kraken.messaging.test

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Cancel
import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.Flow
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.ReplyEnd
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.ReplyPart
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
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.test.assertNull
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

        /** A Register of [commands], exclusive [queries] and [shared] query types with [permits] initial permits; the hub answers it with an ack. */
        fun register(
            service: String,
            instance: String,
            vararg commands: String,
            queries: List<String> = emptyList(),
            shared: List<String> = emptyList(),
            permits: Int = 1_000,
        ) = send(
            ClientMessage.newBuilder().setRegister(
                Register.newBuilder().setServiceName(service).setInstanceId(instance).addAllCommandTypes(commands.toList())
                    .addAllQueryTypes(queries).addAllSharedQueryTypes(shared).setInitialPermits(permits),
            ).build(),
        )

        /** [register], then the hub's ack. */
        suspend fun registered(
            service: String,
            instance: String,
            vararg commands: String,
            queries: List<String> = emptyList(),
            shared: List<String> = emptyList(),
            permits: Int = 1_000,
        ): RawClient {
            register(service, instance, *commands, queries = queries, shared = shared, permits = permits)
            assertTrue(next().hasRegisterAck())
            return this
        }

        fun reply(id: String, json: String) = send(
            ClientMessage.newBuilder().setReply(Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json))).build(),
        )

        suspend fun next(): ServerMessage = withTimeout(TIMEOUT_MS) { inbox.receive() }

        /** Fails when anything arrives within [ms]. */
        suspend fun nothingWithin(ms: Long) = assertNull(withTimeoutOrNull(ms) { inbox.receive() })
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
        val handler = RawClient().registered("svc-a", "a-1", "test.Ping", permits = 2)
        awaitRegistered("test.Ping")
        handler.sendPermits("a-1", permits(5))
        assertEquals(2, hub.permitsAvailable("a-1"), "clamped at initial_permits")
        handler.sendPermits("a-1", permits(2, grant = 4))
        assertEquals(4, hub.permitsAvailable("a-1"), "the grant raised the cap first")
        handler.sendPermits("a-1", permits(3, grant = 1))
        assertEquals(4, hub.permitsAvailable("a-1"), "a grant never lowers the cap")
    }

    @Test
    fun `a handler gets requests only with permits, highest priority first`() = runBlocking {
        val handler = RawClient().registered("svc-a", "a-1", "test.Ping", permits = 1)
        awaitRegistered("test.Ping")
        val sender = RawClient().registered("svc-b", "b-1")

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
    fun `a full queue is RESOURCE_EXHAUSTED`() = runBlocking {
        hub.queueCapacity = 1
        val handler = RawClient().registered("svc-a", "a-1", "test.Ping", permits = 1)
        awaitRegistered("test.Ping")
        val sender = RawClient().registered("svc-b", "b-1")
        sender.send(request("r1", "test.Ping")) // forwarded
        handler.next()
        sender.send(request("r2", "test.Ping")) // queued
        withTimeout(TIMEOUT_MS) { while (hub.queuedFor("a-1") < 1) kotlinx.coroutines.delay(10) }
        sender.send(request("r3", "test.Ping"))
        val busy = sender.next().error
        assertEquals("r3" to ErrorCode.RESOURCE_EXHAUSTED, busy.requestId to busy.code)
    }

    @Test
    fun `a first Register without permits is INVALID_ARGUMENT, like the real hub`() = runBlocking {
        val client = RawClient().also { it.register("svc-a", "a-1", "test.Ping", permits = 0) }
        val failure = assertIs<StatusException>(client.awaitEnd())
        assertEquals(Status.Code.INVALID_ARGUMENT, failure.status.code)
        assertEquals("Register.initial_permits must be at least 1", failure.status.description)
        assertTrue("test.Ping" !in hub.registeredTypes())
    }

    private suspend fun awaitRegistered(type: String) = withTimeout(TIMEOUT_MS) {
        while (type !in hub.registeredTypes()) kotlinx.coroutines.delay(10)
    }

    @Test
    fun `relays command between two raw clients`() = runBlocking {
        val handler = RawClient().registered("svc-a", "a-1", "test.Ping")
        awaitRegistered("test.Ping")
        assertEquals(setOf("a-1"), hub.connectedInstances())
        val sender = RawClient().registered("svc-b", "b-1")

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
        val sender = RawClient().registered("svc-b", "b-1")
        sender.send(request("r1", "nobody.Owns"))
        val err = sender.next()
        assertTrue(err.hasError())
        assertEquals("r1", err.error.requestId)
        assertEquals(ErrorCode.NO_HANDLER, err.error.code)
    }

    @Test
    fun `dropped handler fails in flight with HANDLER_DISCONNECTED`() = runBlocking {
        val handler = RawClient().registered("svc-a", "a-1", "test.Ping")
        awaitRegistered("test.Ping")
        val sender = RawClient().registered("svc-b", "b-1")
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
        val first = RawClient().registered("svc-a", "a-1", "test.Ping")
        awaitRegistered("test.Ping")
        val sender = RawClient().registered("svc-b", "b-1")
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
        assertTrue(second.next().hasRegisterAck())
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
        val x = RawClient().also { it.register("svc-x", "x-1", shared = listOf("test.Search")) }
        assertTrue(x.next().hasRegisterAck())
        awaitRegistered("test.Search")
        val y = RawClient().also { it.register("svc-y", "y-1", shared = listOf("test.Search")) }
        assertTrue(y.next().hasRegisterAck())

        val exclusive = RawClient().also { it.register("svc-z", "z-1", queries = listOf("test.Search")) }
        val failure = assertIs<StatusException>(exclusive.awaitEnd())
        assertEquals(Status.Code.ALREADY_EXISTS, failure.status.code)

        // Even within one service: x-2 of svc-x registers the type exclusive.
        val sameService = RawClient().also { it.register("svc-x", "x-2", queries = listOf("test.Search")) }
        assertEquals(Status.Code.ALREADY_EXISTS, assertIs<StatusException>(sameService.awaitEnd()).status.code)

        RawClient().also { it.register("svc-e", "e-1", queries = listOf("test.Exclusive")) }.next()
        awaitRegistered("test.Exclusive")
        val sharedOverExclusive = RawClient().also { it.register("svc-f", "f-1", shared = listOf("test.Exclusive")) }
        assertEquals(Status.Code.ALREADY_EXISTS, assertIs<StatusException>(sharedOverExclusive.awaitEnd()).status.code)

        val both = RawClient().also { it.register("svc-g", "g-1", queries = listOf("test.Both"), shared = listOf("test.Both")) }
        assertEquals(Status.Code.INVALID_ARGUMENT, assertIs<StatusException>(both.awaitEnd()).status.code)
    }

    @Test
    fun `scatter-gather sends each target its own unary request and relays parts then the end`() = runBlocking {
        val x1 = RawClient().also { it.register("svc-x", "x-1", shared = listOf("test.Search")) }.apply { next() }
        val x2 = RawClient().also { it.register("svc-x", "x-2", shared = listOf("test.Search")) }.apply { next() }
        val y1 = RawClient().also { it.register("svc-y", "y-1", shared = listOf("test.Search")) }.apply { next() }
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 3) kotlinx.coroutines.delay(10) }
        val sender = RawClient().also { it.register("svc-s", "s-1") }.apply { next() }

        sender.send(scatter("g1", "test.Search"))
        val targets = listOf(x1, x2, y1).map { it.next().request }
        assertEquals(listOf("g1#0", "g1#1", "g1#2"), targets.map { it.id }) // target order: service, then instance
        assertTrue(targets.all { it.mode == RequestMode.UNARY })
        // Each reply is read by the sender before the next one is sent: the three answers travel on three streams, so
        // the hub (like the real one) relays them in whichever order it handles them, not in the order sent here.
        val parts = mutableListOf<ReplyPart>()
        y1.reply("g1#2", "{\"by\":\"y-1\"}")
        parts += sender.next().part
        x1.reply("g1#0", "{\"by\":\"x-1\"}")
        parts += sender.next().part
        x2.send(
            ClientMessage.newBuilder().setReply(
                Reply.newBuilder().setRequestId("g1#1").setError(
                    io.damoreira.kraken.hub.v1.Error.newBuilder().setRequestId("g1#1").setCode(ErrorCode.HANDLER_ERROR).setMessage("boom"),
                ),
            ).build(),
        )
        parts += sender.next().part
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
        val x1 = RawClient().also { it.register("svc-x", "x-1", shared = listOf("test.Search")) }.apply { next() }
        val y1 = RawClient().also { it.register("svc-y", "y-1", shared = listOf("test.Search")) }.apply { next() }
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 2) kotlinx.coroutines.delay(10) }
        val sender = RawClient().also { it.register("svc-s", "s-1") }.apply { next() }

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
    fun `scatter-gather refusals - no target, a command`() = runBlocking {
        val sender = RawClient().registered("svc-s", "s-1")
        sender.send(scatter("g1", "test.Nobody"))
        assertEquals(ErrorCode.NO_HANDLER, sender.next().error.code)

        RawClient().also { it.register("svc-c", "c-1", "test.Cmd") }
        awaitRegistered("test.Cmd")
        sender.send(scatter("g2", "test.Cmd"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code)
    }

    // --- streaming queries (P4b) ---

    private fun stream(id: String, type: String, timeoutMs: Int = 0, credit: Int = 0, maxResults: Int = 0) = ClientMessage.newBuilder().setRequest(
        Request.newBuilder().setId(id).setKind(RequestKind.QUERY).setType(type).setMode(RequestMode.STREAMING)
            .setTimeoutMs(timeoutMs).setInitialCredit(credit).setMaxResults(maxResults).setPayload(ByteString.copyFromUtf8("{\"q\":1}")),
    ).build()

    private fun part(id: String, seq: Long, json: String) = ClientMessage.newBuilder()
        .setPart(ReplyPart.newBuilder().setRequestId(id).setSeq(seq).setPayload(ByteString.copyFromUtf8(json))).build()

    private fun errorPart(id: String, seq: Long, message: String) = ClientMessage.newBuilder()
        .setPart(ReplyPart.newBuilder().setRequestId(id).setSeq(seq).setError(Error.newBuilder().setRequestId(id).setCode(ErrorCode.HANDLER_ERROR).setMessage(message))).build()

    private fun end(id: String, error: Error? = null) = ClientMessage.newBuilder()
        .setEnd(ReplyEnd.newBuilder().setRequestId(id).apply { if (error != null) setError(error) }).build()

    private fun flow(id: String, credits: Int) = ClientMessage.newBuilder().setFlow(Flow.newBuilder().setRequestId(id).setCredits(credits)).build()

    private fun cancel(id: String, reason: String) = ClientMessage.newBuilder().setCancel(Cancel.newBuilder().setRequestId(id).setReason(reason)).build()

    private suspend fun streamingPair(): Pair<RawClient, RawClient> {
        val handler = RawClient().also { it.register("svc-h", "h-1", queries = listOf("test.Export")) }.apply { next() }
        awaitRegistered("test.Export")
        val sender = RawClient().also { it.register("svc-s", "s-1") }.apply { next() }
        return handler to sender
    }

    @Test
    fun `a streaming request goes to one instance and its parts flow back under the sender's credit`() = runBlocking {
        val (handler, sender) = streamingPair()
        sender.send(stream("s1", "test.Export", timeoutMs = 5_000, credit = 2))
        val forwarded = handler.next().request
        assertEquals("s1#1", forwarded.id)
        assertEquals(RequestMode.STREAMING, forwarded.mode)
        assertEquals(2, forwarded.initialCredit)
        assertEquals(0, forwarded.maxResults)
        assertEquals("{\"q\":1}", forwarded.payload.toStringUtf8())
        assertEquals(2L, hub.creditOf("s1"))

        handler.send(part("s1#1", 1, "{\"row\":1}"))
        handler.send(part("s1#1", 2, "{\"row\":2}"))
        val first = sender.next().part
        assertEquals(listOf("s1", "svc-h", "h-1", 1L, "{\"row\":1}"), listOf(first.requestId, first.responderService, first.responderInstance, first.seq, first.payload.toStringUtf8()))
        assertEquals(2L, sender.next().part.seq)
        assertEquals(0L, hub.creditOf("s1"))
        handler.nothingWithin(100) // no credit left: nothing is relayed, the handler waits

        sender.send(flow("s1", 3))
        val relayed = handler.next().flow
        assertEquals("s1#1" to 3, relayed.requestId to relayed.credits)
        assertEquals(listOf(3), hub.flowsFrom("s-1"))
        handler.send(part("s1#1", 3, "{\"row\":3}"))
        handler.send(end("s1#1"))
        assertEquals(3L, sender.next().part.seq)
        val ended = sender.next().end
        assertEquals("s1", ended.requestId)
        assertEquals(3, ended.parts)
        assertEquals(false, ended.hasError())
        assertEquals(0, hub.openStreams())
        handler.send(part("s1#1", 4, "{}")) // after the end: ignored
        sender.nothingWithin(100)
        handler.nothingWithin(50)

        // Credit 0 reads as 64, as on the real hub.
        sender.send(stream("s2", "test.Export"))
        assertEquals(64, handler.next().request.initialCredit)
    }

    @Test
    fun `a part beyond the credit, max_results, an error part and an end with error end the stream`() = runBlocking {
        val (handler, sender) = streamingPair()
        sender.send(stream("s1", "test.Export", credit = 1))
        handler.next()
        handler.send(part("s1#1", 1, "{}"))
        handler.send(part("s1#1", 2, "{}")) // beyond the credit
        assertTrue(sender.next().hasPart())
        val invalid = sender.next().end
        assertEquals(ErrorCode.INVALID_REQUEST, invalid.error.code)
        assertEquals(1, invalid.parts)
        val cancelled = handler.next().cancel
        assertEquals("s1#1" to "part without credit", cancelled.requestId to cancelled.reason)

        sender.send(stream("s2", "test.Export", credit = 8, maxResults = 2))
        assertEquals(0, handler.next().request.maxResults) // the hub enforces the cap
        handler.send(part("s2#1", 1, "{}"))
        handler.send(part("s2#1", 2, "{}"))
        handler.send(part("s2#1", 3, "{}"))
        sender.next(); sender.next()
        val capped = sender.next().end
        assertEquals(2, capped.parts)
        assertEquals(false, capped.hasError())
        assertEquals("max results", handler.next().cancel.reason)
        sender.nothingWithin(100) // the third part was dropped

        sender.send(stream("s3", "test.Export", credit = 8))
        handler.next()
        handler.send(part("s3#1", 1, "{}"))
        handler.send(errorPart("s3#1", 2, "boom"))
        assertTrue(sender.next().hasPart())
        val failed = sender.next().end
        assertEquals(ErrorCode.HANDLER_ERROR to "boom", failed.error.code to failed.error.message)
        assertEquals(1, failed.parts)
        handler.nothingWithin(100) // the handler ended itself: no cancel

        sender.send(stream("s4", "test.Export", credit = 8))
        handler.next()
        handler.send(end("s4#1", Error.newBuilder().setRequestId("s4#1").setCode(ErrorCode.HANDLER_ERROR).setMessage("late failure").build()))
        val endError = sender.next().end
        assertEquals("late failure", endError.error.message)
        assertEquals(0, endError.parts)

        // A unary Reply to a streaming target answers no unary request: ignored, like the real hub.
        sender.send(stream("s5", "test.Export", credit = 8))
        handler.next()
        handler.reply("s5#1", "{\"one\":true}")
        sender.nothingWithin(100)
        assertEquals(1, hub.openStreams())
        handler.send(end("s5#1"))
        assertEquals(0, sender.next().end.parts)
    }

    @Test
    fun `the idle timeout and the max duration end the stream and cancel the handler, a sender's cancel is relayed silently`() = runBlocking {
        val (handler, sender) = streamingPair()
        sender.send(stream("s1", "test.Export", timeoutMs = 300, credit = 8))
        handler.next()
        handler.send(part("s1#1", 1, "{}")) // resets the idle timer
        assertTrue(sender.next().hasPart())
        val started = System.nanoTime()
        val timedOut = sender.next().end
        assertEquals(ErrorCode.TIMEOUT, timedOut.error.code)
        assertEquals(1, timedOut.parts)
        assertTrue(System.nanoTime() - started >= 200_000_000L)
        assertEquals("idle timeout", handler.next().cancel.reason)

        hub.maxStreamDuration = java.time.Duration.ofMillis(300)
        sender.send(stream("s2", "test.Export", timeoutMs = 5_000, credit = 8))
        handler.next()
        val overdue = sender.next().end
        assertEquals(ErrorCode.CANCELLED, overdue.error.code)
        assertTrue(overdue.error.message.startsWith("max stream duration"), overdue.error.message)
        assertTrue(handler.next().cancel.reason.startsWith("max stream duration"))
        hub.maxStreamDuration = java.time.Duration.ofMinutes(10)

        sender.send(stream("s3", "test.Export", timeoutMs = 5_000, credit = 8))
        handler.next()
        sender.send(cancel("s3", "user left"))
        val cancelled = handler.next().cancel
        assertEquals("s3#1" to "user left", cancelled.requestId to cancelled.reason)
        assertEquals(listOf("s3" to "user left"), hub.cancelsFrom("s-1").map { it.requestId to it.reason })
        sender.nothingWithin(200)
        assertEquals(0, hub.openStreams())
        handler.send(part("s3#1", 1, "{}")) // after the cancel: ignored
        sender.nothingWithin(100)
    }

    @Test
    fun `streaming refusals follow the gate order, an unknown mode is INVALID_REQUEST, other sessions' messages are ignored`() = runBlocking {
        val sender = RawClient().registered("svc-s", "s-1")
        sender.send(stream("r3", "test.Export"))
        assertEquals(ErrorCode.NO_HANDLER, sender.next().error.code)

        RawClient().also { it.register("svc-c", "c-1", "test.Cmd") }
        awaitRegistered("test.Cmd")
        sender.send(stream("r4", "test.Cmd"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code)

        val handler = RawClient().registered("svc-h", "h-1", queries = listOf("test.Export"))
        val other = RawClient().registered("svc-o", "o-1")
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 4) kotlinx.coroutines.delay(10) }
        sender.send(
            ClientMessage.newBuilder().setRequest(
                Request.newBuilder().setId("r5").setKind(RequestKind.QUERY).setType("test.Export").setModeValue(99).setPayload(ByteString.copyFromUtf8("{}")),
            ).build(),
        )
        val unknown = sender.next().error
        assertEquals(ErrorCode.INVALID_REQUEST to "invalid request: unknown request mode 99", unknown.code to unknown.message)

        repeat(2) { sender.send(stream("r6-$it", "test.Export", timeoutMs = 5_000)) }
        repeat(2) { handler.next() }
        sender.send(stream("r6-0", "test.Export"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code) // duplicate id
        sender.send(stream("r6-0#1", "test.Export"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code) // duplicate target id

        // Flow and Cancel from a session that is not the stream's sender are ignored; so are parts from a non-target.
        other.send(flow("r6-0", 5))
        other.send(cancel("r6-0", "nope"))
        other.send(part("r6-0#1", 1, "{}"))
        handler.nothingWithin(100)
        sender.nothingWithin(50)
        assertEquals(2, hub.openStreams())
    }

    @Test
    fun `the handler leaving ends the stream with HANDLER_DISCONNECTED and the sender leaving cancels the handler`() = runBlocking {
        val (handler, sender) = streamingPair()
        sender.send(stream("s1", "test.Export", timeoutMs = 5_000, credit = 8))
        handler.next()
        handler.send(part("s1#1", 1, "{}"))
        assertTrue(sender.next().hasPart())
        hub.dropInstance("h-1")
        val lost = sender.next().end
        assertEquals(ErrorCode.HANDLER_DISCONNECTED, lost.error.code)
        assertEquals(1, lost.parts)

        val handler2 = RawClient().also { it.register("svc-h", "h-2", queries = listOf("test.Export")) }.apply { next() }
        awaitRegistered("test.Export")
        sender.send(stream("s2", "test.Export", timeoutMs = 5_000, credit = 8))
        handler2.next()
        hub.dropInstance("s-1")
        val cancelled = handler2.next().cancel
        assertEquals("s2#1" to "sender disconnected", cancelled.requestId to cancelled.reason)
        assertEquals(0, hub.openStreams())
    }

    @Test
    fun `a gather that ends early cancels its forwarded targets`() = runBlocking {
        val x = RawClient().registered("svc-x", "x-1", shared = listOf("test.Search"))
        val y = RawClient().registered("svc-y", "y-1", shared = listOf("test.Search"))
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 2) kotlinx.coroutines.delay(10) }
        val sender = RawClient().registered("svc-s", "s-1")

        sender.send(scatter("g1", "test.Search", maxResults = 1))
        x.next(); y.next()
        y.reply("g1#1", "{}")
        assertTrue(sender.next().hasPart())
        assertEquals(1, sender.next().end.parts)
        assertEquals("g1#0" to "max results", x.next().cancel.let { it.requestId to it.reason })
        y.nothingWithin(100) // it answered: nothing to cancel

        sender.send(scatter("g2", "test.Search", timeoutMs = 200))
        x.next(); y.next()
        assertEquals(2, sender.next().end.timedOut) // both missed the deadline
        assertEquals("g2#0" to "deadline", x.next().cancel.let { it.requestId to it.reason })
        assertEquals("g2#1" to "deadline", y.next().cancel.let { it.requestId to it.reason })

        sender.send(scatter("g3", "test.Search", timeoutMs = 5_000))
        x.next(); y.next()
        hub.dropInstance("s-1")
        assertEquals("sender disconnected", x.next().cancel.reason)
        assertEquals("sender disconnected", y.next().cancel.reason)
    }

    // --- subscription queries (P4c) ---

    private fun subscribe(id: String, type: String, timeoutMs: Int = 5_000, credit: Int = 0, maxResults: Int = 0) = ClientMessage.newBuilder().setRequest(
        Request.newBuilder().setId(id).setKind(RequestKind.QUERY).setType(type).setMode(RequestMode.SUBSCRIPTION)
            .setTimeoutMs(timeoutMs).setInitialCredit(credit).setMaxResults(maxResults).setPayload(ByteString.copyFromUtf8("{\"account\":7}")),
    ).build()

    /** The requests [this] gets until nothing arrives for 150 ms, by id. */
    private suspend fun RawClient.requests(): Map<String, Request> {
        val out = mutableMapOf<String, Request>()
        while (true) {
            val m = withTimeoutOrNull(150) { inbox.receive() } ?: break
            assertTrue(m.hasRequest(), m.toString())
            out[m.request.id] = m.request
        }
        return out
    }

    /** Two handlers of the shared `acct.Watch` (svc-a / a-1 and svc-b / b-1) and a sender. */
    private suspend fun subscriptionTrio(): Triple<RawClient, RawClient, RawClient> {
        val a = RawClient().also { it.register("svc-a", "a-1", shared = listOf("acct.Watch")) }.apply { next() }
        val b = RawClient().also { it.register("svc-b", "b-1", shared = listOf("acct.Watch")) }.apply { next() }
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 2) kotlinx.coroutines.delay(10) }
        val sender = RawClient().also { it.register("svc-web", "s-1") }
        assertTrue(sender.next().hasRegisterAck())
        return Triple(a, b, sender)
    }

    /** Opens subscription [id] on `acct.Watch`: both handlers get their update target, one of them the initial request. */
    private suspend fun open(sender: RawClient, a: RawClient, b: RawClient, id: String, credit: Int = 0, maxResults: Int = 0, timeoutMs: Int = 5_000): Opened {
        sender.send(subscribe(id, "acct.Watch", timeoutMs, credit, maxResults))
        val ra = a.requests()
        val rb = b.requests()
        val initialOnA = "$id#0" in ra
        assertTrue(initialOnA != ("$id#0" in rb), "exactly one initial request: a $ra, b $rb")
        fun target(rs: Map<String, Request>): String {
            val t = rs.values.single { it.mode == RequestMode.SUBSCRIPTION }
            assertEquals(0 to 0, t.initialCredit to t.maxResults)
            assertEquals("{\"account\":7}", t.payload.toStringUtf8())
            return t.id
        }
        val initial = (if (initialOnA) ra else rb).getValue("$id#0")
        assertEquals(RequestMode.UNARY, initial.mode)
        assertEquals(timeoutMs, initial.timeoutMs)
        return Opened(if (initialOnA) a else b, target(ra), target(rb))
    }

    private class Opened(val initial: RawClient, val ta: String, val tb: String)

    @Test
    fun `a subscription reaches every target, one answers the initial, updates are renumbered, the last clean end ends it`() = runBlocking {
        val (a, b, sender) = subscriptionTrio()
        val opened = open(sender, a, b, "sub1")
        assertEquals("sub1#1" to "sub1#2", opened.ta to opened.tb)
        assertEquals(1, hub.openSubscriptions())
        assertEquals(1, hub.subscriptionTargetsOf("a-1"))
        assertEquals(64L, hub.subscriptionCreditOf("sub1")) // credit 0 reads as 64
        opened.initial.reply("sub1#0", "{\"balance\":10}")
        val p1 = sender.next().part
        assertEquals(listOf("sub1", 1L, "{\"balance\":10}"), listOf(p1.requestId, p1.seq, p1.payload.toStringUtf8()))
        assertEquals(if (opened.initial === a) "svc-a" else "svc-b", p1.responderService)
        assertEquals(63L, hub.subscriptionCreditOf("sub1"))
        a.send(part(opened.ta, 1, "{\"balance\":11}"))
        val p2 = sender.next().part
        assertEquals(listOf(2L, "svc-a", "a-1"), listOf(p2.seq, p2.responderService, p2.responderInstance))
        b.send(part(opened.tb, 1, "{\"balance\":12}"))
        assertEquals(3L to "svc-b", sender.next().part.let { it.seq to it.responderService })
        a.send(end(opened.ta))
        sender.nothingWithin(150) // b still serves
        assertEquals(1, hub.openSubscriptions())
        b.send(end(opened.tb))
        val ended = sender.next().end
        assertEquals(listOf("sub1", 3, 0, 0), listOf(ended.requestId, ended.parts, ended.failed, ended.timedOut))
        assertEquals(false, ended.hasError())
        assertEquals(0, hub.openSubscriptions())
        a.send(part(opened.ta, 2, "{}")) // late: ignored
        sender.nothingWithin(100)
        a.nothingWithin(50); b.nothingWithin(50) // nobody was cancelled
    }

    @Test
    fun `updates before the initial wait behind it, a Flow drains the buffer, a Cancel reaches every target`() = runBlocking {
        val (a, b, sender) = subscriptionTrio()
        val opened = open(sender, a, b, "sub1", credit = 1)
        a.send(part(opened.ta, 1, "{\"u\":1}"))
        // Two streams reach the hub concurrently: the order of their updates is only defined once the first one has arrived.
        withTimeout(TIMEOUT_MS) { while (hub.bufferedUpdatesOf("sub1") < 1) kotlinx.coroutines.delay(5) }
        b.send(part(opened.tb, 1, "{\"u\":2}"))
        sender.nothingWithin(150) // nothing before the initial
        assertEquals(2, hub.bufferedUpdatesOf("sub1"))
        opened.initial.reply("sub1#0", "{\"initial\":true}")
        assertEquals(1L, sender.next().part.seq)
        sender.nothingWithin(100) // no credit left
        sender.send(flow("sub1", 1))
        val p2 = sender.next().part
        assertEquals(2L to "{\"u\":1}", p2.seq to p2.payload.toStringUtf8())
        sender.send(flow("sub1", 10))
        assertEquals(3L to "{\"u\":2}", sender.next().part.let { it.seq to it.payload.toStringUtf8() })
        assertEquals(9L, hub.subscriptionCreditOf("sub1"))
        a.nothingWithin(100); b.nothingWithin(50) // no Flow reaches a target
        sender.send(cancel("sub1", "user left"))
        assertEquals(opened.ta to "user left", a.next().cancel.let { it.requestId to it.reason })
        assertEquals(opened.tb to "user left", b.next().cancel.let { it.requestId to it.reason })
        sender.nothingWithin(150) // nothing more to the sender
        assertEquals(0, hub.openSubscriptions())
        assertEquals(listOf("sub1" to "user left"), hub.cancelsFrom("s-1").map { it.requestId to it.reason })
    }

    @Test
    fun `the buffer limit and max_results end the subscription and cancel the targets`() = runBlocking {
        hub.subscriptionBuffer = 2
        val (a, b, sender) = subscriptionTrio()
        val opened = open(sender, a, b, "sub1", credit = 1)
        opened.initial.reply("sub1#0", "{}")
        assertEquals(1L, sender.next().part.seq)
        repeat(3) { a.send(part(opened.ta, it + 1L, "{}")) }
        val overflow = sender.next().end
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED to 1, overflow.error.code to overflow.parts)
        assertEquals("subscriber buffer (2) exceeded", overflow.error.message)
        assertEquals(opened.ta to "subscriber buffer exceeded", a.next().cancel.let { it.requestId to it.reason })
        assertEquals(opened.tb, b.next().cancel.requestId)
        assertEquals(0, hub.openSubscriptions())

        val second = open(sender, a, b, "sub2", maxResults = 1)
        second.initial.reply("sub2#0", "{}")
        assertEquals(1L, sender.next().part.seq) // the initial part does not count
        b.send(part(second.tb, 1, "{}"))
        b.send(part(second.tb, 2, "{}"))
        assertEquals(2L, sender.next().part.seq)
        val capped = sender.next().end
        assertEquals(false, capped.hasError())
        assertEquals(2, capped.parts)
        assertEquals(second.ta to "max results", a.next().cancel.let { it.requestId to it.reason })
        assertEquals(second.tb to "max results", b.next().cancel.let { it.requestId to it.reason })
        sender.nothingWithin(100) // the extra update was dropped
    }

    @Test
    fun `an error from a target or the initial request ends the whole subscription, the max duration too`() = runBlocking {
        val (a, b, sender) = subscriptionTrio()
        // An error end from a target: ReplyEnd{error} to the sender, Cancel to the rest (the failed one ended itself).
        val first = open(sender, a, b, "sub1")
        first.initial.reply("sub1#0", "{}")
        assertEquals(1L, sender.next().part.seq)
        a.send(end(first.ta, Error.newBuilder().setRequestId(first.ta).setCode(ErrorCode.HANDLER_ERROR).setMessage("emitter failed").build()))
        val failed = sender.next().end
        assertEquals(ErrorCode.HANDLER_ERROR to "emitter failed", failed.error.code to failed.error.message)
        assertEquals(first.tb to "target a-1 failed", b.next().cancel.let { it.requestId to it.reason })
        a.nothingWithin(100)
        // A part carrying an error: the same, the part is not relayed.
        val second = open(sender, a, b, "sub2")
        second.initial.reply("sub2#0", "{}")
        assertEquals(1L, sender.next().part.seq)
        b.send(errorPart(second.tb, 1, "boom"))
        assertEquals("boom", sender.next().end.error.message)
        assertEquals(second.ta, a.next().cancel.requestId)
        b.nothingWithin(100)
        // The initial request failing (Reply{error}, or no reply within timeout_ms): no part, the targets are cancelled.
        val third = open(sender, a, b, "sub3")
        third.initial.send(ClientMessage.newBuilder().setReply(Reply.newBuilder().setRequestId("sub3#0").setError(Error.newBuilder().setRequestId("sub3#0").setCode(ErrorCode.HANDLER_ERROR).setMessage("no such account"))).build())
        val initialFailed = sender.next().end
        assertEquals(ErrorCode.HANDLER_ERROR to 0, initialFailed.error.code to initialFailed.parts)
        assertEquals(third.ta to "initial result failed", a.next().cancel.let { it.requestId to it.reason })
        assertEquals(third.tb to "initial result failed", b.next().cancel.let { it.requestId to it.reason })
        val fourth = open(sender, a, b, "sub4", timeoutMs = 800)
        a.send(part(fourth.ta, 1, "{}")) // buffered behind an initial that never comes
        assertEquals(ErrorCode.TIMEOUT, sender.next().end.error.code)
        // The targets are cancelled; the timed-out initial request is not (its entry is gone, a late reply is dropped), as on the real hub.
        assertEquals(fourth.ta to "initial result failed", a.next().cancel.let { it.requestId to it.reason })
        assertEquals(fourth.tb to "initial result failed", b.next().cancel.let { it.requestId to it.reason })
        a.nothingWithin(100); b.nothingWithin(50)
        assertEquals(0, hub.openSubscriptions())
        // The max duration (off by default): CANCELLED and a cancel with that reason.
        hub.maxSubscriptionDuration = java.time.Duration.ofMillis(800)
        val fifth = open(sender, a, b, "sub5")
        fifth.initial.reply("sub5#0", "{}")
        assertEquals(1L, sender.next().part.seq)
        val overdue = sender.next().end
        assertEquals(ErrorCode.CANCELLED, overdue.error.code)
        assertEquals("max subscription duration (800 ms) exceeded", overdue.error.message)
        assertTrue(a.next().cancel.reason.startsWith("max subscription duration"))
        assertTrue(b.next().cancel.reason.startsWith("max subscription duration"))
    }

    @Test
    fun `a target leaving is silent, the last one HANDLER_DISCONNECTED, the sender leaving cancels every target`() = runBlocking {
        val (a, b, sender) = subscriptionTrio()
        val first = open(sender, a, b, "sub1")
        first.initial.reply("sub1#0", "{}")
        assertEquals(1L, sender.next().part.seq)
        hub.dropInstance("a-1")
        sender.nothingWithin(150) // b still serves
        assertEquals(0, hub.subscriptionTargetsOf("a-1"))
        b.send(part(first.tb, 1, "{}"))
        assertEquals(2L, sender.next().part.seq)
        hub.dropInstance("b-1")
        val lost = sender.next().end
        assertEquals(ErrorCode.HANDLER_DISCONNECTED to 2, lost.error.code to lost.parts)
        assertEquals(0, hub.openSubscriptions())

        val a2 = RawClient().also { it.register("svc-a", "a-2", shared = listOf("acct.Watch")) }.apply { next() }
        val b2 = RawClient().also { it.register("svc-b", "b-2", shared = listOf("acct.Watch")) }.apply { next() }
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 3) kotlinx.coroutines.delay(10) }
        val second = open(sender, a2, b2, "sub2")
        hub.dropInstance("s-1")
        val cancels = listOf(a2.next().cancel, b2.next().cancel, second.initial.next().cancel).map { it.requestId to it.reason }.sortedBy { it.first }
        assertEquals(listOf("sub2#0", second.ta, second.tb).sorted().map { it to "sender disconnected" }, cancels)
        assertEquals(0, hub.openSubscriptions())
    }

    @Test
    fun `subscription refusals follow the gate order`() = runBlocking {
        val sender = RawClient().registered("svc-s", "s-1")
        sender.send(subscribe("r3", "acct.Watch"))
        assertEquals(ErrorCode.NO_HANDLER, sender.next().error.code)

        RawClient().also { it.register("svc-c", "c-1", "test.Cmd") }
        awaitRegistered("test.Cmd")
        sender.send(subscribe("r4", "test.Cmd"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code)

        val handler = RawClient().registered("svc-h", "h-1", queries = listOf("acct.Watch"))
        val other = RawClient().registered("svc-o", "o-1")
        withTimeout(TIMEOUT_MS) { while (hub.connectedInstances().size < 4) kotlinx.coroutines.delay(10) }
        sender.send(subscribe("r6", "acct.Watch"))
        assertEquals(setOf("r6#1", "r6#0"), handler.requests().keys)
        other.nothingWithin(50)
        sender.send(subscribe("r6", "acct.Watch"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code) // duplicate id
        sender.send(request("r6#0", "acct.Watch"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.next().error.code) // the initial request's id is pending
        // An update target's id is no pending id: a unary request with it is routed normally (INVALID_REQUEST for a command kind on a query type is not checked by this double: NO_HANDLER is what the handler answers).
        sender.send(request("r6#1", "acct.Watch"))
        assertEquals("r6#1", handler.next().request.id)

        // The per-instance limit: refused before anything is sent.
        hub.maxSubscriptionsPerInstance = 1
        sender.send(subscribe("r7", "acct.Watch"))
        val limited = sender.next().error
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED, limited.code)
        assertTrue("h-1" in limited.message, limited.message)
        handler.nothingWithin(100)
        hub.maxSubscriptionsPerInstance = 1024

        // A unary Reply to an update target is ignored; a part from a non-target is ignored.
        handler.reply("r6#1", "{}")
        other.send(part("r6#1", 1, "{}"))
        sender.nothingWithin(100)
        assertEquals(1, hub.openSubscriptions())
    }

    private companion object {
        const val TIMEOUT_MS = 5_000L
    }
}
