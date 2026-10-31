package io.damoreira.kraken.messaging.test

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
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

        suspend fun next(): ServerMessage = withTimeout(TIMEOUT_MS) { inbox.receive() }
        suspend fun awaitEnd(): Throwable? = withTimeout(TIMEOUT_MS) { ended.await() }
    }

    private fun request(id: String, type: String, headers: Map<String, String> = emptyMap(), priority: Int = 0) =
        ClientMessage.newBuilder().setRequest(
            Request.newBuilder().setId(id).setKind(RequestKind.COMMAND).setType(type).setPriority(priority)
                .setPayload(ByteString.copyFromUtf8("{}")).putAllHeaders(headers),
        ).build()

    private fun permits(count: Int) =
        ClientMessage.newBuilder().setPermits(io.damoreira.kraken.hub.v1.Permits.newBuilder().setCount(count)).build()

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

        handler.send(permits(2))
        assertEquals("high", handler.next().request.id)
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

    private companion object {
        const val TIMEOUT_MS = 5_000L
    }
}
