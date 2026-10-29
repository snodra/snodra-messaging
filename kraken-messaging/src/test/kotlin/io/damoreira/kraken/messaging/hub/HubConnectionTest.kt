package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.test.InProcessHub
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class HubConnectionTest {
    private var hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val fast = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(200),
    )
    private val wait = Duration.ofSeconds(5)

    @AfterEach fun tearDown() = runBlocking {
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun connection(
        instanceId: String,
        serviceName: String = "svc-$instanceId",
        settings: HubSettings = fast,
        hubName: String = hub.name,
        inbound: suspend (Request) -> Reply = { req -> errorReply(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
    ) = HubConnection({ InProcessChannelBuilder.forName(hubName).build() }, serviceName, instanceId, settings, inbound, scope)
        .also { connections += it }

    private fun request(type: String = "test.Ping", timeoutMs: Int = 2000) = Request.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setKind(RequestKind.COMMAND)
        .setType(type)
        .setPayload(ByteString.copyFromUtf8("{\"text\":\"x\"}"))
        .setTimeoutMs(timeoutMs)
        .build()

    @Test fun `registers on connect and hub sees types`() = runBlocking {
        val conn = connection("a")
        conn.register(setOf("test.Ping"), setOf("test.GetCount"))
        conn.start()

        assertTrue(conn.awaitRegistered(wait))
        assertTrue(conn.connected.value)
        assertEquals(setOf("test.Ping", "test.GetCount"), hub.registeredTypes())
        assertEquals(setOf("a"), hub.connectedInstances())
    }

    @Test fun `send gets reply from another connection`() = runBlocking {
        val handler = connection("b") { req -> payloadReply(req.id, "{\"echoed\":\"x\"}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait))
        assertTrue(sender.awaitRegistered(wait))

        val response = withTimeout(5_000) { sender.send(request()) }

        assertTrue(response.hasReply())
        assertEquals("{\"echoed\":\"x\"}", response.reply.payload.toStringUtf8())
    }

    @Test fun `send without hub fails with HubUnavailableException within timeout`() = runBlocking {
        val conn = connection("a", settings = fast.copy(requestTimeout = Duration.ofMillis(300)), hubName = "no-such-hub-" + UUID.randomUUID())
        conn.start()

        val started = System.nanoTime()
        assertFailsWith<HubUnavailableException> { withTimeout(5_000) { conn.send(request()) } }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)

        assertTrue(elapsed < Duration.ofSeconds(2), "took $elapsed")
        assertFalse(conn.connected.value)
    }

    @Test fun `reconnects and re-registers after hub restart`() = runBlocking {
        val conn = connection("a")
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        hub.close()
        withTimeout(5_000) { conn.connected.first { !it } }
        hub = InProcessHub(hub.name).start()

        withTimeout(5_000) { conn.connected.first { it } }
        assertTrue(conn.awaitRegistered(wait))
        assertEquals(setOf("test.Ping"), hub.registeredTypes())
    }

    @Test fun `in flight request fails with HandlerDisconnected when handler dropped`() = runBlocking {
        val received = CompletableDeferred<Unit>()
        val handler = connection("b") { received.complete(Unit); awaitCancellation() }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait))
        assertTrue(sender.awaitRegistered(wait))

        val response = async { sender.send(request(timeoutMs = 4_000)) }
        withTimeout(5_000) { received.await() }
        hub.dropInstance("b")

        val result = withTimeout(5_000) { response.await() }
        assertTrue(result.hasError())
        assertEquals(ErrorCode.HANDLER_DISCONNECTED, result.error.code)
    }

    @Test fun `already exists rejection is exposed`() = runBlocking {
        val owner = connection("a", serviceName = "svc-owner")
        owner.register(setOf("test.Ping"), emptySet())
        owner.start()
        assertTrue(owner.awaitRegistered(wait))

        val intruder = connection("b", serviceName = "svc-intruder")
        intruder.register(setOf("test.Ping"), emptySet())
        intruder.start()

        val rejection = withTimeout(5_000) { intruder.lastRejection.first { it != null } }
        assertEquals(Status.Code.ALREADY_EXISTS, rejection!!.code)
        assertFalse(intruder.connected.value)
        assertEquals(setOf("a"), hub.connectedInstances())
    }

    @Test fun `heartbeats are sent`() = runBlocking {
        val conn = connection("a", settings = fast.copy(heartbeatInterval = Duration.ofMillis(50)))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        await atMost Duration.ofMillis(500) until { hub.heartbeatsFrom("a") >= 3 }
    }

    @Test fun `throwing channel factory is retried with backoff`() = runBlocking {
        val calls = AtomicInteger()
        val conn = HubConnection(
            channelFactory = {
                val call = calls.incrementAndGet()
                if (call <= 2) throw IllegalArgumentException("bad target $call")
                InProcessChannelBuilder.forName(hub.name).build()
            },
            serviceName = "svc-flaky",
            instanceId = "flaky",
            settings = fast,
            inbound = { req -> errorReply(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
            scope = scope,
        ).also { connections += it }
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()

        assertTrue(conn.awaitRegistered(wait))
        assertEquals(3, calls.get())
        assertEquals(setOf("test.Ping"), hub.registeredTypes())
    }

    @Test fun `cancellation leaking from a handler is answered as HANDLER_ERROR`() = runBlocking {
        val handler = connection("b") { withTimeout(10) { delay(5_000) }; payloadReply(it.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait))
        assertTrue(sender.awaitRegistered(wait))

        val response = withTimeout(5_000) { sender.send(request(timeoutMs = 4_000)) }

        assertTrue(response.hasReply() && response.reply.hasError(), response.toString())
        assertEquals(ErrorCode.HANDLER_ERROR, response.reply.error.code)
        assertTrue(response.reply.error.message.startsWith("TimeoutCancellationException: "), response.reply.error.message)
    }

    private fun payloadReply(id: String, json: String): Reply =
        Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun errorReply(id: String, code: ErrorCode, message: String): Reply =
        Reply.newBuilder().setRequestId(id).setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message)).build()
}
