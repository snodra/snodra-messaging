package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.test.InProcessHub
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
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
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.random.Random
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class HubConnectionTest {
    private var hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val virtualThreads = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
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
        virtualThreads.close()
        hub.close()
    }

    private fun connection(
        instanceId: String,
        serviceName: String = "svc-$instanceId",
        settings: HubSettings = fast,
        hubName: String = hub.name,
        handlerDispatcher: CoroutineDispatcher = kotlinx.coroutines.Dispatchers.IO,
        inbound: suspend (Request) -> Reply = { req -> errorReply(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
    ) = HubConnection(
        { InProcessChannelBuilder.forName(hubName).build() }, serviceName, instanceId, settings, inbound, scope, handlerDispatcher,
    ).also { connections += it }

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
        val e = assertFailsWith<HubRegistrationRejectedException> { intruder.awaitRegistered(wait) }
        assertTrue("ALREADY_EXISTS" in e.message!!, e.message)
    }

    /**
     * Regression: the connection scope runs on Dispatchers.Default (as in the auto-configuration). B's handler blocks
     * its thread on a nested send to C whose reply only B's stream collector can deliver. With handlers on the
     * connection scope, more blocked handlers than Default threads starved the collector and the nested sends timed out.
     */
    @Test fun nested_blocking_send_inside_handler_does_not_starve_connection() = runBlocking {
        val settings = fast.copy(requestTimeout = Duration.ofSeconds(3))
        val inner = connection("c", settings = settings) { req -> payloadReply(req.id, "{}") }
        inner.register(setOf("test.Inner"), emptySet())
        inner.start()
        lateinit var middle: HubConnection
        middle = connection("b", settings = settings, handlerDispatcher = virtualThreads) { outer ->
            // A non-suspend handler calling sendBlocking: blocks the handler thread until the nested reply arrives.
            val nested = runBlocking { middle.send(request("test.Inner", timeoutMs = 3_000)) }
            check(nested.hasReply() && !nested.reply.hasError()) { "nested send failed: $nested" }
            payloadReply(outer.id, "{}")
        }
        middle.register(setOf("test.Outer"), emptySet())
        middle.start()
        val sender = connection("a", settings = settings)
        sender.start()
        for (c in listOf(inner, middle, sender)) assertTrue(c.awaitRegistered(wait))

        val n = maxOf(20, Runtime.getRuntime().availableProcessors() * 2) // more blocked handlers than Default threads
        val started = System.nanoTime()
        val results = (1..n).map {
            async(kotlinx.coroutines.Dispatchers.IO) { runCatching { sender.send(request("test.Outer", timeoutMs = 3_000)) } }
        }.awaitAll()
        val elapsed = Duration.ofNanos(System.nanoTime() - started)

        val failures = results.filter { r -> r.getOrNull()?.let { it.hasReply() && !it.reply.hasError() } != true }
        assertEquals(emptyList(), failures.map { it.exceptionOrNull()?.toString() ?: it.getOrNull().toString() })
        assertTrue(elapsed < Duration.ofSeconds(3), "took $elapsed")
        assertEquals(0, middle.inFlightHandlers.value)
    }

    @Test fun missing_heartbeat_echoes_trigger_reconnect() = runBlocking {
        hub.heartbeatInterval = Duration.ofMillis(100) // announced in the ack; the 10 s fallback is not used
        val conn = connection("a")
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        assertEquals(1, hub.registrationsFrom("a"))

        hub.echoHeartbeats = false
        withTimeout(3_000) { conn.connected.first { !it } } // > 3 intervals without an echo: stream failed
        hub.echoHeartbeats = true

        withTimeout(3_000) { conn.connected.first { it } }
        assertTrue(conn.awaitRegistered(wait))
        assertTrue(hub.registrationsFrom("a") >= 2, "registrations=${hub.registrationsFrom("a")}")
        assertEquals(setOf("test.Ping"), hub.registeredTypes())
    }

    @Test fun `heartbeats are sent at the interval the hub announces`() = runBlocking {
        hub.heartbeatInterval = Duration.ofMillis(50)
        val conn = connection("a") // fallback interval 10 s
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

    // --- protocol v2 handshake, timeouts, error split, backoff (P0) ---

    @Test fun `register announces protocol v2 and the ack is parsed`() = runBlocking {
        hub.typeTimeouts = mapOf("test.Slow" to Duration.ofSeconds(30))
        val conn = connection("a")
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        val reg = assertNotNull(hub.lastRegisterFrom("a"))
        assertEquals(2, reg.protocolVersion)
        assertEquals(emptyList(), reg.capabilitiesList)
        assertTrue(reg.clientVersion.startsWith("kraken-messaging/"), reg.clientVersion)
        assertFalse(reg.clientVersion.endsWith("/unknown"), reg.clientVersion)
        val handshake = assertIs<HubHandshake.V2>(conn.handshake.value)
        assertEquals(hub.hubId, handshake.ack.hubId)
        assertEquals(2, handshake.ack.protocolVersion)
        assertEquals(3_000, handshake.ack.heartbeatIntervalMs)
        assertEquals(10_000, handshake.ack.heartbeatTimeoutMs)
        assertEquals("default", handshake.ack.context)
        assertEquals(mapOf("test.Slow" to 30_000), handshake.ack.typeTimeoutsMsMap)
    }

    @Test fun `every accepted register is acknowledged on a v2 hub`() = runBlocking {
        val conn = connection("a")
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        conn.register(setOf("test.Ping", "test.Pong"), emptySet())
        assertTrue(conn.awaitRegistered(wait))
        await atMost Duration.ofSeconds(2) until { hub.acksSentTo("a") == 2 }
        assertIs<HubHandshake.V2>(conn.handshake.value)
        Unit
    }

    @Test fun `legacy hub is detected from an echo without ack and the fallback interval is used`() = runBlocking {
        hub.legacy = true
        val conn = connection("a", settings = fast.copy(heartbeatInterval = Duration.ofMillis(50)))
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        assertEquals(HubHandshake.Legacy, conn.handshake.value)
        assertEquals(0, hub.acksSentTo("a"))
        await atMost Duration.ofMillis(500) until { hub.heartbeatsFrom("a") >= 3 }
    }

    @Test fun `v2 sends timeout 0 unless the caller chose one`() = runBlocking {
        val handler = connection("b") { req -> payloadReply(req.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a", settings = fast.copy(requestTimeout = null))
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        withTimeout(5_000) { sender.send(request(timeoutMs = 0)) }
        assertEquals(0, hub.lastRequest()!!.timeoutMs)
        withTimeout(5_000) { sender.send(request(timeoutMs = 1_500)) }
        assertEquals(1_500, hub.lastRequest()!!.timeoutMs)
    }

    @Test fun `legacy mode sends request-timeout or 5 s as before`() = runBlocking {
        hub.legacy = true
        val handler = connection("b") { req -> payloadReply(req.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val unset = connection("a", settings = fast.copy(requestTimeout = null))
        val configured = connection("c", settings = fast.copy(requestTimeout = Duration.ofSeconds(2)))
        unset.start(); configured.start()
        for (c in listOf(handler, unset, configured)) assertTrue(c.awaitRegistered(wait))

        withTimeout(5_000) { unset.send(request(timeoutMs = 0)) }
        assertEquals(5_000, hub.lastRequest()!!.timeoutMs)
        withTimeout(5_000) { configured.send(request(timeoutMs = 0)) }
        assertEquals(2_000, hub.lastRequest()!!.timeoutMs)
    }

    @Test fun `per-type override applies to a request without a timeout`() = runBlocking {
        hub.typeTimeouts = mapOf("test.Slow" to Duration.ofMillis(300))
        val handler = connection("b") { awaitCancellation() }
        handler.register(setOf("test.Slow"), emptySet())
        handler.start()
        val sender = connection("a", settings = fast.copy(requestTimeout = null))
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val started = System.nanoTime()
        val answer = withTimeout(5_000) { sender.send(request("test.Slow", timeoutMs = 0)) }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertEquals(ErrorCode.TIMEOUT, answer.error.code)
        assertTrue(elapsed < Duration.ofSeconds(2), "override not applied: took $elapsed") // default would be 5 s
    }

    @Test fun `local guard fires at the hub bound plus margin when the hub never answers`() = runBlocking {
        hub.emitTimeouts = false
        hub.typeTimeouts = mapOf("test.Lost" to Duration.ofMillis(200))
        val handler = connection("b") { awaitCancellation() }
        handler.register(setOf("test.Lost"), emptySet())
        handler.start()
        val sender = connection("a", settings = fast.copy(requestTimeout = null))
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val started = System.nanoTime()
        assertFailsWith<MessageTimeoutException> { withTimeout(5_000) { sender.send(request("test.Lost", timeoutMs = 0)) } }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(elapsed >= Duration.ofMillis(1_150) && elapsed < Duration.ofMillis(2_500), "guard after $elapsed")
    }

    @Test fun `local guard bound follows the hub's effective timeout rule`() {
        val ack = hub.registerAck().toBuilder().setMaxTimeoutMs(60_000).putTypeTimeoutsMs("t.Long", 120_000)
            .putTypeTimeoutsMs("t.Short", 2_000).build()
        assertEquals(60_000, HubProtocol.hubDeadlineBoundMs(0, "t.Other", ack))
        assertEquals(5_000, HubProtocol.hubDeadlineBoundMs(5_000, "t.Other", ack))
        assertEquals(60_000, HubProtocol.hubDeadlineBoundMs(90_000, "t.Other", ack))
        assertEquals(120_000, HubProtocol.hubDeadlineBoundMs(0, "t.Long", ack))
        assertEquals(90_000, HubProtocol.hubDeadlineBoundMs(90_000, "t.Long", ack)) // the override raises the clamp
        assertEquals(2_000, HubProtocol.hubDeadlineBoundMs(5_000, "t.Short", ack))
        assertEquals(61_000, HubProtocol.localGuardMs(0, "t.Other", ack))
    }

    @Test fun `request written before the stream breaks fails with HubConnectionLostException`() = runBlocking {
        val received = CompletableDeferred<Unit>()
        val handler = connection("b") { received.complete(Unit); awaitCancellation() }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val response = async { runCatching { sender.send(request(timeoutMs = 4_000)) } }
        withTimeout(5_000) { received.await() }
        hub.dropInstance("a") // the sender's own stream breaks: the request was written, the outcome is unknown

        val failure = withTimeout(5_000) { response.await() }.exceptionOrNull()
        assertIs<HubConnectionLostException>(failure, failure.toString())
        Unit
    }

    @Test fun `request that cannot be written to a live stream fails with HubUnavailableException`() = runBlocking {
        val conn = connection("a", settings = fast.copy(requestTimeout = Duration.ofMillis(200)))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        hub.close()
        withTimeout(5_000) { conn.connected.first { !it } }

        assertFailsWith<HubUnavailableException> { withTimeout(5_000) { conn.send(request()) } }
        Unit
    }

    @Test fun `zero request-timeout counts as unset against a legacy hub`() = runBlocking {
        hub.legacy = true
        val zero = fast.copy(requestTimeout = Duration.ZERO)
        val handler = connection("b", settings = zero) { req -> delay(200); payloadReply(req.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a", settings = zero)
        sender.start()
        assertTrue(handler.awaitRegistered(wait))
        assertTrue(sender.awaitRegistered(wait))
        assertIs<HubHandshake.Legacy>(sender.handshake.value)

        // Before the fix: 0 ms sent and a 0 ms local guard, so this failed at once with MessageTimeoutException.
        val response = withTimeout(5_000) { sender.send(request(timeoutMs = 0)) }
        assertTrue(response.hasReply() && !response.reply.hasError(), response.toString())
    }

    @Test fun `zero request-timeout still waits the default for a stream`() = runBlocking {
        val lateName = "late-hub-" + UUID.randomUUID()
        val conn = connection("a", settings = fast.copy(requestTimeout = Duration.ZERO), hubName = lateName)
        conn.start()
        val answer = async { runCatching { conn.send(request(type = "nobody.Owns")) } }
        delay(500)
        InProcessHub(lateName).start().use {
            // Before the fix the wait for a stream was 0 ms: HubUnavailableException at once.
            val result = withTimeout(5_000) { answer.await() }
            val message = result.getOrThrow()
            assertTrue(message.hasError(), message.toString())
            assertEquals(ErrorCode.NO_HANDLER, message.error.code)
            conn.stop()
        }
    }

    @Test fun `capped reconnect waits are jittered downward instead of pinned at the maximum`() {
        val random = Random(7)
        val max = Duration.ofSeconds(5)
        val atMax = (1..10_000).map { ReconnectBackoff.jittered(max, max, random).toMillis() }
        assertTrue(atMax.all { it in 4_000..5_000 }, "out of range: ${atMax.min()}..${atMax.max()}")
        assertTrue(atMax.count { it == 5_000L } < 50, "pinned at the maximum: ${atMax.count { it == 5_000L }} of 10000")
        assertTrue(atMax.min() < 4_050 && atMax.max() > 4_950, "not spread: ${atMax.min()}..${atMax.max()}")
    }

    @Test fun `reconnect waits are jittered within 20 percent and capped at the maximum`() {
        val random = Random(42)
        val base = Duration.ofMillis(1_000)
        val max = Duration.ofSeconds(5)
        val samples = (1..10_000).map { ReconnectBackoff.jittered(base, max, random).toMillis() }
        assertTrue(samples.all { it in 800..1_200 }, "out of range: ${samples.min()}..${samples.max()}")
        assertTrue(samples.min() < 850 && samples.max() > 1_150, "not spread: ${samples.min()}..${samples.max()}")
        val atMax = (1..10_000).map { ReconnectBackoff.jittered(max, max, random).toMillis() }
        assertTrue(atMax.all { it in 4_000..5_000 }, "out of range: ${atMax.min()}..${atMax.max()}")
        assertEquals(Duration.ofSeconds(5), ReconnectBackoff.next(Duration.ofSeconds(4), max))
        assertEquals(Duration.ofSeconds(2), ReconnectBackoff.next(Duration.ofSeconds(1), max))
    }

    @Test fun `after a long outage the connection comes back within the backoff maximum`() = runBlocking {
        val conn = connection("a") // backoff 20 ms .. 200 ms
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        hub.close()
        withTimeout(5_000) { conn.connected.first { !it } }
        delay(1_500) // many attempts: the backoff reached its maximum
        hub = InProcessHub(hub.name).start()
        val started = System.nanoTime()
        withTimeout(5_000) { conn.connected.first { it } }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(elapsed < Duration.ofMillis(600), "reconnected after $elapsed")
        assertTrue(conn.awaitRegistered(wait))
    }

    private fun payloadReply(id: String, json: String): Reply =
        Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun errorReply(id: String, code: ErrorCode, message: String): Reply =
        Reply.newBuilder().setRequestId(id).setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message)).build()
}
