package com.damoreira.snodra.messaging.hub

import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.ControlOk
import com.damoreira.snodra.server.v1.ControlRefusal
import com.damoreira.snodra.server.v1.ControlReply
import com.damoreira.snodra.server.v1.ControlRequest
import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.ProcessorPause
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.server.v1.ServerMessage
import com.damoreira.snodra.messaging.HubConnectionLostException
import com.damoreira.snodra.messaging.HubRegistrationRejectedException
import com.damoreira.snodra.messaging.HubUnavailableException
import com.damoreira.snodra.messaging.MessageTimeoutException
import com.damoreira.snodra.messaging.test.InProcessHub
import com.damoreira.snodra.server.v1.ClientMessage
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.Metadata
import io.grpc.MethodDescriptor
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
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
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
        control: ControlInbound = NO_PROCESSOR_CONTROL,
        inbound: suspend (Request) -> Reply = { req -> errorReply(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
    ) = HubConnection(
        { InProcessChannelBuilder.forName(hubName).build() }, serviceName, instanceId, settings, inbound, scope, handlerDispatcher,
        control = control,
    ).also { connections += it }

    private fun flowConnection(
        instanceId: String,
        maxConcurrentHandlers: Int,
        loadFactor: Int = 100,
        tags: Map<String, String> = emptyMap(),
        inbound: suspend (Request) -> Reply,
    ) = HubConnection(
        { InProcessChannelBuilder.forName(hub.name).build() }, "svc-$instanceId", instanceId, fast, inbound, scope,
        kotlinx.coroutines.Dispatchers.IO, maxConcurrentHandlers, loadFactor, tags,
    ).also { connections += it }

    // --- flow control ---

    @Test fun `register carries the initial permits, load factor and tags`() = runBlocking {
        val conn = flowConnection("a", maxConcurrentHandlers = 4, loadFactor = 150, tags = mapOf("zone" to "eu-1")) { req ->
            payloadReply(req.id, "{}")
        }
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val reg = hub.lastRegisterFrom("a")!!
        assertEquals(4, reg.initialPermits)
        assertEquals(150, reg.loadFactor)
        assertEquals(mapOf("zone" to "eu-1"), reg.tagsMap)
    }

    @Test fun `every Register declares the subscription query types, on a re-register and a new stream too`() = runBlocking {
        val conn = connection("a")
        conn.register(setOf("test.Ping"), setOf("test.Get", "test.Watch"), setOf("test.SharedWatch"), subscriptionQueryTypes = setOf("test.Watch", "test.SharedWatch"))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val first = hub.lastRegisterFrom("a")!!
        assertEquals(listOf("test.Get", "test.Watch"), first.queryTypesList)
        assertEquals(listOf("test.SharedWatch", "test.Watch"), first.subscriptionQueryTypesList)

        conn.register(setOf("test.Ping"), setOf("test.Get", "test.Watch"), setOf("test.SharedWatch"), subscriptionQueryTypes = setOf("test.Watch"))
        assertTrue(conn.awaitRegistered(wait))
        assertEquals(listOf("test.Watch"), hub.lastRegisterFrom("a")!!.subscriptionQueryTypesList)

        val registrations = hub.registrationsFrom("a")
        hub.dropInstance("a")
        await atMost Duration.ofSeconds(5) until { hub.registrationsFrom("a") > registrations }
        assertEquals(listOf("test.Watch"), hub.lastRegisterFrom("a")!!.subscriptionQueryTypesList)
    }

    @Test fun `without subscription handlers the Register declares no subscription query type`() = runBlocking {
        val conn = connection("a")
        conn.register(setOf("test.Ping"), setOf("test.Get"), setOf("test.Shared"))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        assertEquals(emptyList(), hub.lastRegisterFrom("a")!!.subscriptionQueryTypesList)
    }

    @Test fun `fewer than one handler or a negative load factor is rejected`() {
        assertFailsWith<IllegalArgumentException> { flowConnection("x", maxConcurrentHandlers = 0) { req -> payloadReply(req.id, "{}") } }
        val e = assertFailsWith<IllegalArgumentException> {
            flowConnection("x", maxConcurrentHandlers = 1, loadFactor = -1) { req -> payloadReply(req.id, "{}") }
        }
        assertEquals("loadFactor must not be negative, was -1", e.message)
        flowConnection("x", maxConcurrentHandlers = 1, loadFactor = 0) { req -> payloadReply(req.id, "{}") } // 0 reads as 100
    }

    @Test fun `each completed handler grants one permit, batched`() = runBlocking {
        val handler = flowConnection("b", maxConcurrentHandlers = 64) { req -> payloadReply(req.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        (1..40).map { async { sender.send(request()) } }.awaitAll()
        await atMost Duration.ofSeconds(2) until { hub.permitsGrantedBy("b") == 40 }
        val messages = hub.permitMessagesFrom("b")
        assertTrue(messages in 3..39, "40 grants in $messages Permits messages") // batched at 16, rest after 50 ms
        assertEquals(64, hub.permitsAvailable("b")) // every forward was paid back
    }

    @Test fun `a single grant is flushed after about 50 ms`() = runBlocking {
        val handler = flowConnection("b", maxConcurrentHandlers = 8) { req -> payloadReply(req.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        sender.send(request())
        await atMost Duration.ofMillis(1_000) until { hub.permitsGrantedBy("b") == 1 }
        assertEquals(1, hub.permitMessagesFrom("b"))
    }

    @Test fun `with one permit concurrent requests are served one after the other`() = runBlocking {
        val running = AtomicInteger()
        val maxRunning = AtomicInteger()
        val handler = flowConnection("b", maxConcurrentHandlers = 1) { req ->
            maxRunning.accumulateAndGet(running.incrementAndGet(), ::maxOf)
            delay(100)
            running.decrementAndGet()
            payloadReply(req.id, "{}")
        }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val results = (1..3).map { async { sender.send(request(timeoutMs = 4_000)) } }.awaitAll()
        assertTrue(results.all { it.hasReply() && !it.reply.hasError() }, results.toString())
        assertEquals(1, maxRunning.get())
        await atMost Duration.ofSeconds(1) until { hub.permitsGrantedBy("b") == 3 }
    }

    @Test fun `one permit and a zero-time handler serve 100 parallel calls in under a second`() = runBlocking {
        // The hub holds no permit while the only one is in use: each completion must go back at once, not after 50 ms.
        val handler = flowConnection("b", maxConcurrentHandlers = 1) { req -> payloadReply(req.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val started = System.nanoTime()
        val results = (1..100).map { async { sender.send(request(timeoutMs = 10_000)) } }.awaitAll()
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(results.all { it.hasReply() && !it.reply.hasError() }, results.filter { !it.hasReply() || it.reply.hasError() }.toString())
        assertTrue(elapsed < Duration.ofSeconds(1), "100 calls through one permit took $elapsed")
        await atMost Duration.ofSeconds(1) until { hub.permitsAvailable("b") == 1 }
        assertEquals(100, hub.permitsGrantedBy("b"))
    }

    @Test fun `a handler finishing after its stream broke grants nothing on the new stream`() = runBlocking {
        val received = CompletableDeferred<Unit>()
        val release = CompletableDeferred<Unit>()
        val handler = flowConnection("b", maxConcurrentHandlers = 4) { req ->
            received.complete(Unit)
            release.await()
            payloadReply(req.id, "{}")
        }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val call = async { runCatching { sender.send(request(timeoutMs = 4_000)) } }
        withTimeout(5_000) { received.await() }
        hub.dropInstance("b")
        await atMost Duration.ofSeconds(5) until { hub.registrationsFrom("b") >= 2 }
        assertTrue(handler.awaitRegistered(wait))
        release.complete(Unit)
        call.await()
        delay(300)
        assertEquals(0, hub.permitsGrantedBy("b"), "the old stream's permit must not be paid to the new stream")
        assertEquals(4, hub.permitsAvailable("b"))
    }

    private fun request(type: String = "test.Ping", timeoutMs: Int = 2000) = Request.newBuilder()
        .setId(UUID.randomUUID().toString())
        .setKind(RequestKind.COMMAND)
        .setType(type)
        .setPayload(ByteString.copyFromUtf8("{\"text\":\"x\"}"))
        .setTimeoutMs(timeoutMs)
        .build()

    @Test fun `a request still queued when its stream breaks fails as lost while queued, one the transport took as connection lost`() = runBlocking {
        val conn = connection("a")
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        hub.pauseReading = true // the hub stops reading: what the client writes stays queued on its side
        val calls = (1..20).map { async { runCatching { conn.send(request(timeoutMs = 5_000)) } } }
        delay(300)
        hub.dropInstance("a")
        val failures = withTimeout(10_000) { calls.awaitAll() }.map { it.exceptionOrNull() }
        hub.pauseReading = false
        val unwritten = failures.filterIsInstance<HubUnavailableException>()
        // Only the few the transport had taken are outcome-unknown; the queued ones are marked as moves (HubConnections counts them).
        assertTrue(unwritten.size >= 15, "unwritten: ${unwritten.size} of 20 ($failures)")
        assertTrue(unwritten.all { it.lostWhileQueued }, "every request still queued is marked lost while queued")
        failures.filterNot { it is HubUnavailableException }.forEach { assertIs<HubConnectionLostException>(it, it.toString()) }
    }

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
        hub.heartbeatInterval = Duration.ofMillis(100) // announced in the ack
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

    /** What the client side of one `Connect` call saw: heartbeats written, echoes read and when the call ended (System.nanoTime). */
    private class TappedCall {
        val sent = CopyOnWriteArrayList<Long>()
        val echoed = CopyOnWriteArrayList<Long>()
        @Volatile var endedAt = 0L
        fun ended() = synchronized(this) { if (endedAt == 0L) endedAt = System.nanoTime() }
    }

    /**
     * Taps every `Connect` call of a connection's channels. [onEcho] runs on the transport's thread before an echo is handed to the
     * connection (a slow echo can be made by sleeping there).
     */
    private class HeartbeatTap : ClientInterceptor {
        val calls = CopyOnWriteArrayList<TappedCall>()
        @Volatile var onEcho: (() -> Unit)? = null

        override fun <ReqT, RespT> interceptCall(method: MethodDescriptor<ReqT, RespT>, options: CallOptions, next: Channel): ClientCall<ReqT, RespT> {
            val call = TappedCall().also { calls += it }
            return object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, options)) {
                override fun sendMessage(message: ReqT) {
                    if ((message as? ClientMessage)?.hasHeartbeat() == true) call.sent += System.nanoTime()
                    super.sendMessage(message)
                }

                override fun cancel(message: String?, cause: Throwable?) {
                    call.ended()
                    super.cancel(message, cause)
                }

                override fun start(listener: Listener<RespT>, headers: Metadata) {
                    super.start(object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(listener) {
                        override fun onMessage(message: RespT) {
                            if ((message as? ServerMessage)?.hasHeartbeat() == true) {
                                onEcho?.invoke()
                                call.echoed += System.nanoTime()
                            }
                            super.onMessage(message)
                        }

                        override fun onClose(status: Status, trailers: Metadata) {
                            call.ended()
                            super.onClose(status, trailers)
                        }
                    }, headers)
                }
            }
        }
    }

    private fun tappedConnection(instanceId: String, tap: HeartbeatTap) = HubConnection(
        { InProcessChannelBuilder.forName(hub.name).intercept(tap).build() }, "svc-$instanceId", instanceId, fast,
        { req -> errorReply(req.id, ErrorCode.HANDLER_ERROR, "unexpected") }, scope,
    ).also { connections += it }

    @Test fun `the silent-hub decision needs more than three intervals and three unanswered heartbeats`() {
        val i = 1_000L
        val ms = 1_000_000L // nanoseconds
        data class Case(val sinceMs: Long, val unanswered: Int, val silent: Boolean)
        for (c in listOf(
            Case(4 * i - 1, 3, true), // the fourth tick after the last echoed heartbeat
            Case(3 * i + 1, 3, true), // more than three intervals, three heartbeats unanswered
            Case(3 * i, 3, false), // exactly three intervals: not more
            Case(3 * i + 1, 2, false), // a late third tick: only two heartbeats unanswered yet
            Case(10 * i, 2, false), // (the next tick decides)
            Case(2 * i + 500, 3, false), // three unanswered, but not three intervals
            Case(3 * i + 500, 0, false), // an echo just came
        )) {
            assertEquals(c.silent, hubSilent(c.sinceMs * ms, c.unanswered, i), "since ${c.sinceMs} ms, ${c.unanswered} unanswered")
        }
    }

    @Test fun `a silent hub is dropped on the tick after three unanswered heartbeats, never before three intervals`() = runBlocking {
        val interval = 400L
        val slack = 200L // scheduling and transport on a loaded runner; "never before three intervals" stays exact
        hub.heartbeatInterval = Duration.ofMillis(interval)
        val tap = HeartbeatTap()
        val conn = tappedConnection("a", tap)
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        delay(5 * interval) // echoed: the stream stays up past three intervals
        assertTrue(conn.connected.value)

        hub.echoHeartbeats = false
        withTimeout(10 * interval) { conn.connected.first { !it } }
        val call = tap.calls.first()
        await atMost Duration.ofSeconds(2) until { call.endedAt != 0L }
        val lastEcho = call.echoed.last()
        val silenceMs = (call.endedAt - lastEcho) / 1_000_000
        assertTrue(silenceMs > 3 * interval, "dropped $silenceMs ms after the last echo, want more than ${3 * interval}")
        assertTrue(silenceMs <= 4 * interval + slack, "dropped $silenceMs ms after the last echo, want the next tick after three intervals")
        // The check runs on the tick, before that tick's heartbeat: the drop comes one interval after the last heartbeat written.
        val sinceLastBeatMs = (call.endedAt - call.sent.last()) / 1_000_000
        assertTrue(sinceLastBeatMs in (interval - slack)..(interval + slack), "dropped $sinceLastBeatMs ms after the last heartbeat")
        assertEquals(3, call.sent.count { it > lastEcho }, "heartbeats written after the last echo")
    }

    @Test fun `a late echo starts the silence over - the drop never comes before three intervals after it`() = runBlocking {
        val interval = 200L
        hub.heartbeatInterval = Duration.ofMillis(interval)
        val tap = HeartbeatTap()
        val conn = tappedConnection("a", tap)
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        delay(3 * interval)

        // The next echo is the hub's last, and it arrives 1.5 intervals late: the heartbeat written meanwhile is never answered.
        val held = java.util.concurrent.atomic.AtomicBoolean()
        tap.onEcho = {
            if (held.compareAndSet(false, true)) {
                hub.echoHeartbeats = false
                Thread.sleep(interval * 3 / 2)
            }
        }
        withTimeout(10 * interval) { conn.connected.first { !it } }
        val call = tap.calls.first()
        await atMost Duration.ofSeconds(2) until { call.endedAt != 0L }
        val silenceMs = (call.endedAt - call.echoed.last()) / 1_000_000
        assertTrue(silenceMs > 3 * interval, "dropped $silenceMs ms after the last echo, want more than ${3 * interval}")
        assertEquals(3, call.sent.count { it > call.echoed.last() }, "heartbeats written after the last echo")
    }

    @Test fun `heartbeats are sent at the interval the hub announces`() = runBlocking {
        hub.heartbeatInterval = Duration.ofMillis(50)
        val conn = connection("a")
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

    // --- handshake, timeouts, error split, backoff ---

    @Test fun `the ack is parsed`() = runBlocking {
        hub.typeTimeouts = mapOf("test.Slow" to Duration.ofSeconds(30))
        val conn = connection("a")
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))

        val reg = assertNotNull(hub.lastRegisterFrom("a"))
        assertTrue(reg.clientVersion.startsWith("snodra-messaging/"), reg.clientVersion)
        assertFalse(reg.clientVersion.endsWith("/unknown"), reg.clientVersion)
        val ack = assertNotNull(conn.ack.value)
        assertEquals(hub.hubId, ack.hubId)
        assertEquals(hub.hubId, conn.hubId)
        assertEquals(3_000, ack.heartbeatIntervalMs)
        assertEquals(10_000, ack.heartbeatTimeoutMs)
        assertEquals("default", ack.context)
        assertEquals(mapOf("test.Slow" to 30_000), ack.typeTimeoutsMsMap)
    }

    @Test fun `every accepted register is acknowledged`() = runBlocking {
        val conn = connection("a")
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        conn.register(setOf("test.Ping", "test.Pong"), emptySet())
        assertTrue(conn.awaitRegistered(wait))
        await atMost Duration.ofSeconds(2) until { hub.acksSentTo("a") == 2 }
        assertNotNull(conn.ack.value)
        Unit
    }

    @Test fun `a stream without an ack within the handshake timeout fails and the connection reconnects`() = runBlocking {
        hub.ackDelay = Duration.ofSeconds(2) // every ack comes too late (the stream's later messages wait behind it)
        val conn = connection("a", settings = fast.copy(handshakeTimeout = Duration.ofMillis(200)))
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()

        // Registration completes on the ack only: the stream is given up and a new one opened, never connected.
        await atMost Duration.ofSeconds(3) until { hub.streamsOpenedBy("a") >= 3 }
        assertFalse(conn.connected.value)
        assertFalse(conn.awaitRegistered(Duration.ofMillis(100)))

        hub.ackDelay = Duration.ZERO
        assertTrue(conn.awaitRegistered(wait))
        assertNotNull(conn.ack.value)
        Unit
    }

    @Test fun `timeout 0 is sent unless the caller chose one`() = runBlocking {
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

    @Test fun `zero request-timeout counts as unset`() = runBlocking {
        val zero = fast.copy(requestTimeout = Duration.ZERO)
        val handler = connection("b", settings = zero) { req -> delay(200); payloadReply(req.id, "{}") }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("a", settings = zero)
        sender.start()
        assertTrue(handler.awaitRegistered(wait))
        assertTrue(sender.awaitRegistered(wait))

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

    // --- processor control ---

    private fun pause(id: String) = ControlRequest.newBuilder().setControlId(id).setProcessor("audit")
        .setPause(ProcessorPause.getDefaultInstance()).build()

    private fun ok(request: ControlRequest) = ControlReply.newBuilder().setControlId(request.controlId).setOk(ControlOk.getDefaultInstance()).build()

    @Test fun `control requests are served off the read loop and answered on the stream they arrived on`() = runBlocking {
        val gate = CountDownLatch(1)
        val seen = CopyOnWriteArrayList<String>()
        val control = ControlInbound { request, reply ->
            seen += request.controlId
            if (request.controlId == "c-1") gate.await(10, TimeUnit.SECONDS) // a control handler that blocks: the stream must not
            reply(ok(request))
        }
        val handler = connection("h", control = control, inbound = { req -> payloadReply(req.id, "{}") })
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = connection("s")
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))

        val first = async(Dispatchers.Default) { hub.control("h", pause("c-1"), Duration.ofSeconds(30)) }
        await atMost Duration.ofSeconds(5) until { "c-1" in seen }
        val second = async(Dispatchers.Default) { hub.control("h", pause("c-2"), Duration.ofSeconds(30)) }
        val heartbeats = hub.heartbeatsFrom("h")
        assertTrue(sender.send(request()).hasReply(), "requests still reach the handler")
        await atMost Duration.ofSeconds(15) until { hub.heartbeatsFrom("h") > heartbeats }
        assertEquals(listOf("c-1"), seen.toList(), "control requests keep their order")
        gate.countDown()
        assertTrue(first.await().hasOk())
        assertTrue(second.await().hasOk())
        assertEquals(listOf("c-1", "c-2"), hub.controlRepliesFrom("h").map { it.controlId })
    }

    @Test fun `without event processors every control request is UNKNOWN_PROCESSOR`() = runBlocking {
        val handler = connection("h")
        handler.start()
        assertTrue(handler.awaitRegistered(wait))
        val reply = hub.control("h", pause("c-1"))
        assertEquals(ControlRefusal.CONTROL_REFUSAL_UNKNOWN_PROCESSOR, reply.refused.code)
    }
}
