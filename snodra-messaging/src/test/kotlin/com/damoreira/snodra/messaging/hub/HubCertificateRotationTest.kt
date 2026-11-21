package com.damoreira.snodra.messaging.hub

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.ClientMessage
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.server.v1.ServerMessage
import com.damoreira.snodra.messaging.fixtures.TestPki
import com.damoreira.snodra.messaging.test.InProcessHub
import io.grpc.CallOptions
import io.grpc.Channel
import io.grpc.ClientCall
import io.grpc.ClientInterceptor
import io.grpc.ForwardingClientCall
import io.grpc.ForwardingClientCallListener
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.MethodDescriptor
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * The rotation of a client certificate against in-process hubs: the renewed files are noticed on the check tick, and the
 * hub connections roll over to them one hub at a time (told first, drained, closed gracefully, reconnected), without failing a request.
 */
class HubCertificateRotationTest {
    @TempDir lateinit var dir: Path
    private val a = InProcessHub().start()
    private val b = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clients = mutableListOf<HubConnections>()
    private val wait = Duration.ofSeconds(10)
    private lateinit var ca: TestPki.Ca
    private val certFile get() = dir.resolve("client.pem")
    private val keyFile get() = dir.resolve("client.key")

    private val log = LoggerFactory.getLogger(HubClientCertificate::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()
    private val rolloverLog = LoggerFactory.getLogger(HubConnections::class.java) as Logger
    private val connectionLog = LoggerFactory.getLogger(HubConnection::class.java) as Logger

    /** What the handler instance did to each hub's stream, in order: `A:register:<types>`, `A:half-close`, `A:closed`, `A:channel`. */
    private val events = CopyOnWriteArrayList<String>()

    @BeforeEach fun setUp() {
        ca = TestPki.ca(dir.resolve("ca"))
        ca.issue("svc-a").writeTo(certFile, keyFile)
        appender.start()
        log.addAppender(appender)
        rolloverLog.addAppender(appender)
        connectionLog.addAppender(appender)
    }

    @AfterEach fun tearDown() = runBlocking {
        log.detachAppender(appender)
        rolloverLog.detachAppender(appender)
        connectionLog.detachAppender(appender)
        clients.forEach { it.stop() }
        scope.cancel()
        a.close()
        b.close()
    }

    private fun label(hub: InProcessHub) = if (hub === a) "A" else "B"

    /** When each instance last told a hub it is stopping (the empty Register that starts its roll), by instance id, in nanoTime. */
    private val deregisteredAt = java.util.concurrent.ConcurrentHashMap<String, Long>()

    private fun recordingChannel(hub: InProcessHub, instanceId: String = "i-a"): ManagedChannel {
        val label = label(hub)
        events += "$label:channel"
        val recorder = object : ClientInterceptor {
            override fun <ReqT, RespT> interceptCall(method: MethodDescriptor<ReqT, RespT>, options: CallOptions, next: Channel): ClientCall<ReqT, RespT> =
                object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, options)) {
                    override fun start(listener: Listener<RespT>, headers: Metadata) {
                        super.start(object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(listener) {
                            override fun onClose(status: Status, trailers: Metadata) {
                                events += "$label:closed"
                                super.onClose(status, trailers)
                            }
                        }, headers)
                    }

                    override fun sendMessage(message: ReqT) {
                        (message as? ClientMessage)?.takeIf { it.hasRegister() }?.let {
                            events += "$label:register:" + (it.register.commandTypesList + it.register.queryTypesList).joinToString(",")
                            if (it.register.commandTypesList.isEmpty() && it.register.queryTypesList.isEmpty()) deregisteredAt.putIfAbsent(instanceId, System.nanoTime())
                        }
                        super.sendMessage(message)
                    }

                    override fun halfClose() {
                        events += "$label:half-close"
                        super.halfClose()
                    }
                }
        }
        return InProcessChannelBuilder.forName(hub.name).intercept(recorder).build()
    }

    private val byName get() = listOf(a, b).associateBy { it.name }

    private fun settings(certificate: HubClientCertificate?) = HubSettings(
        requestTimeout = Duration.ofSeconds(3),
        backoffMin = Duration.ofMillis(20), backoffMax = Duration.ofMillis(300), clientCertificate = certificate,
    )

    /** The handler instance `svc-a` with the certificate, on [hubs]; check tick 50 ms. */
    private fun handler(
        hubs: List<InProcessHub>, certificate: HubClientCertificate, reconnectTimeout: Duration = Duration.ofSeconds(5),
        drainTimeout: Duration = Duration.ofSeconds(3), instanceId: String = "i-a",
        maxDelay: Duration = Duration.ZERO, random: () -> Double = { 0.0 }, inbound: suspend (Request) -> Reply = { reply(it.id) },
    ) = HubConnections(
        targets = HubTargets.Static(hubs.map { it.name }),
        channelFactory = HubChannelFactory { target -> recordingChannel(byName.getValue(target), instanceId) },
        serviceName = "svc-a", instanceId = instanceId, settings = settings(certificate), inbound = inbound, scope = scope,
        statusInterval = Duration.ofMillis(200),
        certificateCheckInterval = Duration.ofMillis(50),
        rolloverDrainTimeout = drainTimeout, rolloverReconnectTimeout = reconnectTimeout,
        rolloverMaxDelay = maxDelay, rolloverRandom = random,
    ).also { clients += it; it.register(setOf("test.Ping"), emptySet()); it.start() }

    private fun sender(hubs: List<InProcessHub>) = HubConnections(
        targets = HubTargets.Static(hubs.map { it.name }),
        channelFactory = HubChannelFactory { target -> InProcessChannelBuilder.forName(target).build() },
        serviceName = "svc-b", instanceId = "i-b", settings = settings(null), inbound = { reply(it.id) }, scope = scope,
        statusInterval = Duration.ofMillis(200),
    ).also { clients += it; it.start() }

    private fun ping() = Request.newBuilder().setId(java.util.UUID.randomUUID().toString()).setKind(RequestKind.COMMAND).setType("test.Ping")
        .setPayload(ByteString.copyFromUtf8("{}")).setTimeoutMs(3_000).build()

    private fun reply(id: String) = Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8("{}")).build()

    private fun certificate() = HubClientCertificate(certFile, keyFile, "svc-a")

    private fun renew() = ca.issue("svc-a").also { it.writeTo(certFile, keyFile) }

    private fun lines(level: Level) = appender.list.filter { it.level == level }.map { it.formattedMessage }

    @Test fun `a renewed pair rolls the hubs over one at a time, each told first and registered again before the next is touched`() = runBlocking {
        val certificate = certificate()
        val handler = handler(listOf(a, b), certificate)
        assertTrue(handler.awaitRegistered(wait))
        events.clear()

        val renewed = renew()
        await atMost wait until { handler.authStatus().certificateRotations == 1L && events.count { it.endsWith("register:test.Ping") } == 2 && handler.liveHubIds().size == 2 }
        assertEquals(
            listOf(
                "A:register:", "A:half-close", "A:closed", "A:channel", "A:register:test.Ping",
                "B:register:", "B:half-close", "B:closed", "B:channel", "B:register:test.Ping",
            ),
            events.toList(),
        )
        assertEquals(renewed.fingerprint, certificate.current.fingerprint)
        assertEquals(listOf(2, 2), listOf(a, b).map { it.streamsOpenedBy("i-a") })
        assertEquals(emptyList(), lines(Level.ERROR))
    }

    @Test fun `files that did not change cause no rollover`() = runBlocking {
        val handler = handler(listOf(a, b), certificate())
        assertTrue(handler.awaitRegistered(wait))
        delay(500)
        assertEquals(listOf(1, 1), listOf(a, b).map { it.streamsOpenedBy("i-a") })
        assertEquals(0L, handler.authStatus().certificateRotations)
    }

    @Test fun `a replacement that is not valid keeps every session, logs one ERROR and rolls once the pair is complete`() = runBlocking {
        val handler = handler(listOf(a, b), certificate())
        assertTrue(handler.awaitRegistered(wait))
        Files.writeString(certFile, "half a file")
        delay(500)
        assertEquals(listOf(1, 1), listOf(a, b).map { it.streamsOpenedBy("i-a") }, "no stream was touched")
        assertEquals(1, lines(Level.ERROR).size, lines(Level.ERROR).toString())
        assertTrue(handler.connected.value)

        renew()
        await atMost wait until { listOf(a, b).all { it.streamsOpenedBy("i-a") == 2 } && handler.liveHubIds().size == 2 }
        assertEquals(1L, handler.authStatus().certificateRotations)
    }

    @Test fun `with two hubs a steady trickle of requests sees no failure while both roll over`() = runBlocking {
        val handler = handler(listOf(a, b), certificate())
        val sender = sender(listOf(a, b))
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val failures = CopyOnWriteArrayList<String>()
        val sent = AtomicInteger()
        val trickle: Job = scope.launch {
            while (isActive) {
                try {
                    // A NO_HANDLER (or any other error) comes back as a message, not as an exception: look at the answer itself.
                    val answer = sender.send(ping())
                    if (answer.hasError() || !answer.hasReply() || answer.reply.payload.toStringUtf8() != "{}") failures += "not a reply: $answer" else sent.incrementAndGet()
                } catch (e: Exception) {
                    failures += e.toString()
                }
                delay(5)
            }
        }
        delay(300)
        renew()
        await atMost wait until { handler.authStatus().certificateRotations == 1L && listOf(a, b).all { it.streamsOpenedBy("i-a") == 2 } && handler.liveHubIds().size == 2 }
        delay(300)
        trickle.cancel()
        assertEquals(emptyList(), failures.toList())
        assertTrue(sent.get() > 50, "the trickle ran: ${sent.get()}")
    }

    @Test fun `with one hub the roll leaves a short gap in which other services get NO_HANDLER, and nothing else fails`() = runBlocking {
        val handler = handler(listOf(a), certificate())
        val sender = sender(listOf(a))
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val failures = CopyOnWriteArrayList<String>()
        val answers = CopyOnWriteArrayList<ServerMessage>()
        val trickle: Job = scope.launch {
            while (isActive) {
                try {
                    answers += sender.send(ping())
                } catch (e: Exception) {
                    failures += e.toString()
                }
                delay(5)
            }
        }
        delay(300)
        renew()
        await atMost wait until { handler.authStatus().certificateRotations == 1L && a.streamsOpenedBy("i-a") == 2 && handler.liveHubIds().size == 1 }
        delay(300)
        trickle.cancel()
        assertEquals(emptyList(), failures.toList())
        // The documented gap: the hub has no owner of the type between the empty Register and the new stream's, and says so at once. Nothing else.
        val unexpected = answers.filterNot { (it.hasReply() && it.reply.payload.toStringUtf8() == "{}") || (it.hasError() && it.error.code == ErrorCode.NO_HANDLER) }
        assertEquals(emptyList(), unexpected)
        assertTrue(answers.count { it.hasReply() } > 50, "the trickle ran: ${answers.size} answers")
        assertTrue(answers.last().hasReply(), "the new stream serves again: ${answers.last()}")
    }

    @Test fun `a request being handled is finished on the old stream before the roll closes it`() = runBlocking {
        val release = CompletableDeferred<Unit>()
        val handler = handler(listOf(a), certificate()) { req -> release.await(); reply(req.id) }
        val sender = sender(listOf(a))
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val call = scope.launch { sender.send(ping()) }
        await atMost wait until { handler.inFlightHandlers.value == 1 }
        renew()
        await atMost wait until { "A:register:" in events }
        delay(500)
        assertEquals(1, a.streamsOpenedBy("i-a"), "the stream is still the old one")
        release.complete(Unit)
        call.join()
        await atMost wait until { a.streamsOpenedBy("i-a") == 2 && handler.liveHubIds().size == 1 }
        assertEquals(emptyList(), lines(Level.ERROR))
    }

    @Test fun `a hub with another live hub leaves routing at once, the only hub only after its work in flight finished`() = runBlocking {
        val release = CompletableDeferred<Unit>()
        val two = handler(listOf(a, b), certificate()) { req -> release.await(); reply(req.id) }
        val sender = sender(listOf(a, b))
        assertTrue(two.awaitRegistered(wait) && sender.awaitRegistered(wait))
        // A blocked request on whichever hub it picks; keep sending until one is in flight.
        scope.launch { runCatching { sender.send(ping()) } }
        await atMost wait until { two.inFlightHandlers.value == 1 }
        renew()
        await atMost wait until { "A:register:" in events }
        assertEquals(1, two.liveHubIds().size, "hub A is out of routing at once, hub B still takes work")
        release.complete(Unit)
        await atMost wait until { two.authStatus().certificateRotations == 1L && listOf(a, b).all { it.streamsOpenedBy("i-a") == 2 } && two.liveHubIds().size == 2 }
    }

    @Test fun `the only hub stays in routing while its handlers finish`() = runBlocking {
        val release = CompletableDeferred<Unit>()
        val one = handler(listOf(a), certificate()) { req -> release.await(); reply(req.id) }
        val sender = sender(listOf(a))
        assertTrue(one.awaitRegistered(wait) && sender.awaitRegistered(wait))
        scope.launch { runCatching { sender.send(ping()) } }
        await atMost wait until { one.inFlightHandlers.value == 1 }
        renew()
        await atMost wait until { "A:register:" in events }
        delay(300)
        assertEquals(1, one.liveHubIds().size, "the gap starts when the handlers are done, not before")
        release.complete(Unit)
        await atMost wait until { a.streamsOpenedBy("i-a") == 2 && one.liveHubIds().size == 1 && one.authStatus().certificateRotations == 1L }
    }

    @Test fun `a hub that does not register again in time stops the rollover for that pair, with one ERROR, until the files change again`() = runBlocking {
        val handler = handler(listOf(a, b), certificate(), reconnectTimeout = Duration.ofMillis(400))
        assertTrue(handler.awaitRegistered(wait))
        a.ackDelay = Duration.ofSeconds(3) // hub A is slow to acknowledge a new stream
        events.clear()
        renew()
        await atMost wait until { events.count { it == "A:channel" } >= 1 }
        delay(1_200)
        assertEquals(1, b.streamsOpenedBy("i-a"), "hub B keeps the old pair, and is not touched on later ticks")
        val errors = rolloverLog.let { appender.list.filter { e -> e.level == Level.ERROR && "did not complete" in e.formattedMessage } }
        assertEquals(1, errors.size, appender.list.map { it.formattedMessage }.toString())

        a.ackDelay = Duration.ZERO
        renew() // the files change again: both are rolled
        await atMost wait until { b.streamsOpenedBy("i-a") == 2 && handler.liveHubIds().size == 2 }
    }

    @Test fun `the auth status of a certificate service names the certificate, its end and the rotations`() = runBlocking {
        val certificate = certificate()
        val handler = handler(listOf(a), certificate)
        assertTrue(handler.awaitRegistered(wait))
        val before = handler.authStatus()
        assertEquals(HubAuthKind.CERTIFICATE, before.kind)
        assertEquals(certificate.current.fingerprint, before.certificateFingerprint)
        assertEquals(certificate.current.notAfter, before.certificateExpiresAt)
        assertEquals(0L, before.certificateRotations)
        assertEquals("svc-a", before.certificateService)
        assertEquals(null, before.tokenExpiresAt)
        assertEquals(0L, before.credentialsRefreshesSent)

        val renewed = renew()
        await atMost wait until { handler.authStatus().certificateRotations == 1L }
        assertEquals(renewed.fingerprint, handler.authStatus().certificateFingerprint)
        assertEquals(renewed.certificate.notAfter.toInstant(), handler.authStatus().certificateExpiresAt)
    }

    @Test fun `the rotations are a Micrometer counter, bound only for a certificate service`() = runBlocking {
        val handler = handler(listOf(a), certificate())
        assertTrue(handler.awaitRegistered(wait))
        val registry = SimpleMeterRegistry()
        HubClientMetrics(handler).bindTo(registry)
        assertEquals(0.0, registry.get("snodra.hub.client.certificate.rotations").functionCounter().count())
        renew()
        await atMost wait until { registry.get("snodra.hub.client.certificate.rotations").functionCounter().count() == 1.0 }

        val plain = SimpleMeterRegistry()
        HubClientMetrics(sender(listOf(a))).bindTo(plain)
        assertTrue(plain.find("snodra.hub.client.certificate.rotations").functionCounter() == null)
    }

    @Test fun `a roll does not wait for the subscription this instance itself consumes`() = runBlocking {
        // svc-c serves a subscription that never ends; the rolled instance (svc-a) is its consumer.
        val owner = HubConnections(
            targets = HubTargets.Static(listOf(a.name)),
            channelFactory = HubChannelFactory { target -> InProcessChannelBuilder.forName(target).build() },
            serviceName = "svc-c", instanceId = "i-c", settings = settings(null),
            inbound = { reply(it.id) }, scope = scope, statusInterval = Duration.ofMillis(200),
            subscriptions = SubscriptionRegistry(), inboundSubscription = { _, register -> register(Any()) },
        ).also { clients += it; it.register(emptySet(), setOf("test.Watch")); it.start() }
        val handler = handler(listOf(a), certificate(), drainTimeout = Duration.ofSeconds(10))
        assertTrue(owner.awaitRegistered(wait) && handler.awaitRegistered(wait))
        val watch = Request.newBuilder().setId(java.util.UUID.randomUUID().toString()).setKind(RequestKind.QUERY).setType("test.Watch")
            .setPayload(ByteString.copyFromUtf8("{}")).setTimeoutMs(3_000).build()
        assertTrue(handler.subscribe(watch) is SubscribeResult.Open, "the subscription is open")

        renew()
        // The drain of 10 s must not wait for the subscription: the roll is done in well under that.
        await atMost Duration.ofSeconds(4) until { a.streamsOpenedBy("i-a") == 2 && handler.liveHubIds().size == 1 }
        assertEquals(emptyList(), lines(Level.WARN).filter { "still in flight" in it })
        assertEquals(emptyList(), lines(Level.ERROR))
    }

    // --- the random delay before a roll (spreads the replicas of one service) ---

    private fun millisSince(start: Long, instanceId: String) = (deregisteredAt.getValue(instanceId) - start) / 1_000_000

    @Test fun `two replicas that see the same renewed pair do not deregister at the same instant`() = runBlocking {
        val first = handler(listOf(a), certificate(), instanceId = "i-a1", maxDelay = Duration.ofSeconds(2), random = { 0.0 })
        val second = handler(listOf(a), certificate(), instanceId = "i-a2", maxDelay = Duration.ofSeconds(2), random = { 1.0 })
        assertTrue(first.awaitRegistered(wait) && second.awaitRegistered(wait))
        val start = System.nanoTime()
        renew()
        await atMost wait until { deregisteredAt.size == 2 }
        val early = millisSince(start, "i-a1")
        val late = millisSince(start, "i-a2")
        assertTrue(late - early >= 1_500, "the replicas deregistered ${late - early} ms apart (first after $early ms, second after $late ms)")
        await atMost wait until { listOf(first, second).all { it.authStatus().certificateRotations == 1L && it.liveHubIds().size == 1 } && a.streamsOpenedBy("i-a1") == 2 && a.streamsOpenedBy("i-a2") == 2 }
    }

    @Test fun `the delay is at most a tenth of what is left of the presented certificate`() = runBlocking {
        ca.issue("svc-a", notBefore = java.time.Instant.now(), validFor = Duration.ofSeconds(20)).writeTo(certFile, keyFile) // 20 s left: at most 2 s
        val handler = handler(listOf(a), certificate(), maxDelay = Duration.ofSeconds(30), random = { 1.0 })
        assertTrue(handler.awaitRegistered(wait))
        val start = System.nanoTime()
        renew()
        await atMost wait until { deregisteredAt.containsKey("i-a") }
        val waited = millisSince(start, "i-a")
        assertTrue(waited in 1_500..6_000, "waited $waited ms, expected about 2000 (a tenth of 20 s)")
        await atMost wait until { handler.authStatus().certificateRotations == 1L && a.streamsOpenedBy("i-a") == 2 && handler.liveHubIds().size == 1 }
    }

    @Test fun `a presented certificate that has ended is rolled without a delay`() = runBlocking {
        var now = java.time.Instant.now()
        val certificate = HubClientCertificate(certFile, keyFile, "svc-a") { now }
        val handler = handler(listOf(a), certificate, maxDelay = Duration.ofSeconds(30), random = { 1.0 })
        assertTrue(handler.awaitRegistered(wait))
        now = certificate.current.notAfter.plusSeconds(60) // the presented pair ended (the hub would close the session)
        val start = System.nanoTime()
        ca.issue("svc-a", validFor = Duration.ofHours(4)).writeTo(certFile, keyFile)
        await atMost wait until { deregisteredAt.containsKey("i-a") }
        assertTrue(millisSince(start, "i-a") < 3_000, "waited ${millisSince(start, "i-a")} ms")
    }

    @Test fun `a pair renewed again during the delay is rolled to once, as the newer one`() = runBlocking {
        val certificate = certificate()
        val handler = handler(listOf(a), certificate, maxDelay = Duration.ofSeconds(2), random = { 1.0 })
        assertTrue(handler.awaitRegistered(wait))
        renew()
        delay(600) // the first pair is seen, the delay runs
        val newest = renew()
        await atMost wait until { handler.authStatus().certificateFingerprint == newest.fingerprint && a.streamsOpenedBy("i-a") >= 2 && handler.liveHubIds().size == 1 }
        delay(500)
        assertEquals(2, a.streamsOpenedBy("i-a"), "one roll, to the newest pair, not one per pair")
        assertEquals(newest.fingerprint, certificate.current.fingerprint)
    }

    @Test fun `an unexpected exception in a check is logged once and does not end the rotation`() = runBlocking {
        val failing = AtomicInteger(0) // checks that still throw
        val thrown = AtomicInteger()
        val certificate = HubClientCertificate(certFile, keyFile, "svc-a") {
            if (failing.get() > 0 && thrown.get() < failing.get()) { thrown.incrementAndGet(); throw IllegalStateException("odd key DER") }
            java.time.Instant.now()
        }
        val handler = handler(listOf(a), certificate)
        assertTrue(handler.awaitRegistered(wait))
        failing.set(3) // the next three checks throw (a JCA provider exception on a key it cannot parse, say), the same problem each time
        await atMost wait until { thrown.get() == 3 }
        delay(200)
        assertEquals(1, lines(Level.ERROR).count { "odd key DER" in it }, lines(Level.ERROR).toString())

        val renewed = renew() // the loop is still alive: the renewed pair is rolled to
        await atMost wait until { handler.authStatus().certificateRotations == 1L && a.streamsOpenedBy("i-a") == 2 && handler.liveHubIds().size == 1 }
        assertEquals(renewed.fingerprint, handler.authStatus().certificateFingerprint)
    }

    @Test fun `stopping while a roll is waiting for work in flight ends at once`() = runBlocking {
        val release = CompletableDeferred<Unit>()
        val handler = handler(listOf(a), certificate()) { req -> release.await(); reply(req.id) }
        val sender = sender(listOf(a))
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        scope.launch { runCatching { sender.send(ping()) } }
        await atMost wait until { handler.inFlightHandlers.value == 1 }
        renew()
        await atMost wait until { "A:register:" in events }
        val started = System.nanoTime()
        handler.stop()
        assertTrue((System.nanoTime() - started) / 1_000_000 < 4_000)
        release.complete(Unit)
        Unit
    }
}
