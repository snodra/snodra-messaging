package com.damoreira.snodra.messaging.hub

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.ClientMessage
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
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
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
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
import java.time.Instant
import java.time.Duration
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * One hub stream of a service that authenticates with a client certificate, against the in-process hub (which cannot do TLS: the
 * handshake is covered by [HubMutualTlsChannelTest] and the end-to-end tests; what is checked here is what the connection itself does).
 */
class HubCertificateSessionTest {
    @TempDir lateinit var dir: Path
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val wait = Duration.ofSeconds(5)
    private val cap = Duration.ofSeconds(5)
    private lateinit var ca: TestPki.Ca
    private val certFile get() = dir.resolve("client.pem")
    private val keyFile get() = dir.resolve("client.key")

    private val log = LoggerFactory.getLogger(HubConnection::class.java) as Logger
    private val certificateLog = LoggerFactory.getLogger(HubClientCertificate::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    /** What the client did to the hub's stream, in order: `register:<types>`, `half-close`, `closed`, `channel`. */
    private val events = CopyOnWriteArrayList<String>()

    @BeforeEach fun setUp() {
        ca = TestPki.ca(dir.resolve("ca"))
        ca.issue("svc-a").writeTo(certFile, keyFile)
        appender.start()
        log.addAppender(appender)
        certificateLog.addAppender(appender)
    }

    @AfterEach fun tearDown() = runBlocking {
        log.detachAppender(appender)
        certificateLog.detachAppender(appender)
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun settings(certificate: HubClientCertificate?, token: HubToken = HubToken.NONE) = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        backoffMin = Duration.ofMillis(20), backoffMax = cap, token = token, clientCertificate = certificate,
    )

    private fun certificate() = HubClientCertificate(certFile, keyFile, "svc-a")

    /** A channel whose outgoing messages and end are logged into [events]. */
    private fun recordingChannel(): ManagedChannel {
        events += "channel"
        val recorder = object : ClientInterceptor {
            override fun <ReqT, RespT> interceptCall(method: MethodDescriptor<ReqT, RespT>, options: CallOptions, next: Channel): ClientCall<ReqT, RespT> =
                object : ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(next.newCall(method, options)) {
                    override fun start(listener: Listener<RespT>, headers: Metadata) {
                        super.start(object : ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(listener) {
                            override fun onClose(status: Status, trailers: Metadata) {
                                events += "closed"
                                super.onClose(status, trailers)
                            }
                        }, headers)
                    }

                    override fun sendMessage(message: ReqT) {
                        (message as? ClientMessage)?.takeIf { it.hasRegister() }?.let {
                            events += "register:" + (it.register.commandTypesList + it.register.queryTypesList).joinToString(",")
                        }
                        super.sendMessage(message)
                    }

                    override fun halfClose() {
                        events += "half-close"
                        super.halfClose()
                    }
                }
        }
        return InProcessChannelBuilder.forName(hub.name).intercept(recorder).build()
    }

    private fun connection(
        instanceId: String,
        service: String = "svc-a",
        settings: HubSettings,
        inbound: suspend (Request) -> Reply = { req -> Reply.newBuilder().setRequestId(req.id).build() },
    ) = HubConnection({ recordingChannel() }, service, instanceId, settings, inbound, scope).also { connections += it }

    private fun ping() = Request.newBuilder().setId("r-" + System.nanoTime()).setKind(RequestKind.COMMAND).setType("test.Ping").setTimeoutMs(5_000).build()

    private fun reply(id: String) = Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8("{}")).build()

    // --- credentials ---

    @Test fun `a certificate session registers without credentials and never refreshes any, even when a token was also given`() = runBlocking {
        val conn = connection("i-1", settings = settings(certificate(), HubToken.of("a-stale-token", null)))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        delay(300)
        assertFalse(hub.lastRegisterFrom("i-1")!!.hasCredentials())
        assertEquals(emptyList(), hub.tokensSeenFrom("i-1").filterNotNull())
        assertEquals(emptyList(), hub.credentialsSeenFrom("i-1"))
        assertEquals(0, conn.credentialsRefreshesSent)
    }

    @Test fun `a certificate that has ended is refused locally with UNAUTHENTICATED, no stream is opened, and it is logged once`() = runBlocking {
        var now = Instant.now()
        val certificate = HubClientCertificate(certFile, keyFile, "svc-a") { now }
        now = certificate.current.notAfter.plusSeconds(5)
        val conn = connection("i-1", settings = settings(certificate))
        conn.start()
        await atMost Duration.ofSeconds(3) until { conn.lastRejection.value != null }
        assertEquals(Status.Code.UNAUTHENTICATED, conn.lastRejection.value!!.code)
        assertTrue("expired at" in conn.lastRejection.value!!.description!!, conn.lastRejection.value.toString())
        assertEquals(0, hub.streamsOpenedBy("i-1"))
        assertEquals(0, events.count { it == "channel" }, "no channel, no handshake")
        assertEquals(1, appender.list.count { it.level == Level.ERROR && "expired at" in it.formattedMessage })
        // The files are renewed: the next attempt (at the rejection back-off) presents them.
        ca.issue("svc-a").writeTo(certFile, keyFile)
        now = Instant.now()
        await atMost Duration.ofSeconds(10) until { conn.connected.value }
        assertNull(conn.lastRejection.value)
    }

    // --- one refresh per connection attempt ---

    /** A connection whose channels are the real TLS ones of the starter (to a port nobody listens on: only the attempt matters), counted per attempt. */
    private fun tlsConnection(certificate: HubClientCertificate, beforeChannel: () -> Unit = {}): Pair<HubConnection, AtomicInteger> {
        val attempts = AtomicInteger()
        val factory = DefaultHubChannelFactory(HubTls(clientCertificate = certificate))
        val conn = HubConnection(
            { beforeChannel(); factory.channel("localhost:1").also { attempts.incrementAndGet() } }, "svc-a", "i-1",
            settings(certificate).copy(backoffMin = Duration.ofSeconds(30), backoffMax = Duration.ofSeconds(30)), { reply(it.id) }, scope,
        ).also { connections += it }
        return conn to attempts
    }

    @Test fun `a half-written pair seen at one reconnect is seen once, so it is not logged at ERROR yet`() = runBlocking {
        val certificate = certificate()
        Files.writeString(certFile, "half a file")
        val (conn, attempts) = tlsConnection(certificate)
        conn.start()
        await atMost wait until { attempts.get() == 1 }
        delay(200)
        assertEquals(emptyList(), appender.list.filter { it.level == Level.ERROR }.map { it.formattedMessage }, "the second-sighting rule: one attempt is one sighting")
        assertEquals(1, appender.list.count { it.level == Level.WARN && "presenting the last good certificate" in it.formattedMessage })
    }

    @Test fun `a pair that appears while an attempt builds its channel is not presented by it, so the connection's generation is still the certificate's`() = runBlocking {
        val certificate = certificate()
        val (conn, attempts) = tlsConnection(certificate) { ca.issue("svc-a").writeTo(certFile, keyFile) } // renewed right after the attempt read the files
        conn.start()
        await atMost wait until { attempts.get() == 1 }
        // The attempt presents what it read; the renewed pair is adopted by the next check, which then rolls this connection.
        assertEquals(certificate.generation, conn.certificateGeneration, "what the stream presents and what it is recorded as must agree")
        certificate.refresh()
        assertTrue(conn.certificateGeneration < certificate.generation, "the renewed pair is newer than the stream's, the next tick rolls it")
    }

    // --- the close at notAfter ---

    @Test fun `the hub closing the session because the certificate ended is a normal reconnect, not a registration rejection`() = runBlocking {
        val conn = connection("i-1", settings = settings(certificate()))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        hub.dropInstance("i-1", Status.UNAUTHENTICATED.withDescription("certificate expired"))
        // A rejection would wait the 5 s cap; a normal reconnect starts again within a moment.
        await atMost Duration.ofSeconds(2) until { hub.streamsOpenedBy("i-1") >= 2 }
        assertTrue(conn.awaitRegistered(wait))
        assertNull(conn.lastRejection.value)
        assertEquals(emptyList(), appender.list.filter { it.level == Level.ERROR }.map { it.formattedMessage })
        assertTrue(appender.list.any { "client certificate" in it.formattedMessage && "reconnecting" in it.formattedMessage }, appender.list.map { it.formattedMessage }.toString())
    }

    @Test fun `without a certificate the same close stays a registration rejection`() = runBlocking {
        val conn = connection("i-1", settings = settings(null))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        hub.dropInstance("i-1", Status.UNAUTHENTICATED.withDescription("certificate expired"))
        await atMost Duration.ofSeconds(2) until { conn.lastRejection.value != null }
        assertEquals(Status.Code.UNAUTHENTICATED, conn.lastRejection.value!!.code)
    }

    // --- the rollover of one stream ---

    @Test fun `a rollover tells the hub first, waits for the work in flight, half-closes, then reconnects and registers again`() = runBlocking {
        val release = CompletableDeferred<Unit>()
        val handler = connection("i-1", settings = settings(certificate())) { req -> events += "handler start"; release.await(); events += "handler end"; reply(req.id) }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = HubConnection({ InProcessChannelBuilder.forName(hub.name).build() }, "svc-b", "i-2", settings(null), { reply(it.id) }, scope).also { connections += it }
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val call = scope.async { sender.send(ping()) }
        await atMost wait until { "handler start" in events }
        events.clear()

        val rolled = scope.async { handler.rollOver(drainTimeout = Duration.ofSeconds(5), reconnectTimeout = Duration.ofSeconds(5)) }
        await atMost wait until { "register:" in events } // the empty Register: nothing to route to this instance any more
        delay(400)
        assertEquals(listOf("register:"), events.toList(), "while a request is in flight the stream stays open")
        assertFalse(rolled.isCompleted)

        release.complete(Unit)
        assertTrue(rolled.await())
        assertTrue(call.await().hasReply(), "the request finished with its reply")
        assertEquals(listOf("register:", "handler end", "half-close", "closed", "channel", "register:test.Ping"), events.toList())
        assertTrue(handler.awaitRegistered(wait))
        assertEquals(2, hub.streamsOpenedBy("i-1"))
    }

    @Test fun `a rollover that cannot drain within its bound closes anyway`() = runBlocking {
        val release = CompletableDeferred<Unit>()
        val handler = connection("i-1", settings = settings(certificate())) { req -> release.await(); reply(req.id) }
        handler.register(setOf("test.Ping"), emptySet())
        handler.start()
        val sender = HubConnection({ InProcessChannelBuilder.forName(hub.name).build() }, "svc-b", "i-2", settings(null), { reply(it.id) }, scope).also { connections += it }
        sender.start()
        assertTrue(handler.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val call = scope.async { runCatching { sender.send(ping()) } }
        await atMost wait until { handler.inFlightHandlers.value == 1 }

        val started = System.nanoTime()
        assertTrue(handler.rollOver(drainTimeout = Duration.ofMillis(300), reconnectTimeout = Duration.ofSeconds(5)))
        assertTrue((System.nanoTime() - started) / 1_000_000 < 4_000)
        assertEquals(2, hub.streamsOpenedBy("i-1"))
        release.complete(Unit)
        call.await()
        Unit
    }

    @Test fun `a rollover with no live stream is a no-op that reports done`() = runBlocking {
        val conn = connection("i-1", settings = settings(certificate()))
        assertTrue(conn.rollOver(drainTimeout = Duration.ofMillis(100), reconnectTimeout = Duration.ofMillis(100)))
        assertEquals(emptyList(), events.toList())
    }
}
