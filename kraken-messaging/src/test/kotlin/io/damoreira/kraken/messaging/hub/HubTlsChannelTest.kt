package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.TopologyRequest
import io.damoreira.kraken.hub.v1.TopologyResponse
import io.damoreira.kraken.messaging.test.TestCertificates
import io.grpc.Grpc
import io.grpc.ManagedChannel
import io.grpc.Server
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.TlsServerCredentials
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.net.InetSocketAddress
import java.nio.file.Files
import java.nio.file.Path
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** [DefaultHubChannelFactory] against a real TLS gRPC server (Netty, a throwaway certificate generated for the test). */
class HubTlsChannelTest {
    @TempDir lateinit var dir: Path
    private lateinit var certificates: TestCertificates
    private lateinit var tlsServer: Server
    private lateinit var plainServer: Server
    private val channels = mutableListOf<ManagedChannel>()

    private class Service : HubGrpcKt.HubCoroutineImplBase() {
        override suspend fun topology(request: TopologyRequest): TopologyResponse = TopologyResponse.newBuilder().setHubId("tls-test-hub").build()
    }

    @BeforeEach fun startServers() {
        certificates = TestCertificates.generate(dir, listOf("localhost", "127.0.0.1", "hub.internal"))
        val credentials = TlsServerCredentials.create(certificates.certificate.toFile(), certificates.privateKey.toFile())
        tlsServer = Grpc.newServerBuilderForPort(0, credentials).addService(Service()).build().start()
        plainServer = io.grpc.ServerBuilder.forPort(0).addService(Service()).build().start()
    }

    @AfterEach fun stop() {
        channels.forEach { it.shutdownNow() }
        tlsServer.shutdownNow()
        plainServer.shutdownNow()
    }

    private val tlsPort get() = tlsServer.port
    private fun channel(tls: HubTls, target: String): ManagedChannel = DefaultHubChannelFactory(tls).channel(target).also { channels += it }

    private fun topology(channel: ManagedChannel): String = runBlocking {
        withTimeout(10_000) { HubGrpcKt.HubCoroutineStub(channel).topology(TopologyRequest.getDefaultInstance()).hubId }
    }

    private fun ca() = HubTls(enabled = true, caFile = certificates.certificate)

    @Test fun `TLS with the CA file works for a host name, an IP, dns and every target of a static list`() {
        val statics = (HubTargets.parse("static://localhost:$tlsPort,127.0.0.1:$tlsPort") as HubTargets.Static).targets
        val targets = listOf("localhost:$tlsPort", "127.0.0.1:$tlsPort", "dns:///localhost:$tlsPort") + statics
        for (target in targets) assertEquals("tls-test-hub", topology(channel(ca(), target)), target)
    }

    @Test fun `TLS without a CA file uses the system roots and rejects a certificate they do not know`() {
        val e = assertFailsWith<StatusException> { topology(channel(HubTls(enabled = true), "localhost:$tlsPort")) }
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
        assertTrue(e.message!!.contains("ssl", ignoreCase = true) || e.message!!.contains("certificate", ignoreCase = true) || e.message!!.contains("trust", ignoreCase = true), e.message)
    }

    @Test fun `a certificate for another name is refused, server-name verifies against the name given`() {
        // The certificate lists hub.internal, localhost and 127.0.0.1; a name it does not list fails.
        val refused = assertFailsWith<StatusException> { topology(channel(ca().copy(serverName = "elsewhere.example"), "localhost:$tlsPort")) }
        assertEquals(Status.Code.UNAVAILABLE, refused.status.code)
        assertEquals("tls-test-hub", topology(channel(ca().copy(serverName = "hub.internal"), "localhost:$tlsPort")))
    }

    @Test fun `a CA that did not sign the certificate is refused`() {
        val other = TestCertificates.generate(Files.createDirectory(dir.resolve("other")), listOf("localhost"))
        val e = assertFailsWith<StatusException> { topology(channel(HubTls(enabled = true, caFile = other.certificate), "localhost:$tlsPort")) }
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
    }

    @Test fun `plain text to a TLS server fails with UNAVAILABLE at once, not a hang`() {
        val started = System.nanoTime()
        val e = assertFailsWith<StatusException> { topology(channel(HubTls(), "localhost:$tlsPort")) }
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
        assertTrue((System.nanoTime() - started) / 1_000_000 < 8_000, "the failure took too long")
    }

    @Test fun `TLS to a plain-text server fails with UNAVAILABLE`() {
        val e = assertFailsWith<StatusException> { topology(channel(ca(), "localhost:${plainServer.port}")) }
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
    }

    @Test fun `plain text still works when TLS is off`() {
        assertEquals("tls-test-hub", topology(channel(HubTls(), "localhost:${plainServer.port}")))
    }

    @Test fun `a missing or unreadable CA file fails with the setting named`() {
        val e = assertFailsWith<IllegalStateException> { DefaultHubChannelFactory(HubTls(true, dir.resolve("absent.pem"))).validate() }
        assertTrue("kraken.messaging.hub.tls.ca-file" in e.message!!, e.message)
    }

    @Test fun `the mTLS hook is called for every TLS channel and never for a plain one`() {
        var calls = 0
        val factory = object : DefaultHubChannelFactory(ca()) {
            override fun customizeTls(credentials: io.grpc.TlsChannelCredentials.Builder) { calls++ }
        }
        factory.channel("localhost:$tlsPort").also { channels += it }
        assertEquals(1, calls)
        object : DefaultHubChannelFactory(HubTls()) {
            override fun customizeTls(credentials: io.grpc.TlsChannelCredentials.Builder) { calls++ }
        }.channel("localhost:${plainServer.port}").also { channels += it }
        assertEquals(1, calls)
        assertIs<DefaultHubChannelFactory>(factory)
    }

    // --- the log line of a mismatch (HubConnection over the same servers) ---

    private val connectionLog = org.slf4j.LoggerFactory.getLogger(HubConnection::class.java) as ch.qos.logback.classic.Logger

    private fun failingConnection(tls: HubTls, port: Int): List<String> = runBlocking {
        val appender = ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>().also { it.start() }
        connectionLog.addAppender(appender)
        val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default)
        val factory = DefaultHubChannelFactory(tls)
        val connection = HubConnection(
            { factory.channel("localhost:$port") }, "svc", "i-1",
            HubSettings(null, java.time.Duration.ofSeconds(10), java.time.Duration.ofMillis(50), java.time.Duration.ofMillis(200)),
            { io.damoreira.kraken.hub.v1.Reply.getDefaultInstance() }, scope,
        )
        try {
            connection.start()
            withTimeout(20_000) { while (appender.list.none { it.level == ch.qos.logback.classic.Level.WARN }) kotlinx.coroutines.delay(20) }
            appender.list.filter { it.level == ch.qos.logback.classic.Level.WARN }.map { it.formattedMessage }
        } finally {
            connection.stop()
            scope.cancel()
            connectionLog.detachAppender(appender)
        }
    }

    // gRPC reports both mismatches as a bare "io exception", so the one hint names both directions.
    @Test fun `a stream that fails because the hub serves TLS says to enable it`() {
        val warn = failingConnection(HubTls(), tlsPort).first()
        assertTrue("UNAVAILABLE" in warn && "set kraken.messaging.hub.tls.enabled=true" in warn, warn)
    }

    @Test fun `a stream that fails because the hub is plain text says to leave TLS off`() {
        val warn = failingConnection(ca(), plainServer.port).first()
        assertTrue("leave kraken.messaging.hub.tls.enabled off" in warn, warn)
    }

    @Test fun `an unreachable hub gets no TLS hint`() {
        val closed = java.net.ServerSocket(0).use { it.localPort } // nothing listens there any more
        val warn = failingConnection(HubTls(), closed).first()
        assertTrue("tls" !in warn.lowercase(), warn)
    }

    @Test fun `the TLS hint reaches WARN once even when the first attempt was a refusal`() = runBlocking {
        val closed = java.net.ServerSocket(0).use { it.localPort } // the hub is still starting: nothing listens yet
        val appender = ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>().also { it.start() }
        connectionLog.addAppender(appender)
        val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default)
        val factory = DefaultHubChannelFactory(HubTls())
        val attempts = java.util.concurrent.atomic.AtomicInteger()
        val connection = HubConnection(
            { factory.channel("localhost:${if (attempts.incrementAndGet() == 1) closed else tlsPort}") }, "svc", "i-1",
            HubSettings(null, java.time.Duration.ofSeconds(10), java.time.Duration.ofMillis(50), java.time.Duration.ofMillis(200)),
            { io.damoreira.kraken.hub.v1.Reply.getDefaultInstance() }, scope,
        )
        try {
            connection.start()
            withTimeout(20_000) { while (attempts.get() < 6) kotlinx.coroutines.delay(20) } // refused, then four mismatches
            kotlinx.coroutines.delay(300)
            val warns = appender.list.filter { it.level == ch.qos.logback.classic.Level.WARN }.map { it.formattedMessage }
            assertEquals(1, warns.count { "kraken.messaging.hub.tls.enabled" in it }, warns.toString())
            assertEquals(2, warns.size, warns.toString()) // the refusal (no hint), then the mismatch (hint)
            assertTrue("kraken.messaging.hub.tls.enabled" !in warns.first(), warns.first())
            val hinted = appender.list.filter { "kraken.messaging.hub.tls.enabled" in it.formattedMessage }
            assertTrue(hinted.drop(1).all { it.level == ch.qos.logback.classic.Level.DEBUG }, hinted.toString())
        } finally {
            connection.stop()
            scope.cancel()
            connectionLog.detachAppender(appender)
        }
    }
}
