package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.server.v1.HubGrpcKt
import com.damoreira.snodra.server.v1.TopologyRequest
import com.damoreira.snodra.server.v1.TopologyResponse
import com.damoreira.snodra.messaging.fixtures.TestPki
import com.damoreira.snodra.messaging.test.TestCertificates
import io.grpc.Grpc
import io.grpc.ManagedChannel
import io.grpc.Metadata
import io.grpc.Server
import io.grpc.ServerCall
import io.grpc.ServerCallHandler
import io.grpc.ServerInterceptor
import io.grpc.ServerInterceptors
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.TlsServerCredentials
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path
import java.security.MessageDigest
import java.security.cert.X509Certificate
import java.util.concurrent.CopyOnWriteArrayList
import javax.net.ssl.SSLSession
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** [DefaultHubChannelFactory] with a client certificate against a real TLS gRPC server that requires one (Netty, certificates made for the test). */
class HubMutualTlsChannelTest {
    @TempDir lateinit var dir: Path
    private lateinit var serverTls: TestCertificates
    private lateinit var clientCa: TestPki.Ca
    private lateinit var server: Server
    private val channels = mutableListOf<ManagedChannel>()
    private val seen = CopyOnWriteArrayList<String>() // fingerprint prefix of each client certificate the server read

    private val certFile get() = dir.resolve("client.pem")
    private val keyFile get() = dir.resolve("client.key")

    private class Service : HubGrpcKt.HubCoroutineImplBase() {
        override suspend fun topology(request: TopologyRequest): TopologyResponse = TopologyResponse.newBuilder().setHubId("mtls-test-hub").build()
    }

    @BeforeEach fun startServer() {
        serverTls = TestCertificates.generate(dir.resolve("server"), listOf("localhost", "127.0.0.1"))
        clientCa = TestPki.ca(dir.resolve("ca"))
        val credentials = TlsServerCredentials.newBuilder()
            .keyManager(serverTls.certificate.toFile(), serverTls.privateKey.toFile())
            .trustManager(clientCa.pem.toFile())
            .clientAuth(TlsServerCredentials.ClientAuth.REQUIRE)
            .build()
        val reader = object : ServerInterceptor {
            override fun <ReqT, RespT> interceptCall(call: ServerCall<ReqT, RespT>, headers: Metadata, next: ServerCallHandler<ReqT, RespT>): ServerCall.Listener<ReqT> {
                val session = call.attributes.get(Grpc.TRANSPORT_ATTR_SSL_SESSION) as SSLSession?
                (session?.peerCertificates?.firstOrNull() as? X509Certificate)?.let {
                    seen += MessageDigest.getInstance("SHA-256").digest(it.encoded).joinToString("") { b -> "%02x".format(b) }.take(8)
                }
                return next.startCall(call, headers)
            }
        }
        server = Grpc.newServerBuilderForPort(0, credentials).addService(ServerInterceptors.intercept(Service(), reader)).build().start()
    }

    @AfterEach fun stop() {
        channels.forEach { it.shutdownNow() }
        server.shutdownNow()
    }

    private fun certificate(service: String = "billing-service", ca: TestPki.Ca = clientCa) = ca.issue(service).also { it.writeTo(certFile, keyFile) }

    private fun tls(certificate: HubClientCertificate? = null, enabled: Boolean = true) =
        HubTls(enabled = enabled, caFile = serverTls.certificate, clientCertificate = certificate)

    private fun call(tls: HubTls): String = runBlocking {
        val channel = DefaultHubChannelFactory(tls).channel("localhost:${server.port}").also { channels += it }
        withTimeout(10_000) { HubGrpcKt.HubCoroutineStub(channel).topology(TopologyRequest.getDefaultInstance()).hubId }
    }

    @Test fun `a channel with a client certificate completes a handshake that requires one, and the server reads that certificate`() {
        val issued = certificate()
        assertEquals("mtls-test-hub", call(tls(HubClientCertificate(certFile, keyFile, "billing-service"))))
        assertEquals(listOf(issued.fingerprint), seen.toList())
    }

    @Test fun `a client certificate turns TLS on by itself, as the setting implies it`() {
        certificate()
        assertEquals("mtls-test-hub", call(tls(HubClientCertificate(certFile, keyFile, "billing-service"), enabled = false)))
    }

    @Test fun `without a client certificate the handshake against a server that requires one fails at the transport`() {
        val e = assertFailsWith<StatusException> { call(tls()) }
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
        assertTrue(seen.isEmpty())
    }

    @Test fun `a certificate from a CA the server does not trust fails at the transport`() {
        val otherCa = TestPki.ca(dir.resolve("other-ca"), "other-ca")
        certificate(ca = otherCa)
        val e = assertFailsWith<StatusException> { call(tls(HubClientCertificate(certFile, keyFile, "billing-service"))) }
        assertEquals(Status.Code.UNAVAILABLE, e.status.code)
    }

    @Test fun `every new channel presents the pair the connection attempt read just before, so a rotated pair needs no restart`() {
        val first = certificate()
        val source = HubClientCertificate(certFile, keyFile, "billing-service")
        val factory = DefaultHubChannelFactory(tls(source))
        fun hubId() = runBlocking {
            source.refresh(connecting = true) // the connection reads the files once per attempt (HubConnection.connectLoop); the factory only presents
            val channel = factory.channel("localhost:${server.port}").also { channels += it }
            withTimeout(10_000) { HubGrpcKt.HubCoroutineStub(channel).topology(TopologyRequest.getDefaultInstance()).hubId }
        }
        hubId()
        val second = certificate()
        hubId()
        assertEquals(listOf(first.fingerprint, second.fingerprint), seen.toList())
        assertEquals(1, source.rotations)
    }

    @Test fun `a replacement that is not valid is not presented, the last good pair is`() {
        val first = certificate()
        val source = HubClientCertificate(certFile, keyFile, "billing-service")
        Files.writeString(certFile, "garbage")
        assertEquals("mtls-test-hub", call(tls(source)))
        assertEquals(listOf(first.fingerprint), seen.toList())
    }

    @Test fun `a connection whose client certificate the hub refuses at the handshake retries and says what to check, with no rejection`() {
        val otherCa = TestPki.ca(dir.resolve("other-ca"), "other-ca")
        val issued = certificate(ca = otherCa)
        val log = org.slf4j.LoggerFactory.getLogger(HubConnection::class.java) as ch.qos.logback.classic.Logger
        val appender = ch.qos.logback.core.read.ListAppender<ch.qos.logback.classic.spi.ILoggingEvent>().also { it.start() }
        log.addAppender(appender)
        val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + kotlinx.coroutines.Dispatchers.Default)
        val source = HubClientCertificate(certFile, keyFile, "billing-service")
        val factory = DefaultHubChannelFactory(tls(source))
        val settings = HubSettings(null, java.time.Duration.ofSeconds(10), java.time.Duration.ofMillis(20), java.time.Duration.ofMillis(100), clientCertificate = source)
        val connection = HubConnection({ factory.channel("localhost:${server.port}") }, "billing-service", "i-1", settings, { error("no requests") }, scope)
        try {
            connection.start()
            org.awaitility.kotlin.await atMost java.time.Duration.ofSeconds(10) until { appender.list.any { it.level == ch.qos.logback.classic.Level.WARN } }
            val warning = appender.list.first { it.level == ch.qos.logback.classic.Level.WARN }.formattedMessage
            assertTrue(issued.fingerprint in warning && "SNODRA_SERVER_TLS_CLIENT_CA" in warning, warning)
            assertEquals(null, connection.lastRejection.value)
            assertTrue(appender.list.none { it.level == ch.qos.logback.classic.Level.ERROR })
        } finally {
            log.detachAppender(appender)
            runBlocking { connection.stop() }
            scope.cancel()
        }
    }
}
