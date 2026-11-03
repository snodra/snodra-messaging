package io.damoreira.kraken.messaging.e2e

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.TestPki
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.DefaultHubChannelFactory
import io.damoreira.kraken.messaging.hub.HubAuthKind
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubClientCertificate
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.HubSettings
import io.damoreira.kraken.messaging.hub.HubTargets
import io.damoreira.kraken.messaging.hub.HubTls
import io.damoreira.kraken.messaging.test.AclEntry
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.Status
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.io.ByteArrayInputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.security.cert.CertificateFactory
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import javax.net.ssl.SSLContext
import javax.net.ssl.TrustManagerFactory
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Mutual TLS (P5c, ADR 0020) against the real hub image (`KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`) in mode `enforce` with
 * `KRAKEN_HUB_TLS_CLIENT_AUTH=required` and `[[service]]` tables. A CA and every certificate are generated at test time (nothing is
 * checked in); the hub's own pair is issued by the same CA with both usages, so the image's health check (which presents it) passes.
 * The tests skip when the image is not built.
 */
class HubMutualTlsEndToEndTest {
    @TempDir lateinit var dir: Path
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val clients = mutableListOf<HubConnections>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val wait = Duration.ofSeconds(30)
    private val json = JsonMapper.builder().build()
    private lateinit var ca: TestPki.Ca
    private lateinit var serverPair: Pair<Path, Path>

    private val services = listOf(
        AclEntry("orders-service", handle = listOf("test.GetCount", "test.Ping"), send = emptyList()),
        AclEntry("billing-service", handle = emptyList(), send = listOf("test.GetCount")),
    )

    private val log = LoggerFactory.getLogger(HubConnection::class.java) as Logger
    private val certLog = LoggerFactory.getLogger(HubClientCertificate::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach fun setUp() {
        ca = TestPki.ca(dir.resolve("ca"))
        serverPair = TestPki.hubPair(ca, dir.resolve("hub"))
        appender.start()
        log.addAppender(appender)
        certLog.addAppender(appender)
    }

    @AfterEach fun tearDown() = runBlocking {
        log.detachAppender(appender)
        certLog.detachAppender(appender)
        connections.forEach { it.stop() }
        clients.forEach { it.stop() }
        scope.cancel()
        containers.forEach { runCatching { it.stop() } }
    }

    private fun hub(id: String = "e2e-hub-" + UUID.randomUUID().toString().take(8), clientAuth: String = "required"): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubImageAvailable(), "hub image not available")
        val acl = dir.resolve("acl-$id").also { Files.createDirectories(it) }
        return KrakenContainers.hubMtls(acl, services, serverPair.first, serverPair.second, ca.pem, clientAuth, mapOf("KRAKEN_HUB_ID" to id)).also {
            containers += it
            it.start()
        }
    }

    private fun target(hub: GenericContainer<*>): String {
        // 127.0.0.1, not "localhost": the forwarded port listens on IPv4 only (the hub's certificate lists both).
        val host = if (hub.host == "localhost") "127.0.0.1" else hub.host
        return "$host:${hub.getMappedPort(7000)}"
    }

    /** A pair of files for [service], issued by the CA. */
    private fun files(name: String, service: String? = null, validFor: Duration = Duration.ofHours(1), notBefore: Instant = Instant.now().minusSeconds(60)): Triple<Path, Path, TestPki.Issued> {
        val certFile = dir.resolve("$name.pem")
        val keyFile = dir.resolve("$name.key")
        return Triple(certFile, keyFile, ca.issue(service ?: name, validFor = validFor, notBefore = notBefore).also { it.writeTo(certFile, keyFile) })
    }

    private fun settings(certificate: HubClientCertificate?) = HubSettings(
        requestTimeout = null, heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(200), backoffMax = Duration.ofSeconds(1), clientCertificate = certificate,
    )

    private fun tls(certificate: HubClientCertificate?) = HubTls(enabled = true, caFile = ca.pem, clientCertificate = certificate)

    private fun connection(
        hub: GenericContainer<*>,
        service: String,
        certificate: HubClientCertificate?,
        instance: String = service + "-" + UUID.randomUUID(),
        inbound: suspend (Request) -> Reply = { req -> Reply.newBuilder().setRequestId(req.id).build() },
    ): HubConnection {
        val factory = DefaultHubChannelFactory(tls(certificate))
        val target = target(hub)
        return HubConnection({ factory.channel(target) }, service, instance, settings(certificate), inbound, scope, target = target).also { connections += it }
    }

    private fun multi(
        hubs: List<GenericContainer<*>>,
        service: String,
        certificate: HubClientCertificate,
        inbound: suspend (Request) -> Reply = { req -> Reply.newBuilder().setRequestId(req.id).build() },
    ): HubConnections {
        val factory = DefaultHubChannelFactory(tls(certificate))
        return HubConnections(
            targets = HubTargets.parse(hubs.joinToString(",") { target(it) }),
            channelFactory = HubChannelFactory { factory.channel(it) },
            serviceName = service, instanceId = service + "-" + UUID.randomUUID(), settings = settings(certificate), inbound = inbound, scope = scope,
            certificateCheckInterval = Duration.ofMillis(500),
        ).also { clients += it }
    }

    private fun bus(transport: io.damoreira.kraken.messaging.hub.HubTransport): HubBus {
        val resolver = MessageTypeResolver("test")
        val codec = JsonCodec()
        val registry = HandlerRegistry(resolver)
        return HubBus(transport, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), null)
    }

    private fun reply(id: String, json: String) = Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun certificate(files: Triple<Path, Path, TestPki.Issued>, service: String) = HubClientCertificate(files.first, files.second, service)

    private fun startOwner(hub: GenericContainer<*>, certificate: HubClientCertificate): HubConnection =
        connection(hub, "orders-service", certificate, inbound = { req -> reply(req.id, "{\"value\":7}") }).also {
            it.register(setOf("test.Ping"), emptySet(), setOf("test.GetCount"))
            it.start()
        }

    /** The hub's `/topology` over HTTPS (the pair is the hub's own), the instance [instanceId] of [service], or null. */
    private suspend fun instance(hub: GenericContainer<*>, service: String, instanceId: String): JsonNode? = withContext(Dispatchers.IO) {
        val trust = KeyStore.getInstance(KeyStore.getDefaultType()).apply {
            load(null)
            setCertificateEntry("ca", CertificateFactory.getInstance("X.509").generateCertificate(ByteArrayInputStream(Files.readAllBytes(ca.pem))))
        }
        val context = SSLContext.getInstance("TLS").apply {
            init(null, TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()).apply { init(trust) }.trustManagers, null)
        }
        val http = HttpClient.newBuilder().sslContext(context).connectTimeout(Duration.ofSeconds(3)).build()
        val host = if (hub.host == "localhost") "127.0.0.1" else hub.host
        val response = http.send(
            HttpRequest.newBuilder(URI.create("https://$host:${hub.getMappedPort(7001)}/topology")).timeout(Duration.ofSeconds(5)).build(),
            HttpResponse.BodyHandlers.ofString(),
        )
        check(response.statusCode() == 200) { "GET /topology: ${response.statusCode()} ${response.body().take(200)}" }
        json.readTree(response.body())["services"]
            .firstOrNull { it["serviceName"].asString() == service }
            ?.get("instances")?.firstOrNull { it["instanceId"].asString() == instanceId }
    }

    private suspend fun fingerprintOn(hub: GenericContainer<*>, service: String, instanceId: String): String? =
        runCatching { instance(hub, service, instanceId) }.getOrNull()?.get("certFingerprint")?.asString()

    private fun lines(level: Level) = appender.list.filter { it.level == level }.map { it.formattedMessage }

    @Test fun `a service with a client certificate registers, serves and sends, and the hub shows its certificate`() = runBlocking {
        val hub = hub()
        val ordersFiles = files("orders-service")
        val billingFiles = files("billing-service")
        val owner = startOwner(hub, certificate(ordersFiles, "orders-service"))
        val sender = connection(hub, "billing-service", certificate(billingFiles, "billing-service")).also { it.start() }
        assertTrue(owner.awaitRegistered(wait), "owner registered")
        assertTrue(sender.awaitRegistered(wait), "sender registered")
        assertEquals(null, owner.lastRejection.value)

        assertEquals(Count(7), bus(sender).send(GetCount("k")))
        val shown = instance(hub, "orders-service", owner.instanceId)!!
        assertEquals("certificate", shown["authKind"].asString())
        assertEquals(ordersFiles.third.fingerprint, shown["certFingerprint"].asString())
        assertEquals(ordersFiles.third.certificate.notAfter.time, shown["expiresAt"].asLong() / 1000 * 1000)
        assertEquals(emptyList(), lines(Level.ERROR))
    }

    @Test fun `a send outside the service's table is refused, one inside it works`() = runBlocking {
        val hub = hub()
        val owner = startOwner(hub, certificate(files("orders-service"), "orders-service"))
        val sender = connection(hub, "billing-service", certificate(files("billing-service"), "billing-service")).also { it.start() }
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        assertFailsWith<AccessDeniedException> { bus(sender).send(Ping("x")) }
        assertEquals(Count(7), bus(sender).send(GetCount("k")))
        assertTrue(sender.connected.value)
    }

    @Test fun `a client without a certificate fails at the transport and retries, with no registration rejection and no error spam`() = runBlocking {
        val hub = hub()
        // The hub is up (a certificate client reaches it); a client without one fails the handshake.
        assertTrue(connection(hub, "billing-service", certificate(files("billing-service"), "billing-service")).also { it.start() }.awaitRegistered(wait))
        appender.list.clear()
        val anonymous = connection(hub, "billing-service", null).also { it.start() }
        delay(4_000)
        assertFalse(anonymous.connected.value)
        assertNull(anonymous.lastRejection.value, "a transport failure is not a rejection")
        assertEquals(emptyList(), lines(Level.ERROR))
        assertTrue(lines(Level.WARN).any { "UNAVAILABLE" in it && "kraken.messaging.hub.tls.client-cert-file" in it }, lines(Level.WARN).toString())
        assertFalse(anonymous.awaitRegistered(Duration.ofMillis(500)))
    }

    @Test fun `a certificate that names another service than the one registered is refused with PERMISSION_DENIED`() = runBlocking {
        val hub = hub()
        val billing = certificate(files("billing-service"), "billing-service")
        val impostor = connection(hub, "orders-service", billing).also { it.start() } // billing's certificate, orders' name
        val e = assertFailsWith<HubRegistrationRejectedException> { impostor.awaitRegistered(wait) }
        assertTrue("PERMISSION_DENIED" in e.message!!, e.message)
        assertEquals(Status.Code.PERMISSION_DENIED, impostor.lastRejection.value!!.code)
    }

    @Test fun `a certificate from a CA the hub does not trust fails at the transport`() = runBlocking {
        val hub = hub()
        val otherCa = TestPki.ca(dir.resolve("other"), "other-ca")
        val certFile = dir.resolve("o.pem")
        val keyFile = dir.resolve("o.key")
        otherCa.issue("billing-service").writeTo(certFile, keyFile)
        val stranger = connection(hub, "billing-service", HubClientCertificate(certFile, keyFile, "billing-service")).also { it.start() }
        // The very first attempt may be refused by the port forwarder (no hint); a later one fails the handshake and carries it.
        await atMost Duration.ofSeconds(15) until { lines(Level.WARN).any { "KRAKEN_HUB_TLS_CLIENT_CA" in it } }
        assertFalse(stranger.connected.value)
        assertNull(stranger.lastRejection.value)
    }

    @Test fun `a certificate that lives a few seconds is closed by the hub at notAfter and the client comes back with the renewed file`() = runBlocking {
        val hub = hub()
        val short = files("orders-service", validFor = Duration.ofSeconds(18), notBefore = Instant.now().minusSeconds(10)) // ends about 8 s from now
        val owner = startOwner(hub, certificate(short, "orders-service"))
        assertTrue(owner.awaitRegistered(wait))
        assertEquals(short.third.fingerprint, fingerprintOn(hub, "orders-service", owner.instanceId))
        val renewed = ca.issue("orders-service").also { it.writeTo(short.first, short.second) } // the platform renewed the files

        await atMost Duration.ofSeconds(40) until { runBlocking { fingerprintOn(hub, "orders-service", owner.instanceId) } == renewed.fingerprint }
        assertTrue(owner.awaitRegistered(wait))
        assertNull(owner.lastRejection.value, "the close at notAfter is not a rejection")
        assertTrue(lines(Level.INFO).any { "client certificate ended" in it }, lines(Level.INFO).toString())
        assertEquals(emptyList(), lines(Level.ERROR))
    }

    @Test fun `rotating the files moves the sessions of both hubs to the new certificate, with no failed request in a steady trickle`() = runBlocking {
        val one = hub()
        val two = hub()
        val ordersFiles = files("orders-service")
        val certificate = certificate(ordersFiles, "orders-service")
        val owner = multi(listOf(one, two), "orders-service", certificate) { req -> reply(req.id, "{\"value\":7}") }
        owner.register(setOf("test.Ping"), emptySet(), setOf("test.GetCount"))
        owner.start()
        val billingCertificate = certificate(files("billing-service"), "billing-service")
        val sender = multi(listOf(one, two), "billing-service", billingCertificate)
        sender.start()
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        assertEquals(HubAuthKind.CERTIFICATE, owner.authStatus().kind)
        for (hub in listOf(one, two)) assertEquals(ordersFiles.third.fingerprint, fingerprintOn(hub, "orders-service", owner.instanceId))

        val failures = CopyOnWriteArrayList<String>()
        val sent = AtomicInteger()
        val trickle = scope.launch {
            val bus = bus(sender)
            while (isActive) {
                try {
                    assertEquals(Count(7), bus.send(GetCount("k")))
                    sent.incrementAndGet()
                } catch (e: Exception) {
                    failures += e.toString()
                }
                delay(20)
            }
        }
        delay(1_000)
        val renewed = ca.issue("orders-service").also { it.writeTo(ordersFiles.first, ordersFiles.second) }

        await atMost Duration.ofSeconds(40) until {
            runBlocking { listOf(one, two).all { fingerprintOn(it, "orders-service", owner.instanceId) == renewed.fingerprint } }
        }
        delay(1_000)
        trickle.cancel()
        assertEquals(emptyList(), failures.toList())
        assertTrue(sent.get() > 50, "the trickle ran: ${sent.get()}")
        assertEquals(renewed.fingerprint, owner.authStatus().certificateFingerprint)
        assertEquals(1L, owner.authStatus().certificateRotations)
        assertEquals(listOf(true, true), owner.connections().map { it.connected.value })
        assertEquals(emptyList(), lines(Level.ERROR))
    }
}
