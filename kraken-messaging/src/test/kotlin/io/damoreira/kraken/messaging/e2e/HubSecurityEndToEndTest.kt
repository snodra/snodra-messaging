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
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.ExportRows
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.WatchAccount
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.DefaultHubChannelFactory
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.hub.HubSettings
import io.damoreira.kraken.messaging.hub.HubTls
import io.damoreira.kraken.messaging.hub.HubToken
import io.damoreira.kraken.messaging.test.AclEntry
import io.damoreira.kraken.messaging.test.KrakenContainers
import io.damoreira.kraken.messaging.test.TestCertificates
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.Status
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Hub access control and TLS (P5a, ADR 0018) against the hub image under test (`KRAKEN_HUB_IMAGE`) in mode `enforce`, with an
 * ACL file and a TLS certificate generated for each test: tokens are random strings, the certificate is created at test
 * time, nothing is checked in. The tests skip when the image is not built.
 */
class HubSecurityEndToEndTest {
    @TempDir lateinit var dir: Path
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val wait = Duration.ofSeconds(20)

    private val ordersToken = "orders-" + UUID.randomUUID()
    private val billingToken = "billing-" + UUID.randomUUID()
    private val acl = mapOf(
        ordersToken to AclEntry("orders-service", handle = listOf("test.GetCount", "test.Ping"), send = emptyList()),
        billingToken to AclEntry("billing-service", handle = emptyList(), send = listOf("test.GetCount")),
    )

    private val log = LoggerFactory.getLogger(HubConnection::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach fun captureLog() {
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() = runBlocking {
        log.detachAppender(appender)
        connections.forEach { it.stop() }
        scope.cancel()
        containers.forEach { runCatching { it.stop() } }
    }

    private fun hub(tls: TestCertificates? = null, mode: String = "enforce"): GenericContainer<*> {
        Assumptions.assumeTrue(KrakenContainers.hubImageAvailable(), "hub image under test not built")
        return KrakenContainers.hubAcl(dir, acl, mode, tls).also {
            containers += it
            it.start()
        }
    }

    private fun connection(
        hub: GenericContainer<*>,
        service: String,
        token: String?,
        tls: TestCertificates?,
        instance: String = service + "-" + UUID.randomUUID(),
        inbound: suspend (Request) -> Reply = { req -> Reply.newBuilder().setRequestId(req.id).build() },
    ): HubConnection {
        val settings = HubSettings(
            requestTimeout = null,
            backoffMin = Duration.ofMillis(200), backoffMax = Duration.ofSeconds(1), token = HubToken.of(token, null),
        )
        val factory = DefaultHubChannelFactory(HubTls(enabled = tls != null, caFile = tls?.certificate))
        // 127.0.0.1, not "localhost": the forwarded port listens on IPv4 only, and a refused ::1 would hide the TLS mismatch (the certificate lists both).
        val host = if (hub.host == "localhost") "127.0.0.1" else hub.host
        val target = "$host:${hub.getMappedPort(7000)}"
        return HubConnection({ factory.channel(target) }, service, instance, settings, inbound, scope, target = target)
            .also { connections += it }
    }

    private fun bus(connection: HubConnection, retry: RetryPolicy? = null): HubBus {
        val resolver = MessageTypeResolver("test")
        val codec = JsonCodec()
        val registry = HandlerRegistry(resolver)
        return HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), retry)
    }

    private fun reply(id: String, json: String) = Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun startOwner(hub: GenericContainer<*>, tls: TestCertificates?): HubConnection =
        connection(hub, "orders-service", ordersToken, tls, inbound = { req -> reply(req.id, "{\"value\":7}") }).also {
            it.register(setOf("test.Ping"), emptySet(), setOf("test.GetCount"))
            it.start()
        }

    @Test fun `a service with the right token registers over TLS and serves a query from a second service`() = runBlocking {
        val tls = TestCertificates.generate(dir.resolve("tls"))
        val hub = hub(tls)
        val owner = startOwner(hub, tls)
        val sender = connection(hub, "billing-service", billingToken, tls).also { it.start() }
        assertTrue(owner.awaitRegistered(wait), "owner registered")
        assertTrue(sender.awaitRegistered(wait), "sender registered")
        assertEquals(null, owner.lastRejection.value)

        assertEquals(Count(7), bus(sender).send(GetCount("k")))
    }

    @Test fun `a wrong token is rejected with UNAUTHENTICATED, as is no token`() = runBlocking {
        val tls = TestCertificates.generate(dir.resolve("tls"))
        val hub = hub(tls)
        for (token in listOf("wrong-" + UUID.randomUUID(), null)) {
            val conn = connection(hub, "billing-service", token, tls).also { it.start() }
            val e = assertFailsWith<HubRegistrationRejectedException> { conn.awaitRegistered(wait) }
            assertTrue("UNAUTHENTICATED" in e.message!!, e.message)
            assertEquals(Status.Code.UNAUTHENTICATED, conn.lastRejection.value!!.code)
            assertFalse(conn.connected.value)
        }
    }

    @Test fun `a service name the token does not grant and a type outside handle are rejected with PERMISSION_DENIED`() = runBlocking {
        val tls = TestCertificates.generate(dir.resolve("tls"))
        val hub = hub(tls)
        val impostor = connection(hub, "orders-service", billingToken, tls).also { it.start() } // billing's token, orders' name
        assertTrue("PERMISSION_DENIED" in assertFailsWith<HubRegistrationRejectedException> { impostor.awaitRegistered(wait) }.message!!)
        val greedy = connection(hub, "orders-service", ordersToken, tls).also {
            it.register(setOf("test.Ping", "billing.Charge"), emptySet())
            it.start()
        }
        val e = assertFailsWith<HubRegistrationRejectedException> { greedy.awaitRegistered(wait) }
        assertTrue("PERMISSION_DENIED" in e.message!! && "billing.Charge" in e.message!!, e.message)
        assertEquals(Status.Code.PERMISSION_DENIED, greedy.lastRejection.value!!.code)
    }

    @Test fun `a send outside the ACL is AccessDeniedException in every request mode and is not retried, one inside it works`() = runBlocking {
        val tls = TestCertificates.generate(dir.resolve("tls"))
        val hub = hub(tls)
        val owner = startOwner(hub, tls)
        val sender = connection(hub, "billing-service", billingToken, tls).also { it.start() }
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val bus = bus(sender, RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(10), multiplier = 1.0, maxBackoff = Duration.ofMillis(10)))

        assertFailsWith<AccessDeniedException> { bus.send(Ping("x")) } // a command outside send
        assertFailsWith<AccessDeniedException> { bus.stream(ExportRows(2)).toList() }
        assertFailsWith<AccessDeniedException> { bus.subscribe(WatchAccount(1)) }
        assertEquals(Count(7), bus.send(GetCount("k"))) // test.GetCount is in send
        assertEquals(listOf(Count(7)), bus.scatterGather(GetCount("k")).results) // the type is allowed in every mode: shared owners answer
        assertTrue(sender.connected.value)
    }

    @Test fun `permissive lets a service without a token register and send, which is the rollout mode`() = runBlocking {
        val hub = hub(mode = "permissive")
        val owner = startOwner(hub, null) // with its token: fine either way
        val anonymous = connection(hub, "someone-else", null, null).also { it.start() }
        assertTrue(owner.awaitRegistered(wait) && anonymous.awaitRegistered(wait))
        assertEquals(Count(7), bus(anonymous).send(GetCount("k")))
    }

    @Test fun `plain text against the TLS hub fails cleanly with a log line that says what to do, not a hang`() = runBlocking {
        val tls = TestCertificates.generate(dir.resolve("tls"))
        val hub = hub(tls)
        // The hub serves (a first attempt right after the container started may be refused by the port forwarder instead).
        assertTrue(connection(hub, "billing-service", billingToken, tls).also { it.start() }.awaitRegistered(wait))
        appender.list.clear()
        val conn = connection(hub, "billing-service", billingToken, tls = null).also { it.start() } // TLS off: plain text
        val registered = withTimeout(30_000) { conn.awaitRegistered(Duration.ofSeconds(3)) }
        assertFalse(registered)
        assertFalse(conn.connected.value)
        assertEquals(null, conn.lastRejection.value) // a transport failure, not a rejection
        withTimeout(10_000) { while (appender.list.none { it.level == Level.WARN }) delay(50) }
        val warn = appender.list.first { it.level == Level.WARN }.formattedMessage
        assertTrue("kraken.messaging.hub.tls.enabled" in warn, warn)
        assertFalse(billingToken in appender.list.joinToString { it.formattedMessage })
    }
}
