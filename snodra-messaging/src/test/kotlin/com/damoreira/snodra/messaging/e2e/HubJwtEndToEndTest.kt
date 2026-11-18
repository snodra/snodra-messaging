package com.damoreira.snodra.messaging.e2e

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.messaging.AccessDeniedException
import com.damoreira.snodra.messaging.HubRegistrationRejectedException
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Count
import com.damoreira.snodra.messaging.fixtures.GetCount
import com.damoreira.snodra.messaging.fixtures.Ping
import com.damoreira.snodra.messaging.fixtures.TestOAuth2Server
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.hub.DefaultHubChannelFactory
import com.damoreira.snodra.messaging.hub.HubBus
import com.damoreira.snodra.messaging.hub.HubConnection
import com.damoreira.snodra.messaging.hub.HubOAuth2
import com.damoreira.snodra.messaging.hub.HubSettings
import com.damoreira.snodra.messaging.hub.HubTokenProvider
import com.damoreira.snodra.messaging.hub.TokenEndpoint
import com.damoreira.snodra.messaging.test.AclEntry
import com.damoreira.snodra.messaging.test.SnodraContainers
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import io.grpc.Status
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.slf4j.LoggerFactory
import org.testcontainers.Testcontainers
import org.testcontainers.containers.GenericContainer
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * JWT authentication (P5b, ADR 0019) against the real hub image (`SNODRA_SERVER_IMAGE`, default `snodra-server:dev`) in mode `enforce`
 * with `[[service]]` tables. A [TestOAuth2Server] on the host plays Keycloak: it issues the access tokens (RS256, a key made at test
 * time) and serves the JWKS the hub container fetches through `host.testcontainers.internal`. Leeway 0 s and expiry grace 2 s, so
 * `exp` is exact. The tests skip when the image is not built.
 */
class HubJwtEndToEndTest {
    @TempDir lateinit var dir: Path
    private val oauth = TestOAuth2Server()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val wait = Duration.ofSeconds(30)

    private val services = listOf(
        AclEntry("orders-service", handle = listOf("test.GetCount", "test.Ping"), send = emptyList()),
        AclEntry("billing-service", handle = emptyList(), send = listOf("test.GetCount")),
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
        oauth.close()
    }

    private fun hub(): GenericContainer<*> {
        Assumptions.assumeTrue(SnodraContainers.hubImageAvailable(), "hub image not available")
        Testcontainers.exposeHostPorts(oauth.port)
        return SnodraContainers.hubJwt(dir, services, oauth.issuer, "http://host.testcontainers.internal:${oauth.port}/certs").also {
            containers += it
            it.start()
        }
    }

    private fun provider(service: String) = HubTokenProvider(
        TokenEndpoint(HubOAuth2(oauth.tokenUri, service, { "the-secret" })), scope,
        retryMin = Duration.ofMillis(200), retryMax = Duration.ofSeconds(1),
    )

    private fun connection(
        hub: GenericContainer<*>,
        service: String,
        provider: HubTokenProvider = provider(service),
        inbound: suspend (Request) -> Reply = { req -> Reply.newBuilder().setRequestId(req.id).build() },
    ): HubConnection {
        val settings = HubSettings(
            requestTimeout = null,
            backoffMin = Duration.ofMillis(200), backoffMax = Duration.ofSeconds(1), tokenProvider = provider,
        )
        val factory = DefaultHubChannelFactory()
        val host = if (hub.host == "localhost") "127.0.0.1" else hub.host
        val target = "$host:${hub.getMappedPort(7000)}"
        return HubConnection({ factory.channel(target) }, service, service + "-" + UUID.randomUUID(), settings, inbound, scope, target = target)
            .also { connections += it }
    }

    private fun bus(connection: HubConnection): HubBus {
        val resolver = MessageTypeResolver("test")
        val codec = JsonCodec()
        val registry = HandlerRegistry(resolver)
        return HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), null)
    }

    private fun reply(id: String, json: String) = Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun startOwner(hub: GenericContainer<*>): HubConnection =
        connection(hub, "orders-service", inbound = { req -> reply(req.id, "{\"value\":7}") }).also {
            it.register(setOf("test.Ping"), emptySet(), setOf("test.GetCount"))
            it.start()
        }

    @Test fun `a service that registers with a Keycloak-style access token serves a query from another JWT service`() = runBlocking {
        val hub = hub()
        val owner = startOwner(hub)
        val sender = connection(hub, "billing-service").also { it.start() }
        assertTrue(owner.awaitRegistered(wait), "owner registered")
        assertTrue(sender.awaitRegistered(wait), "sender registered")
        assertEquals(null, owner.lastRejection.value)

        assertEquals(Count(7), bus(sender).send(GetCount("k")))
        assertEquals(setOf("orders-service", "billing-service"), oauth.requests.mapNotNull { it.user }.toSet())
    }

    @Test fun `a send outside the service's ACL entry is refused, one inside it works`() = runBlocking {
        val hub = hub()
        val owner = startOwner(hub)
        val sender = connection(hub, "billing-service").also { it.start() }
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        assertFailsWith<AccessDeniedException> { bus(sender).send(Ping("x")) }
        assertEquals(Count(7), bus(sender).send(GetCount("k")))
        assertTrue(sender.connected.value)
    }

    @Test fun `a token whose azp is not the service name is refused with PERMISSION_DENIED, and no session is kept`() = runBlocking {
        val hub = hub()
        oauth.azpOverride = "orders-service" // the token says orders-service, the client registers as billing-service
        val impostor = connection(hub, "billing-service").also { it.start() }
        val e = assertFailsWith<HubRegistrationRejectedException> { impostor.awaitRegistered(wait) }
        assertTrue("PERMISSION_DENIED" in e.message!! && "azp" in e.message!!, e.message)
        assertEquals(Status.Code.PERMISSION_DENIED, impostor.lastRejection.value!!.code)
        assertTrue(!impostor.connected.value)
    }

    @Test fun `a service with no ACL table is refused even with a valid token`() = runBlocking {
        val hub = hub()
        val stranger = connection(hub, "files-service").also { it.start() }
        val e = assertFailsWith<HubRegistrationRejectedException> { stranger.awaitRegistered(wait) }
        assertTrue("PERMISSION_DENIED" in e.message!!, e.message)
    }

    @Test fun `the session outlives the token's expiry plus the hub's grace because the renewed token reaches the hub, with no reconnect`() = runBlocking {
        val hub = hub()
        oauth.expiresIn = 6 // the provider renews after 3 s; the hub would close the session 2 s after exp (8 s), enforce
        val owner = startOwner(hub)
        val sender = connection(hub, "billing-service").also { it.start() }
        assertTrue(owner.awaitRegistered(wait) && sender.awaitRegistered(wait))
        val connectsBefore = appender.list.count { "Connected to hub" in it.formattedMessage }

        delay(12_000) // well past exp + grace of the first token (8 s) and of the second (11 s)
        assertTrue(oauth.tokens.size >= 3, "the provider renewed in the background: ${oauth.tokens.size} tokens")
        assertTrue(owner.connected.value && sender.connected.value, "both sessions are still open")
        assertTrue(sender.credentialsRefreshesSent >= 2, "the hub received refreshes: ${sender.credentialsRefreshesSent}")
        assertEquals(connectsBefore, appender.list.count { "Connected to hub" in it.formattedMessage }, "nobody reconnected")
        assertEquals(null, sender.lastRejection.value)
        assertEquals(Count(7), bus(sender).send(GetCount("k"))) // and it still works on the same stream
        assertTrue(appender.list.none { it.level == Level.ERROR }, appender.list.filter { it.level == Level.ERROR }.joinToString { it.formattedMessage })
    }
}
