package io.damoreira.kraken.messaging.hub

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.fixtures.TestOAuth2Server
import io.damoreira.kraken.messaging.fixtures.TokenResponse
import io.damoreira.kraken.messaging.test.AclEntry
import io.damoreira.kraken.messaging.test.HubAuth
import io.damoreira.kraken.messaging.test.InProcessHub
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The client side of P5b (ADR 0019): the access token in `Register`, shared by every connection, and pushed to live streams as `ClientMessage.credentials`. */
class HubJwtAuthTest {
    private val oauth = TestOAuth2Server()
    private val hubA = InProcessHub().start()
    private val hubB = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val wait = Duration.ofSeconds(10)
    private val service = "billing-service"
    private val services = listOf(AclEntry(service, handle = listOf("test.*"), send = listOf("*")))

    private val log = LoggerFactory.getLogger(HubConnection::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()

    @BeforeEach fun captureLog() {
        hubA.auth = HubAuth.enforce(emptyMap(), services)
        hubB.auth = HubAuth.enforce(emptyMap(), services)
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() = runBlocking {
        log.detachAppender(appender)
        connections.forEach { it.stop() }
        scope.cancel()
        hubA.close()
        hubB.close()
        oauth.close()
    }

    private fun provider() = HubTokenProvider(
        TokenEndpoint(HubOAuth2(oauth.tokenUri, service, { "the-secret" })), scope,
        retryMin = Duration.ofMillis(50), retryMax = Duration.ofMillis(200),
    )

    private fun settings(provider: HubTokenProvider?) = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(300),
        tokenProvider = provider,
    )

    private fun connection(hub: InProcessHub, instanceId: String, settings: HubSettings) = HubConnection(
        { InProcessChannelBuilder.forName(hub.name).build() }, service, instanceId, settings,
        { req: Request -> Reply.newBuilder().setRequestId(req.id).build() }, scope,
    ).also { connections += it }

    @Test fun `Register carries the access token as a jwt credential and no static token`() = runBlocking {
        val conn = connection(hubA, "i-1", settings(provider()))
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val token = oauth.tokens.single()
        assertEquals(listOf(token), hubA.jwtsSeenFrom("i-1"))
        assertEquals(token, hubA.lastRegisterFrom("i-1")!!.credentials.jwt)
        assertEquals(listOf<String?>(null), hubA.tokensSeenFrom("i-1").distinct())
        assertEquals(service, oauth.requests.single().user)
        assertTrue(hubA.authViolations().isEmpty())
    }

    @Test fun `two hubs share one provider, so one token and one fetch`() = runBlocking {
        val settings = settings(provider())
        val a = connection(hubA, "i-1", settings).also { it.start() }
        val b = connection(hubB, "i-1", settings).also { it.start() }
        assertTrue(a.awaitRegistered(wait) && b.awaitRegistered(wait))
        assertEquals(1, oauth.requests.size)
        assertEquals(hubA.jwtsSeenFrom("i-1"), hubB.jwtsSeenFrom("i-1"))
    }

    @Test fun `connections that start together wait for one fetch`() = runBlocking {
        oauth.delayMs = 300
        val settings = settings(provider())
        val started = (1..6).map { connection(hubA, "i-$it", settings).also { c -> c.start() } }
        for (c in started) assertTrue(c.awaitRegistered(wait))
        assertEquals(1, oauth.requests.size)
    }

    @Test fun `a reconnect registers with the current token, not the one the first stream used`() {
        oauth.expiresIn = 2
        val provider = provider()
        val conn = connection(hubA, "i-1", settings(provider)).also { it.start() }
        runBlocking { assertTrue(conn.awaitRegistered(wait)) }
        val first = hubA.jwtsSeenFrom("i-1").single()
        await atMost wait until { provider.current.value?.value != first }
        hubA.dropInstance("i-1")
        await atMost wait until { hubA.streamsOpenedBy("i-1") >= 2 }
        assertTrue(hubA.jwtsSeenFrom("i-1").last() != first, "the new stream presents a newer token")
    }

    // --- refresh on the wire ---

    @Test fun `a renewed token goes to live streams whose ack lists credentials-refresh, and to no other`() {
        hubB.credentialsRefresh = false // a pre-P5b hub: no capability in its ack
        oauth.expiresIn = 2
        val provider = provider()
        val settings = settings(provider)
        val capable = connection(hubA, "i-1", settings).also { it.start() }
        val incapable = connection(hubB, "i-1", settings).also { it.start() }
        runBlocking { assertTrue(capable.awaitRegistered(wait) && incapable.awaitRegistered(wait)) }
        val first = hubA.jwtsSeenFrom("i-1").single()

        await atMost wait until { hubA.credentialsSeenFrom("i-1").isNotEmpty() }
        val sent = hubA.credentialsSeenFrom("i-1")
        assertFalse(first in sent, "a fresh token, not the Register's")
        assertTrue(sent.all { it in oauth.tokens }, "only tokens the endpoint issued")
        assertTrue(hubB.credentialsSeenFrom("i-1").isEmpty(), "a hub that did not offer the capability gets nothing")
        assertEquals(1, hubA.sessionsOf("i-1"))
        assertEquals(1, hubA.streamsOpenedBy("i-1"), "refreshed on the same stream, no reconnect")
        assertTrue(capable.credentialsRefreshesSent >= 1)
        assertEquals(0, incapable.credentialsRefreshesSent)
    }

    @Test fun `a token renewed before the ack arrives is sent as soon as the handshake completes`() {
        oauth.expiresIn = 2
        hubA.ackDelay = Duration.ofMillis(1500) // the renewal (after 1 s) falls between Register and RegisterAck
        val provider = provider()
        val conn = connection(hubA, "i-1", settings(provider)).also { it.start() }
        await atMost wait until { hubA.credentialsSeenFrom("i-1").isNotEmpty() }
        assertEquals(1, hubA.streamsOpenedBy("i-1"))
        assertTrue(hubA.jwtsSeenFrom("i-1").single() != hubA.credentialsSeenFrom("i-1").first())
        assertTrue(conn.connected.value)
    }

    @Test fun `a failing renewal sends nothing, keeps the stream, and the next good token is sent`() {
        oauth.expiresIn = 2
        val provider = provider()
        val conn = connection(hubA, "i-1", settings(provider)).also { it.start() }
        runBlocking { assertTrue(conn.awaitRegistered(wait)) }
        oauth.responder = { TokenResponse(503, "down") }
        await atMost wait until { provider.fetches(TokenFetchResult.HTTP_ERROR) >= 2 }
        assertTrue(conn.connected.value)
        assertEquals(1, hubA.streamsOpenedBy("i-1"))
        val sentWhileDown = hubA.credentialsSeenFrom("i-1").size
        oauth.responder = null
        await atMost wait until { hubA.credentialsSeenFrom("i-1").size > sentWhileDown }
        assertEquals(1, hubA.streamsOpenedBy("i-1"))
    }

    // --- an endpoint that cannot give a token ---

    @Test fun `a token endpoint that refuses the client opens no stream, is logged at ERROR once, is retried and recovers`() = runBlocking {
        oauth.responder = { TokenResponse(401, """{"error":"invalid_client","error_description":"Invalid client credentials"}""") }
        val provider = provider()
        val conn = connection(hubA, "i-1", settings(provider)).also { it.start() }
        await atMost wait until { oauth.requests.size >= 4 } // retried at the normal reconnect back-off
        assertEquals(0, hubA.streamsOpenedBy("i-1"), "no stream without a token")
        assertEquals(1, appender.list.count { it.level == Level.ERROR }, appender.list.joinToString { it.formattedMessage })
        assertTrue("invalid_client" in appender.list.first { it.level == Level.ERROR }.formattedMessage)
        val status = assertNotNull(conn.lastRejection.value)
        assertEquals(Status.Code.UNAUTHENTICATED, status.code)
        val e = assertFailsWith<HubRegistrationRejectedException> { conn.awaitRegistered(wait) }
        assertTrue("invalid_client" in e.message!! && "access token" in e.message!!, e.message)
        assertFalse(conn.connected.value)

        oauth.responder = null
        await atMost wait until { conn.connected.value } // the stale refusal is cleared by the first stream that gets a token
        assertTrue(conn.awaitRegistered(wait))
        assertNull(conn.lastRejection.value)
        assertEquals(1, hubA.streamsOpenedBy("i-1"))
    }

    // --- a credential the hub refused (I1) ---

    /** A 200 token response whose access token the hub cannot parse (`malformed jwt`, UNAUTHENTICATED), good for an hour. */
    private fun garbageToken() = TokenResponse(200, """{"access_token":"not-a-jwt","token_type":"Bearer","expires_in":3600}""")

    @Test fun `a Register refused with UNAUTHENTICATED drops the cached token, so the next attempt fetches a new one`() {
        val garbage = AtomicBoolean(true)
        oauth.responder = { if (garbage.getAndSet(false)) garbageToken() else null }
        val conn = connection(hubA, "i-1", settings(provider())).also { it.start() }
        await atMost wait until { conn.connected.value } // awaitRegistered would throw at the first refusal
        assertEquals(2, oauth.requests.size, "one fetch per refused token, no more")
        assertEquals(listOf("not-a-jwt", oauth.tokens.single()), hubA.jwtsSeenFrom("i-1"))
    }

    @Test fun `a credentials refresh the hub refuses with UNAUTHENTICATED drops that token, so the reconnect fetches a new one`() {
        oauth.expiresIn = 2 // renewed after 1 s
        val renewals = AtomicInteger()
        oauth.responder = { if (renewals.getAndIncrement() == 1) garbageToken() else null } // the renewal is the garbage one, valid for an hour
        val conn = connection(hubA, "i-1", settings(provider())).also { it.start() }
        runBlocking { assertTrue(conn.awaitRegistered(wait)) }
        await atMost wait until { hubA.streamsOpenedBy("i-1") >= 2 } // the refused refresh closed the stream
        await atMost wait until { hubA.jwtsSeenFrom("i-1").size >= 2 }
        assertTrue("not-a-jwt" !in hubA.jwtsSeenFrom("i-1").drop(1), "the reconnect does not present the token the hub refused: ${hubA.jwtsSeenFrom("i-1")}")
        assertTrue(oauth.requests.size >= 3, "a third fetch: ${oauth.requests.size}")
    }

    @Test fun `a Register refused with PERMISSION_DENIED keeps the cached token and does not ask the endpoint again`() {
        hubA.auth = HubAuth.enforce(emptyMap(), listOf(AclEntry("other-service", handle = listOf("test.*")))) // no entry for this service: PERMISSION_DENIED
        val conn = connection(hubA, "i-1", settings(provider())).also { it.start() }
        await atMost wait until { hubA.jwtsSeenFrom("i-1").size >= 4 } // refused attempts count: the Register is recorded before it is judged
        assertEquals(Status.Code.PERMISSION_DENIED, conn.lastRejection.value?.code)
        assertEquals(1, oauth.requests.size, "a new token would not help: one fetch for all attempts")
        assertEquals(1, hubA.jwtsSeenFrom("i-1").distinct().size)
    }

    // --- secrecy ---

    @Test fun `the access token never appears in the settings, the connection's logs or the hub-facing exceptions`() = runBlocking {
        val provider = provider()
        val settings = settings(provider)
        val conn = connection(hubA, "i-1", settings).also { it.start() }
        assertTrue(conn.awaitRegistered(wait))
        val token = oauth.tokens.single()
        assertFalse(token in settings.toString(), settings.toString())
        assertFalse(token in provider.toString())
        assertFalse("the-secret" in settings.toString())
        assertFalse(token in appender.list.joinToString { it.formattedMessage })
    }
}
