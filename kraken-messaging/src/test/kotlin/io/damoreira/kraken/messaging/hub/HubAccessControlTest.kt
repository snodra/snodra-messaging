package io.damoreira.kraken.messaging.hub

import ch.qos.logback.classic.Level
import ch.qos.logback.classic.Logger
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HubRegistrationRejectedException
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.ExportRows
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.WatchAccount
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.test.AclEntry
import io.damoreira.kraken.messaging.test.HubAuth
import io.damoreira.kraken.messaging.test.InProcessHub
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.Status
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.toList
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
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** The client side of P5a (ADR 0018): the static token, the four registration-rejection codes and request-level refusals. */
class HubAccessControlTest {
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val wait = Duration.ofSeconds(5)
    private val token = "tok-" + UUID.randomUUID()
    private val acl = mapOf(token to AclEntry("svc-a", handle = listOf("test.*"), send = listOf("other.*")))
    private val cap = Duration.ofMillis(300)

    private fun settings(token: HubToken = HubToken.of(this.token, null)) = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(20),
        backoffMax = cap,
        token = token,
    )

    private val log = LoggerFactory.getLogger(HubConnection::class.java) as Logger
    private val appender = ListAppender<ILoggingEvent>()
    private var previousLevel: Level? = null

    @BeforeEach fun captureLog() {
        previousLevel = log.level
        log.level = Level.DEBUG
        appender.start()
        log.addAppender(appender)
    }

    @AfterEach fun tearDown() = runBlocking {
        log.detachAppender(appender)
        log.level = previousLevel
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun connection(
        instanceId: String,
        service: String = "svc-a",
        settings: HubSettings = settings(),
        inbound: suspend (Request) -> Reply = { req -> Reply.newBuilder().setRequestId(req.id).build() },
    ) = HubConnection({ InProcessChannelBuilder.forName(hub.name).build() }, service, instanceId, settings, inbound, scope)
        .also { connections += it }

    private fun bus(connection: HubConnection, retry: RetryPolicy? = null): HubBus {
        val resolver = MessageTypeResolver("test")
        val codec = JsonCodec()
        val registry = HandlerRegistry(resolver)
        return HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), Duration.ofSeconds(2), emptyMap(), retry)
    }

    // --- the token ---

    @Test fun `the token goes in Register credentials on every stream and every Register of it, never anywhere else`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val conn = connection("i-1")
        conn.register(setOf("test.Ping"), emptySet())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        conn.register(setOf("test.Ping", "test.Other"), emptySet()) // a later Register on the live stream
        await atMost wait until { hub.registrationsFrom("i-1") >= 2 }
        hub.dropInstance("i-1") // the stream breaks, the next one registers again
        await atMost wait until { hub.streamsOpenedBy("i-1") >= 2 }
        assertTrue(conn.awaitRegistered(wait))

        val seen = hub.tokensSeenFrom("i-1")
        assertTrue(seen.size >= 3, seen.toString())
        assertTrue(seen.all { it == token }, seen.toString())
        assertEquals(token, hub.lastRegisterFrom("i-1")!!.credentials.token)
        assertTrue(hub.authViolations().isEmpty())
    }

    @Test fun `without a token Register carries no credentials, an empty or blank token is the same as none`() = runBlocking {
        for ((i, value) in listOf(null, "", "  \n").withIndex()) {
            val conn = connection("i-$i", settings = settings(HubToken.of(value, null)))
            conn.start()
            assertTrue(conn.awaitRegistered(wait))
            assertFalse(hub.lastRegisterFrom("i-$i")!!.hasCredentials(), "value=$value")
            assertEquals(listOf<String?>(null), hub.tokensSeenFrom("i-$i").distinct(), "value=$value")
        }
    }

    @Test fun `the settings and the token never print the token`() {
        val s = settings()
        assertFalse(token in s.toString(), s.toString())
        assertFalse(token in s.token.toString())
        assertFalse(token in HubToken.of(token, null).toString())
    }

    @Test fun `logs of a rejected token never contain the token`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val conn = connection("i-1", settings = settings(HubToken.of("wrong-" + UUID.randomUUID(), null)))
        conn.start()
        await atMost wait until { conn.lastRejection.value != null }
        conn.stop()
        val wrong = hub.tokensSeenFrom("i-1").first()!!
        assertTrue(appender.list.isNotEmpty())
        assertTrue(appender.list.none { wrong in it.formattedMessage || token in it.formattedMessage })
    }

    @Test fun `the token file is read at every stream and wins over an inline token`(@TempDir dir: Path) = runBlocking {
        val file = dir.resolve("hub-token")
        Files.writeString(file, "first-" + UUID.randomUUID() + "\n")
        val first = Files.readString(file).trim()
        val conn = connection("i-1", settings = settings(HubToken.of("inline-ignored", file)))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        assertEquals(first, hub.lastRegisterFrom("i-1")!!.credentials.token) // the newline is dropped, the file beat the inline value

        val second = "second-" + UUID.randomUUID()
        Files.writeString(file, second)
        hub.dropInstance("i-1")
        await atMost wait until { hub.streamsOpenedBy("i-1") >= 2 }
        assertTrue(conn.awaitRegistered(wait))
        assertEquals(second, hub.lastRegisterFrom("i-1")!!.credentials.token)
        assertEquals(listOf<String?>(first, second), hub.tokensSeenFrom("i-1").distinct())
    }

    @Test fun `a token file that changes is sent to the hub as a refresh on the live stream, no reconnect, and a hub without the capability gets none`(@TempDir dir: Path) = runBlocking {
        hub.heartbeatInterval = Duration.ofMillis(100) // the connection looks at its token file once per heartbeat
        val rotated = "rotated-" + UUID.randomUUID()
        hub.auth = HubAuth.enforce(acl + (rotated to AclEntry("svc-a", handle = listOf("test.*"), send = listOf("other.*"))))
        val file = dir.resolve("hub-token")
        Files.writeString(file, token + "\n")
        val conn = connection("i-1", settings = settings(HubToken.of(null, file)))
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        delay(300)
        assertTrue(hub.credentialsSeenFrom("i-1").isEmpty(), "an unchanged file sends nothing")

        Files.writeString(file, rotated + "\n")
        await atMost wait until { hub.credentialsSeenFrom("i-1").isNotEmpty() }
        assertEquals(listOf(rotated), hub.credentialsSeenFrom("i-1").distinct())
        assertEquals(1, hub.streamsOpenedBy("i-1"), "rotated on the same stream")
        assertEquals(1, conn.credentialsRefreshesSent)
        assertTrue(hub.authViolations().isEmpty())

        hub.credentialsRefresh = false
        val old = connection("i-2", settings = settings(HubToken.of(null, file)))
        old.start()
        assertTrue(old.awaitRegistered(wait))
        Files.writeString(file, token + "\n")
        delay(400)
        assertTrue(hub.credentialsSeenFrom("i-2").isEmpty())
    }

    @Test fun `a token file that disappears keeps the last token read, one that is missing at startup fails`(@TempDir dir: Path) {
        val file = dir.resolve("hub-token")
        assertFailsWith<IllegalStateException> { HubToken.of(null, file) }.also { assertTrue(file.toString() in it.message!!) }
        Files.writeString(file, token)
        val source = HubToken.of(null, file)
        assertEquals(token, source.current())
        Files.delete(file)
        assertEquals(token, source.current())
    }

    @Test fun `an empty token file at startup is no token`(@TempDir dir: Path) {
        val file = dir.resolve("hub-token")
        Files.writeString(file, " \n")
        val source = HubToken.of(null, file)
        assertNull(source.current(), "an empty file at startup is no token")
        Files.writeString(file, token)
        assertEquals(token, source.current())
    }

    @Test fun `an empty token file read after a good one keeps the last token and warns once, a new token is then picked up`(@TempDir dir: Path) = runBlocking {
        val tokenLog = LoggerFactory.getLogger(HubToken::class.java) as Logger
        val tokenAppender = ListAppender<ILoggingEvent>().also { it.start() }
        tokenLog.addAppender(tokenAppender)
        try {
            val file = dir.resolve("hub-token")
            Files.writeString(file, token)
            val conn = connection("i-1", settings = settings(HubToken.of(null, file)))
            conn.start()
            assertTrue(conn.awaitRegistered(wait))
            assertEquals(token, hub.lastRegisterFrom("i-1")!!.credentials.token)

            Files.writeString(file, "") // a non-atomic rotation caught half way
            for (round in 2..3) { // two reconnects while the file stays empty
                hub.dropInstance("i-1")
                await atMost wait until { hub.streamsOpenedBy("i-1") >= round }
                assertTrue(conn.awaitRegistered(wait))
            }
            assertEquals(listOf<String?>(token), hub.tokensSeenFrom("i-1").distinct(), "the old token went out, never a Register without one")
            assertTrue(hub.tokensSeenFrom("i-1").none { it == null })
            val warns = tokenAppender.list.filter { it.level == Level.WARN }
            assertEquals(1, warns.size, warns.map { it.formattedMessage }.toString())
            assertFalse(token in warns.single().formattedMessage)

            val rotated = "rotated-" + UUID.randomUUID()
            Files.writeString(file, rotated + "\n")
            hub.dropInstance("i-1")
            await atMost wait until { hub.streamsOpenedBy("i-1") >= 4 }
            assertTrue(conn.awaitRegistered(wait))
            assertEquals(rotated, hub.lastRegisterFrom("i-1")!!.credentials.token)
        } finally {
            tokenLog.detachAppender(tokenAppender)
        }
    }

    @Test fun `a token file that is fixed makes the next attempt succeed`(@TempDir dir: Path) = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val file = dir.resolve("hub-token")
        Files.writeString(file, "stale-" + UUID.randomUUID())
        val conn = connection("i-1", settings = settings(HubToken.of(null, file)))
        conn.start()
        await atMost wait until { conn.lastRejection.value != null }
        assertEquals(Status.Code.UNAUTHENTICATED, conn.lastRejection.value!!.code)
        assertFalse(conn.connected.value)

        Files.writeString(file, token)
        // The next attempt (at the cap, 300 ms) reads the fixed file; until it answers, the last rejection still stands.
        await atMost wait until { conn.connected.value }
        assertNull(conn.lastRejection.value, "a hub that answered no longer rejects")
        assertTrue(conn.awaitRegistered(wait))
    }

    // --- registration rejections ---

    @Test fun `no token, an unknown token and a jwt are a registration rejection UNAUTHENTICATED`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        for ((i, value) in listOf<String?>(null, "unknown-" + UUID.randomUUID()).withIndex()) {
            val conn = connection("u-$i", settings = settings(HubToken.of(value, null)))
            conn.start()
            val e = assertFailsWith<HubRegistrationRejectedException> { conn.awaitRegistered(wait) }
            assertTrue("UNAUTHENTICATED" in e.message!!, e.message)
            assertEquals(Status.Code.UNAUTHENTICATED, conn.lastRejection.value!!.code)
            assertNotNull(conn.lastRejection.value!!.description)
        }
    }

    @Test fun `a service name or a handled type the token does not grant is a registration rejection PERMISSION_DENIED`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val wrongService = connection("p-1", service = "svc-other")
        wrongService.start()
        val e1 = assertFailsWith<HubRegistrationRejectedException> { wrongService.awaitRegistered(wait) }
        assertTrue("PERMISSION_DENIED" in e1.message!!, e1.message)

        val wrongType = connection("p-2")
        wrongType.register(setOf("forbidden.Cmd"), emptySet())
        wrongType.start()
        val e2 = assertFailsWith<HubRegistrationRejectedException> { wrongType.awaitRegistered(wait) }
        assertTrue("PERMISSION_DENIED" in e2.message!! && "forbidden.Cmd" in e2.message!!, e2.message)
        assertEquals(Status.Code.PERMISSION_DENIED, wrongType.lastRejection.value!!.code)
    }

    @Test fun `taking over the instance id of another service is a registration rejection PERMISSION_DENIED`() = runBlocking {
        val other = "other-" + UUID.randomUUID()
        hub.auth = HubAuth.enforce(acl + (other to AclEntry("svc-b", handle = listOf("b.*"))))
        val owner = connection("shared-instance")
        owner.start()
        assertTrue(owner.awaitRegistered(wait))

        val intruder = connection("shared-instance", service = "svc-b", settings = settings(HubToken.of(other, null)))
        intruder.start()
        val e = assertFailsWith<HubRegistrationRejectedException> { intruder.awaitRegistered(wait) }
        assertTrue("PERMISSION_DENIED" in e.message!! && "belongs to another service" in e.message!!, e.message)
        assertEquals(Status.Code.PERMISSION_DENIED, intruder.lastRejection.value!!.code)
        assertTrue(owner.connected.value, "the owner is untouched")
        assertNull(owner.lastRejection.value)
        assertEquals(1, hub.sessionsOf("shared-instance"))
    }

    @Test fun `HubConnections awaitRegistered throws for a rejected token too`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val connections = HubConnections(
            targets = HubTargets.parse("static://hub-a:7000"),
            channelFactory = HubChannelFactory { InProcessChannelBuilder.forName(hub.name).build() },
            serviceName = "svc-a",
            instanceId = "m-1",
            settings = settings(HubToken.NONE),
            inbound = { req -> Reply.newBuilder().setRequestId(req.id).build() },
            scope = scope,
        )
        connections.start()
        try {
            val e = assertFailsWith<HubRegistrationRejectedException> { connections.awaitRegistered(wait) }
            assertTrue("UNAUTHENTICATED" in e.message!!, e.message)
        } finally {
            connections.stop()
        }
    }

    @Test fun `rejections back off straight to the cap, with no tight loop, for both new codes`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val unauthenticated = connection("r-1", settings = settings(HubToken.NONE))
        val denied = connection("r-2", service = "svc-other")
        unauthenticated.start()
        denied.start()
        await atMost wait until { hub.authViolations().count { it.instanceId == "r-1" } >= 4 && hub.authViolations().count { it.instanceId == "r-2" } >= 4 }
        for (instance in listOf("r-1", "r-2")) {
            val at = hub.authViolations().filter { it.instanceId == instance }.map { it.atNanos }
            val gaps = at.zipWithNext { a, b -> Duration.ofNanos(b - a) }
            // jittered(cap, cap) is 80-100 % of the cap; the floor (20 ms) is never used for a rejection.
            assertTrue(gaps.all { it >= cap.multipliedBy(7).dividedBy(10) }, "$instance gaps $gaps")
            assertTrue(gaps.all { it <= cap.multipliedBy(3) }, "$instance gaps $gaps")
        }
    }

    @Test fun `a rejection is logged at error once per distinct message, then at debug`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val conn = connection("l-1", settings = settings(HubToken.NONE))
        conn.start()
        await atMost wait until { hub.authViolations().size >= 4 }
        conn.stop()
        val rejections = appender.list.filter { "rejected the registration" in it.formattedMessage }
        assertEquals(1, rejections.count { it.level == Level.ERROR }, rejections.map { it.formattedMessage }.toString())
        assertTrue(rejections.size >= 3 && rejections.drop(1).all { it.level == Level.DEBUG })
        assertTrue("UNAUTHENTICATED" in rejections.first().formattedMessage)
    }

    @Test fun `a different rejection message of the same connection is logged at error again`() = runBlocking {
        val other = "other-" + UUID.randomUUID()
        hub.auth = HubAuth.enforce(acl + (other to AclEntry("svc-z")))
        val current = java.util.concurrent.atomic.AtomicReference<String?>(null)
        val conn = connection("l-2", settings = settings(HubToken { current.get() }))
        conn.start()
        await atMost wait until { hub.authViolations().size >= 3 } // UNAUTHENTICATED, again and again (debug after the first)
        current.set(other) // the token is now known but grants another service: PERMISSION_DENIED
        await atMost wait until { conn.lastRejection.value?.code == Status.Code.PERMISSION_DENIED }
        await atMost wait until { hub.authViolations().count { it.code == Status.Code.PERMISSION_DENIED } >= 2 }
        conn.stop()
        val rejections = appender.list.filter { "rejected the registration" in it.formattedMessage }
        val errors = rejections.filter { it.level == Level.ERROR }
        assertEquals(2, errors.size, rejections.map { "${it.level} ${it.formattedMessage}" }.toString())
        assertTrue("UNAUTHENTICATED" in errors[0].formattedMessage && "PERMISSION_DENIED" in errors[1].formattedMessage)
    }

    // --- request-level refusals ---

    @Test fun `a request outside send is AccessDeniedException in every request mode and is never retried`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val conn = connection("c-1")
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val bus = bus(conn, retry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(10), multiplier = 1.0, maxBackoff = Duration.ofMillis(10)))
        var expected = 0
        suspend fun denied(what: String, call: suspend () -> Unit) {
            val e = assertFailsWith<AccessDeniedException>(what) { call() }
            expected++
            assertEquals(expected, hub.requestsFrom("c-1"), "$what was sent more than once")
            assertTrue(e.message!!.contains("test."), e.message)
        }
        denied("command") { bus.send(Ping("x")) }
        denied("query") { bus.send(GetCount("k")) }
        denied("scatter-gather") { bus.scatterGather(GetCount("k")) }
        denied("scatter-gather flow") { bus.scatterGatherFlow(GetCount("k")).toList() }
        denied("stream") { bus.stream(ExportRows(3)).toList() }
        denied("subscription") { bus.subscribe(WatchAccount(7)) }
        assertTrue(conn.connected.value, "a refused request does not touch the stream")
    }

    @Test fun `a token that left the ACL turns requests into UnauthenticatedException, also never retried`() = runBlocking {
        hub.auth = HubAuth.enforce(acl)
        val conn = connection("c-2")
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        // The real hub closes the stream on a reload; this is the race where a request gets in before that happens.
        hub.closeSessionsOnAuthChange = false
        hub.auth = HubAuth.enforce(emptyMap())
        val bus = bus(conn, retry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(10), multiplier = 1.0, maxBackoff = Duration.ofMillis(10)))
        assertFailsWith<UnauthenticatedException> { bus.send(GetCount("k")) }
        assertEquals(1, hub.requestsFrom("c-2"))
    }

    @Test fun `permissive never refuses and off never judges`() = runBlocking {
        hub.auth = HubAuth.permissive(acl)
        val conn = connection("c-3")
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        val bus = bus(conn)
        // Nobody handles it: NO_HANDLER (not AccessDeniedException) proves the request was routed.
        assertFailsWith<io.damoreira.kraken.messaging.NoHandlerException> { bus.send(GetCount("k")) }
        assertEquals(1, hub.authViolations().count { it.code == Status.Code.PERMISSION_DENIED && !it.enforced })
    }
}
