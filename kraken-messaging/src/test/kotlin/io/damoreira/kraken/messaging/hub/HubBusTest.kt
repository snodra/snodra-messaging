package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.test.InProcessHub
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.handler.RequiresAuthentication
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.oauth2.jwt.JwtDecoder
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.slf4j.MDC
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import kotlin.concurrent.thread
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.SendOptions
import java.util.concurrent.CopyOnWriteArrayList

class LocalPingHandlers {
    @CommandHandler fun handle(command: Ping): Pong = Pong("local:${command.text}", Instant.EPOCH)
}

class AdminOnlyHandlers {
    @CommandHandler @RequiresAuthentication("hasRole('kraken-admin')")
    fun handle(command: Ping): Pong = Pong("admin:${command.text}", Instant.EPOCH)
}

class UserOnlyHandlers {
    @CommandHandler @RequiresAuthentication("hasRole('kraken-user')")
    fun handle(command: Ping): Pong = Pong("user:${command.text}", Instant.EPOCH)
}

/** Handles `test.Ping` for any authenticated caller and asks another service for `test.GetCount` while doing so. */
class NestingHandlers(private val queries: () -> QueryBus) {
    @CommandHandler @RequiresAuthentication
    suspend fun handle(command: Ping): Pong = Pong("count:${queries().send(GetCount(command.text)).value}", Instant.EPOCH)
}

class AdminCountHandlers {
    @QueryHandler @RequiresAuthentication("hasRole('kraken-admin')")
    fun handle(query: GetCount): Count = Count(42)
}

class OwnerCheckHandlers {
    @CommandHandler @RequiresAuthentication("hasRole('kraken-user') and #message.text == #ctx.userId")
    fun handle(command: Ping, ctx: MessageContext): Pong = Pong("owner:${ctx.userId}", Instant.EPOCH)
}

class HubBusTest {
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val registry = HandlerRegistry(resolver)
    private val invoker = HandlerInvoker(registry, codec)
    private val settings = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(200),
    )
    private val wait = Duration.ofSeconds(5)

    @AfterEach fun tearDown() = runBlocking {
        SecurityContextHolder.clearContext()
        MDC.clear()
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun connection(
        instanceId: String,
        hubName: String = hub.name,
        inbound: suspend (Request) -> Reply = { req -> error(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
    ) = HubConnection({ InProcessChannelBuilder.forName(hubName).build() }, "svc-$instanceId", instanceId, settings, inbound, scope)
        .also { connections += it }

    private fun bus(
        connection: HubConnection,
        forwardJwt: Boolean = true,
        requestTimeout: Duration? = Duration.ofSeconds(2),
        priorities: Map<String, Int> = emptyMap(),
        retry: RetryPolicy? = null,
    ) = HubBus(connection, registry, invoker, codec, resolver, forwardJwt, TraceHeaders(null, null), requestTimeout, priorities, retry)

    /** A connected caller plus, when [remoteTypes] is not empty, a remote service answering with [answer]. */
    private suspend fun connectedBus(
        commandTypes: Set<String> = emptySet(),
        queryTypes: Set<String> = emptySet(),
        forwardJwt: Boolean = true,
        answer: (Request) -> Reply = { req -> payload(req.id, "{\"echoed\":\"remote\",\"at\":\"1970-01-01T00:00:00Z\"}") },
    ): HubBus {
        if (commandTypes.isNotEmpty() || queryTypes.isNotEmpty()) {
            val remote = connection("remote") { answer(it) }
            remote.register(commandTypes, queryTypes)
            remote.start()
            assertTrue(remote.awaitRegistered(wait))
        }
        val caller = connection("caller")
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        return bus(caller, forwardJwt)
    }

    /** A caller bus (built by [build]) plus a remote owner of `test.Ping` and `test.GetCount` answering with [answer]. */
    private suspend fun remoteBus(answer: (Request) -> Reply, build: (HubConnection) -> HubBus): HubBus {
        val remote = connection("remote") { answer(it) }
        remote.register(setOf("test.Ping"), setOf("test.GetCount"))
        remote.start()
        assertTrue(remote.awaitRegistered(wait))
        val caller = connection("caller")
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        return build(caller)
    }

    private val pong = "{\"echoed\":\"remote\",\"at\":\"1970-01-01T00:00:00Z\"}"

    // --- SendOptions, priority (P1) ---

    @Test fun `priority comes from the per-type property, SendOptions wins`() = runBlocking {
        val bus = remoteBus({ payload(it.id, if (it.type == "test.GetCount") "{\"value\":1}" else pong) }) {
            bus(it, priorities = mapOf("test.Ping" to 7))
        }
        bus.send(Ping("x"))
        assertEquals(7, hub.lastRequest()!!.priority)
        bus.send(Ping("x"), SendOptions(priority = -3))
        assertEquals(-3, hub.lastRequest()!!.priority)
        bus.send(GetCount("k"))
        assertEquals(0, hub.lastRequest()!!.priority)
    }

    @Test fun `SendOptions carry timeout and routing key`() = runBlocking {
        val bus = remoteBus({ payload(it.id, pong) }) { bus(it, requestTimeout = null) }
        bus.send(Ping("x"), SendOptions(timeout = Duration.ofMillis(1_234), routingKey = "agg-1"))
        assertEquals(1_234, hub.lastRequest()!!.timeoutMs)
        assertEquals("agg-1", hub.lastRequest()!!.routingKey)
        bus.send(Ping("x"))
        assertEquals(0, hub.lastRequest()!!.timeoutMs) // nothing chosen: the hub decides
        assertEquals(Pong("remote", Instant.EPOCH), bus.sendBlocking(Ping("x"), SendOptions(routingKey = "k")))
    }

    @Test fun `RESOURCE_EXHAUSTED surfaces as HandlerBusyException`() = runBlocking {
        val bus = remoteBus({ error(it.id, ErrorCode.RESOURCE_EXHAUSTED, "queue full") }) { bus(it) }
        val e = assertFailsWith<HandlerBusyException> { bus.send(Ping("x")) }
        assertEquals("test.Ping", e.type)
    }

    // --- retry (P1) ---

    private val fastRetry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(10), maxBackoff = Duration.ofMillis(50))

    @Test fun `retry is off unless enabled`() = runBlocking {
        val attempts = CopyOnWriteArrayList<String>()
        val bus = remoteBus({ attempts += it.id; error(it.id, ErrorCode.RESOURCE_EXHAUSTED, "full") }) { bus(it) }
        assertFailsWith<HandlerBusyException> { bus.send(Ping("x")) }
        assertEquals(1, attempts.size)
    }

    @Test fun `a command retries HandlerBusy with a fresh request id until it succeeds`() = runBlocking {
        val attempts = CopyOnWriteArrayList<Request>()
        val bus = remoteBus({ req ->
            attempts += req
            if (attempts.size < 3) error(req.id, ErrorCode.RESOURCE_EXHAUSTED, "full") else payload(req.id, pong)
        }) { bus(it, retry = fastRetry) }
        assertEquals(Pong("remote", Instant.EPOCH), bus.send(Ping("x")))
        assertEquals(3, attempts.size)
        assertEquals(3, attempts.map { it.id }.toSet().size, "every attempt has its own id")
        assertEquals(1, attempts.map { it.headersMap["correlationId"] }.toSet().size, "one correlation id for all attempts")
    }

    @Test fun `a command is never retried after a timeout, a query is`() = runBlocking {
        val attempts = CopyOnWriteArrayList<Request>()
        val bus = remoteBus({ req ->
            attempts += req
            if (attempts.count { it.type == req.type } < 2) error(req.id, ErrorCode.TIMEOUT, "late") else payload(req.id, if (req.type == "test.GetCount") "{\"value\":3}" else pong)
        }) { bus(it, retry = fastRetry) }
        assertFailsWith<MessageTimeoutException> { bus.send(Ping("x")) }
        assertEquals(1, attempts.count { it.type == "test.Ping" })
        assertEquals(Count(3), bus.send(GetCount("k")))
        assertEquals(2, attempts.count { it.type == "test.GetCount" })
    }

    @Test fun `a non-retryable error is thrown at once`() = runBlocking {
        val attempts = CopyOnWriteArrayList<String>()
        val bus = remoteBus({ attempts += it.id; error(it.id, ErrorCode.HANDLER_ERROR, "boom") }) { bus(it, retry = fastRetry) }
        assertFailsWith<RemoteHandlerException> { bus.send(Ping("x")) }
        assertEquals(1, attempts.size)
    }

    @Test fun `after max attempts the last error is thrown`() = runBlocking {
        val attempts = CopyOnWriteArrayList<String>()
        val bus = remoteBus({ attempts += it.id; error(it.id, ErrorCode.NO_HANDLER, "gone") }) { bus(it, retry = fastRetry) }
        assertFailsWith<NoHandlerException> { bus.send(Ping("x")) }
        assertEquals(3, attempts.size)
    }

    @Test fun `all attempts stay within the caller's budget and send the remaining budget`() = runBlocking {
        val attempts = CopyOnWriteArrayList<Request>()
        val slow = RetryPolicy(maxAttempts = 10, initialBackoff = Duration.ofMillis(200), multiplier = 1.0, maxBackoff = Duration.ofMillis(200))
        val bus = remoteBus({ attempts += it; error(it.id, ErrorCode.RESOURCE_EXHAUSTED, "full") }) { bus(it, requestTimeout = null, retry = slow) }
        val started = System.nanoTime()
        assertFailsWith<HandlerBusyException> { bus.send(Ping("x"), SendOptions(timeout = Duration.ofMillis(700))) }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(elapsed < Duration.ofMillis(900), "took $elapsed for a 700 ms budget")
        assertTrue(attempts.size in 2..4, "attempts=${attempts.size}")
        val sent = attempts.map { it.timeoutMs }
        assertTrue(sent.first() in 600..700 && sent.zipWithNext().all { (a, b) -> b < a }, "timeout_ms per attempt: $sent")
    }

    @Test fun `no attempt is started on a near-zero remaining budget`() = runBlocking {
        val attempts = CopyOnWriteArrayList<Request>()
        val policy = RetryPolicy(maxAttempts = 100, initialBackoff = Duration.ofMillis(100), multiplier = 1.0)
        val bus = remoteBus({ attempts += it; error(it.id, ErrorCode.NO_HANDLER, "gone") }) { bus(it, requestTimeout = null, retry = policy) }
        val started = System.nanoTime()
        assertFailsWith<NoHandlerException> { bus.send(Ping("x"), SendOptions(timeout = Duration.ofSeconds(1))) }
        val elapsed = Duration.ofNanos(System.nanoTime() - started)
        assertTrue(elapsed < Duration.ofSeconds(1), "took $elapsed for a 1 s budget")
        val retried = attempts.drop(1).map { it.timeoutMs }
        // Every retry still gets at least max(initialBackoff, 250 ms) of the budget.
        assertTrue(retried.isNotEmpty() && retried.all { it >= 250 }, "timeout_ms of the retried attempts: $retried")
    }

    @Test fun `retry waits are jittered`() = runBlocking {
        val arrivals = CopyOnWriteArrayList<Long>()
        val policy = RetryPolicy(maxAttempts = 13, initialBackoff = Duration.ofMillis(100), multiplier = 1.0)
        val bus = remoteBus({ arrivals += System.nanoTime(); error(it.id, ErrorCode.RESOURCE_EXHAUSTED, "full") }) {
            bus(it, requestTimeout = null, retry = policy)
        }
        assertFailsWith<HandlerBusyException> { bus.send(Ping("x"), SendOptions(timeout = Duration.ofSeconds(5))) }
        assertEquals(13, arrivals.size)
        val gaps = arrivals.zipWithNext { a, b -> Duration.ofNanos(b - a).toMillis() }
        // 100 ms +-20 %: twelve waits spread over 80..120 ms, not twelve times the same 100 ms.
        assertTrue(gaps.max() - gaps.min() >= 10, "waits between attempts: $gaps")
    }

    @Test fun `without a chosen timeout the budget is the hub's bound for the type`() = runBlocking {
        hub.typeTimeouts = mapOf("test.Ping" to Duration.ofMillis(600))
        val attempts = CopyOnWriteArrayList<Request>()
        val slow = RetryPolicy(maxAttempts = 10, initialBackoff = Duration.ofMillis(150), multiplier = 1.0, maxBackoff = Duration.ofMillis(150))
        val bus = remoteBus({ attempts += it; error(it.id, ErrorCode.RESOURCE_EXHAUSTED, "full") }) { bus(it, requestTimeout = null, retry = slow) }
        assertFailsWith<HandlerBusyException> { bus.send(Ping("x")) }
        assertTrue(attempts.size in 2..4, "attempts=${attempts.size}")
        assertTrue(attempts.first().timeoutMs in 500..600, "first attempt sent ${attempts.first().timeoutMs}")
    }

    @Test fun `SendOptions retry NONE disables a configured policy`() = runBlocking {
        val attempts = CopyOnWriteArrayList<String>()
        val bus = remoteBus({ attempts += it.id; error(it.id, ErrorCode.RESOURCE_EXHAUSTED, "full") }) { bus(it, retry = fastRetry) }
        assertFailsWith<HandlerBusyException> { bus.send(Ping("x"), SendOptions(retry = RetryPolicy.NONE)) }
        assertEquals(1, attempts.size)
    }

    private fun authenticate(token: String) {
        SecurityContextHolder.getContext().authentication = JwtAuthenticationToken(TestJwt.decoder().decode(token))
    }

    @Test fun `local handler short circuits without hub`() = runBlocking {
        registry.register(LocalPingHandlers())
        val bus = bus(connection("lonely", hubName = "no-such-hub-" + UUID.randomUUID()))

        val pong = withTimeout(5_000) { bus.send(Ping("x")) }

        assertEquals(Pong("local:x", Instant.EPOCH), pong)
    }

    @Test fun `remote command carries bearer token and correlation id`() = runBlocking {
        val token = TestJwt.token("user-42", listOf("member"))
        authenticate(token)
        MDC.put("correlationId", "corr-1")
        val bus = connectedBus(commandTypes = setOf("test.Ping"))

        val pong = withTimeout(5_000) { bus.send(Ping("x")) }

        assertEquals(Pong("remote", Instant.EPOCH), pong)
        val headers = assertNotNull(hub.lastRequestHeaders())
        assertEquals("Bearer $token", headers["authorization"])
        assertEquals("corr-1", headers["correlationId"])
    }

    @Test fun `forward-jwt false omits authorization header`() = runBlocking {
        authenticate(TestJwt.token("user-42", listOf("member")))
        val bus = connectedBus(commandTypes = setOf("test.Ping"), forwardJwt = false)

        withTimeout(5_000) { bus.send(Ping("x")) }

        val headers = assertNotNull(hub.lastRequestHeaders())
        assertFalse("authorization" in headers)
        assertTrue(headers["correlationId"]!!.isNotBlank())
    }

    @Test fun `unset request-timeout sends timeout 0 to the hub and a set one is sent as is`() = runBlocking {
        val configured = connectedBus(commandTypes = setOf("test.Ping"))
        withTimeout(5_000) { configured.send(Ping("x")) }
        assertEquals(2_000, hub.lastRequest()!!.timeoutMs)

        val unset = bus(connections.last(), requestTimeout = null) // the connected caller, without a configured timeout
        withTimeout(5_000) { unset.send(Ping("y")) }
        assertEquals(0, hub.lastRequest()!!.timeoutMs)
    }

    @Test fun `NO_HANDLER maps to NoHandlerException`() = runBlocking {
        val bus = connectedBus()

        val e = assertFailsWith<NoHandlerException> { withTimeout(5_000) { bus.send(Ping("x")) } }

        assertEquals("test.Ping", e.type)
    }

    @Test fun `HANDLER_ERROR reply maps to RemoteHandlerException with details`() = runBlocking {
        val bus = connectedBus(commandTypes = setOf("test.Ping")) { req ->
            error(req.id, ErrorCode.HANDLER_ERROR, "bad input", "[\"text: must not be blank\"]")
        }

        val e = assertFailsWith<RemoteHandlerException> { withTimeout(5_000) { bus.send(Ping("x")) } }

        assertEquals("test.Ping", e.type)
        assertEquals("bad input", e.message)
        assertContentEquals("[\"text: must not be blank\"]".toByteArray(), e.details)
    }

    @Test fun `wrong shaped reply is InvalidMessageException`() = runBlocking {
        val bus = connectedBus(queryTypes = setOf("test.GetCount")) { req -> payload(req.id, "{\"nope\":1}") }

        val e = assertFailsWith<InvalidMessageException> { withTimeout(5_000) { bus.send(GetCount("k")) } }

        assertEquals("Reply to test.GetCount did not match ${Count::class.qualifiedName}", e.message)
    }

    @Test fun `sendBlocking works from a plain thread`() {
        val bus = runBlocking { connectedBus(commandTypes = setOf("test.Ping")) }
        val result = CompletableFuture<Pong>()

        thread(name = "plain-caller") {
            try {
                result.complete(bus.sendBlocking(Ping("x")))
            } catch (e: Throwable) {
                result.completeExceptionally(e)
            }
        }

        assertEquals(Pong("remote", Instant.EPOCH), result.get(5, TimeUnit.SECONDS))
    }

    private fun payload(id: String, json: String): Reply =
        Reply.newBuilder().setRequestId(id).setPayload(ByteString.copyFromUtf8(json)).build()

    private fun error(id: String, code: ErrorCode, message: String, details: String = ""): Reply =
        Reply.newBuilder().setRequestId(id)
            .setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message).setDetails(ByteString.copyFromUtf8(details)))
            .build()

    /** A caller authenticated the way a resource server does it: with authorities, hence authenticated. */
    private fun authenticateWithRoles(token: String) {
        val jwt = TestJwt.decoder().decode(token)
        val roles = (jwt.claims["realm_access"] as Map<*, *>)["roles"] as List<*>
        SecurityContextHolder.getContext().authentication =
            JwtAuthenticationToken(jwt, roles.map { SimpleGrantedAuthority("ROLE_$it") })
    }

    /** A remote service instance: its own registry and dispatcher behind a hub connection registered for its types. */
    private suspend fun remoteService(instanceId: String, decoder: JwtDecoder?, vararg beans: Any): Pair<HubConnection, HubBus> {
        val remoteRegistry = HandlerRegistry(resolver)
        beans.forEach { remoteRegistry.register(it) }
        val remoteInvoker = HandlerInvoker(remoteRegistry, codec)
        val dispatcher = InboundRequestDispatcher(remoteInvoker, ForwardedIdentityResolver(decoder), codec)
        val remote = connection(instanceId) { dispatcher.dispatch(it) }
        remote.register(remoteRegistry.commandTypes(), remoteRegistry.queryTypes())
        remote.start()
        assertTrue(remote.awaitRegistered(wait))
        val remoteBus = HubBus(remote, remoteRegistry, remoteInvoker, codec, resolver, true, TraceHeaders(null, null), Duration.ofSeconds(2))
        return remote to remoteBus
    }

    private suspend fun callerBus(): HubBus {
        val caller = connection("caller")
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        return bus(caller)
    }

    @Test fun `remote annotated handler rejects sender without token as UnauthenticatedException`() = runBlocking {
        remoteService("remote", TestJwt.decoder(), AdminOnlyHandlers())
        val bus = callerBus()

        val e = assertFailsWith<UnauthenticatedException> { withTimeout(5_000) { bus.send(Ping("x")) } }

        assertEquals("handler AdminOnlyHandlers.handle requires authentication", e.message)
    }

    @Test fun `remote annotated handler denies wrong role as AccessDeniedException`() = runBlocking {
        remoteService("remote", TestJwt.decoder(), AdminOnlyHandlers())
        authenticateWithRoles(TestJwt.token("user-1", listOf("kraken-user")))
        val bus = callerBus()

        val e = assertFailsWith<AccessDeniedException> { withTimeout(5_000) { bus.send(Ping("x")) } }

        assertEquals("handler AdminOnlyHandlers.handle: access denied", e.message)
    }

    @Test fun `remote annotated handler accepts matching role`() = runBlocking {
        remoteService("remote", TestJwt.decoder(), AdminOnlyHandlers())
        authenticateWithRoles(TestJwt.token("admin-1", listOf("kraken-admin")))
        val bus = callerBus()

        assertEquals(Pong("admin:x", Instant.EPOCH), withTimeout(5_000) { bus.send(Ping("x")) })
    }

    @Test fun `nested denied send inside annotated handler propagates AccessDeniedException`() = runBlocking {
        remoteService("counter", TestJwt.decoder(), AdminCountHandlers())
        lateinit var middleBus: HubBus
        middleBus = remoteService("middle", TestJwt.decoder(), NestingHandlers { middleBus }).second
        authenticateWithRoles(TestJwt.token("user-1", listOf("kraken-user")))
        val bus = callerBus()

        val e = assertFailsWith<AccessDeniedException> { withTimeout(5_000) { bus.send(Ping("x")) } }

        assertEquals("handler AdminCountHandlers.handle: access denied", e.message)
    }

    @Test fun `local short-circuit enforces the same rules`() = runBlocking {
        registry.register(AdminOnlyHandlers())
        val bus = bus(connection("lonely", hubName = "no-such-hub-" + UUID.randomUUID()))

        assertFailsWith<UnauthenticatedException> { withTimeout(5_000) { bus.send(Ping("x")) } }
        authenticateWithRoles(TestJwt.token("user-1", listOf("kraken-user")))
        assertFailsWith<AccessDeniedException> { withTimeout(5_000) { bus.send(Ping("x")) } }
        authenticateWithRoles(TestJwt.token("admin-1", listOf("kraken-admin")))
        assertEquals(Pong("admin:x", Instant.EPOCH), withTimeout(5_000) { bus.send(Ping("x")) })
    }

    @Test fun `receiver without JwtDecoder accepts forwarded token unverified`() = runBlocking {
        remoteService("remote", null, UserOnlyHandlers())
        authenticateWithRoles(TestJwt.token("user-1", listOf("kraken-user")))
        val bus = callerBus()

        assertEquals(Pong("user:x", Instant.EPOCH), withTimeout(5_000) { bus.send(Ping("x")) })
    }

    @Test fun `local short-circuit uses realm roles and identity for ctx regardless of the app authorities`() = runBlocking {
        registry.register(OwnerCheckHandlers())
        val bus = bus(connection("lonely", hubName = "no-such-hub-" + UUID.randomUUID()))
        val jwt = TestJwt.decoder().decode(TestJwt.token("u-7", listOf("kraken-user")))
        SecurityContextHolder.getContext().authentication = JwtAuthenticationToken(jwt, listOf(SimpleGrantedAuthority("SCOPE_profile")))

        assertEquals(Pong("owner:u-7", Instant.EPOCH), withTimeout(5_000) { bus.send(Ping("u-7")) })
        assertFailsWith<AccessDeniedException> { withTimeout(5_000) { bus.send(Ping("someone-else")) } }
        Unit
    }
}
