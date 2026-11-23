package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.messaging.SendOptions
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Ping
import com.damoreira.snodra.messaging.fixtures.Pong
import com.damoreira.snodra.messaging.handler.CommandHandler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.RequiresAuthentication
import com.damoreira.snodra.messaging.saga.LocalRawCommandSender
import com.damoreira.snodra.messaging.saga.RawCommandSender
import com.damoreira.snodra.messaging.saga.RawOutcome
import com.damoreira.snodra.messaging.saga.SagaCommandFailed.Reason
import com.damoreira.snodra.messaging.security.ForwardedIdentityResolver
import com.damoreira.snodra.messaging.test.InProcessHub
import com.damoreira.snodra.messaging.tracing.CorrelationContext
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.jwt.Jwt
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertTrue

/** A `test.Ping` handler that needs an authenticated caller. */
class SecuredPingHandler {
    @RequiresAuthentication @CommandHandler fun handle(command: Ping): Pong = Pong("secret", Instant.EPOCH)
}

/** A `test.Ping` handler that fails. */
class FailingPingHandler {
    @CommandHandler fun handle(command: Ping): Pong = throw IllegalStateException("printer on fire")
}

/** [HubBus.sendRaw] (the saga relay's send path): raw bytes by wire type name, the outcome classified, no caller identity. */
class HubBusRawSendTest {
    private val hub = InProcessHub().start()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val settings = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(200),
    )
    private val wait = Duration.ofSeconds(5)
    private val headers = mapOf("idempotency-key" to "saga-1:1001", "saga-id" to "saga-1")
    private val payload = """{"text":"x"}""".toByteArray()

    @AfterEach fun tearDown() = runBlocking {
        SecurityContextHolder.clearContext()
        connections.forEach { it.stop() }
        scope.cancel()
        hub.close()
    }

    private fun connection(instanceId: String, hubName: String = hub.name, inbound: suspend (com.damoreira.snodra.server.v1.Request) -> com.damoreira.snodra.server.v1.Reply) =
        HubConnection({ InProcessChannelBuilder.forName(hubName).build() }, "svc-$instanceId", instanceId, settings, inbound, scope).also { connections += it }

    private fun bus(connection: HubConnection, registry: HandlerRegistry) =
        HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), Duration.ofSeconds(2))

    private fun signIn() {
        val jwt = Jwt.withTokenValue("the-user-token").header("alg", "none").subject("user-7").build()
        SecurityContextHolder.getContext().authentication = JwtAuthenticationToken(jwt)
    }

    private suspend fun remoteOwner(owner: Any): HubBus {
        val ownerRegistry = HandlerRegistry(resolver).also { it.register(owner) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(ownerRegistry, codec), ForwardedIdentityResolver(null), codec)
        val remote = connection("owner") { dispatcher.dispatch(it) }
        remote.register(ownerRegistry.commandTypes(), ownerRegistry.queryTypes())
        remote.start()
        assertTrue(remote.awaitRegistered(wait))
        val caller = connection("caller") { error("unexpected") }
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        return bus(caller, HandlerRegistry(resolver))
    }

    @Test fun `sendRaw hands the bytes, the headers and the correlation id to a remote handler and answers Success`() = runBlocking {
        val owner = ContextCapturingHandlers()
        val callerBus = remoteOwner(owner)

        val outcome = withContext(CorrelationContext("corr-9")) { callerBus.sendRaw("test.Ping", payload, SendOptions(headers = headers)) }

        assertEquals(RawOutcome.Success, outcome)
        val ctx = owner.contexts.single()
        assertEquals("saga-1:1001", ctx.headers["idempotency-key"])
        assertEquals("saga-1", ctx.headers["saga-id"])
        assertEquals("corr-9", ctx.correlationId)
        assertEquals("x", String(hub.lastRequest()!!.payload.toByteArray()).substringAfter("\"text\":\"").substringBefore("\""))
    }

    @Test fun `sendRaw forwards no caller token even when the calling thread has a user`() = runBlocking {
        val callerBus = remoteOwner(ContextCapturingHandlers())
        signIn()

        assertEquals(RawOutcome.Success, callerBus.sendRaw("test.Ping", payload, SendOptions(headers = headers)))

        assertFalse("authorization" in hub.lastRequest()!!.headersMap, "a saga command has no user: ${hub.lastRequest()!!.headersMap}")
    }

    @Test fun `a local handler without @RequiresAuthentication runs and one that requires it denies, whoever is signed in`() = runBlocking {
        val open = HandlerRegistry(resolver).also { it.register(ContextCapturingHandlers()) }
        val secured = HandlerRegistry(resolver).also { it.register(SecuredPingHandler()) }
        val lonely = "no-such-hub-" + UUID.randomUUID()
        signIn()

        assertEquals(RawOutcome.Success, bus(connection("a", lonely) { error("unexpected") }, open).sendRaw("test.Ping", payload, SendOptions(headers = headers)))
        val denied = bus(connection("b", lonely) { error("unexpected") }, secured).sendRaw("test.Ping", payload, SendOptions(headers = headers))

        assertEquals(Reason.DENIED, assertIs<RawOutcome.Permanent>(denied).reason)
    }

    @Test fun `the relay waits for a call through the hub until the hub gave up on it, a local call only its send timeout`() = runBlocking {
        hub.maxTimeout = Duration.ofSeconds(20)
        hub.typeTimeouts = mapOf("test.Slow" to Duration.ofSeconds(90))
        val local = HandlerRegistry(resolver).also { it.register(ContextCapturingHandlers()) }
        val caller = connection("caller") { error("unexpected") } // waits up to its request-timeout, 2 s, for a stream
        caller.start()
        assertTrue(caller.awaitRegistered(wait))
        val unset = HubBus(caller, local, HandlerInvoker(local, codec), codec, resolver, true, TraceHeaders(null, null), null)
        val chosen = HubBus(caller, local, HandlerInvoker(local, codec), codec, resolver, true, TraceHeaders(null, null), Duration.ofSeconds(2))
        val sendTimeout = Duration.ofMillis(300)
        // The wait for a stream (2 s) and the client's own guard (1 s past the hub's deadline), then the relay's margin.
        val extra = Duration.ofSeconds(2).plus(HubProtocol.LOCAL_GUARD_MARGIN).plus(RawCommandSender.HUB_CALL_GUARD)

        assertEquals(sendTimeout, unset.sendTimeoutFor("test.Ping", sendTimeout), "a local handler: the send timeout alone")
        assertEquals(Duration.ofSeconds(20).plus(extra), unset.sendTimeoutFor("test.Remote", sendTimeout), "the hub's clamp for timeout_ms 0")
        assertEquals(Duration.ofSeconds(90).plus(extra), unset.sendTimeoutFor("test.Slow", sendTimeout), "the type's override")
        assertEquals(Duration.ofSeconds(2).plus(extra), chosen.sendTimeoutFor("test.Slow", sendTimeout), "the request timeout sent as timeout_ms")
        assertEquals(Duration.ofMinutes(5), unset.sendTimeoutFor("test.Slow", Duration.ofMinutes(5)), "never shorter than the send timeout")
    }

    @Test fun `over several hubs the relay waits for a call moved to another hub just before its timeout, with the full timeout again`() = runBlocking {
        // request-timeout 60 s: the call may wait for a hub (and move) for up to 60 s, and the hop it starts then carries the full
        // 60 s again; the relay must not abandon it before that hop's guard (60 s + 1 s) has passed too.
        val sixty = Duration.ofSeconds(60)
        val connections = HubConnections(
            targets = HubTargets.Static(listOf(hub.name)),
            channelFactory = HubChannelFactory { InProcessChannelBuilder.forName(it).build() },
            serviceName = "svc-caller",
            instanceId = "caller-n",
            settings = settings.copy(requestTimeout = sixty),
            inbound = { error("unexpected") },
            scope = scope,
        )
        connections.start()
        try {
            assertTrue(connections.awaitRegistered(wait))
            val local = HandlerRegistry(resolver)
            val relayBus = HubBus(connections, local, HandlerInvoker(local, codec), codec, resolver, true, TraceHeaders(null, null), sixty)

            val bound = relayBus.sendTimeoutFor("test.Remote", Duration.ofMillis(300))

            val longestCall = sixty.plus(sixty).plus(HubProtocol.LOCAL_GUARD_MARGIN) // the last hop starts at 60 s and is guarded to 61 s
            assertTrue(bound >= longestCall.plus(RawCommandSender.HUB_CALL_GUARD), "the relay abandons the call after $bound")
            assertEquals(longestCall, connections.callBound("test.Remote", 60_000))
        } finally {
            connections.stop()
        }
    }

    @Test fun `a type no service handles is transient through the hub (a rolling redeploy of the target)`() = runBlocking {
        val callerBus = remoteOwner(ContextCapturingHandlers())

        val outcome = callerBus.sendRaw("test.Nobody", payload, SendOptions())

        val transient = assertIs<RawOutcome.Transient>(outcome, "the relay retries it with backoff until EXHAUSTED")
        assertTrue("test.Nobody" in transient.message, transient.message)
    }

    @Test fun `a type this service does not handle is NO_HANDLER through the local sender (no hub to wait for)`() = runBlocking {
        val registry = HandlerRegistry(resolver).also { it.register(ContextCapturingHandlers()) }

        val outcome = LocalRawCommandSender(HandlerInvoker(registry, codec)).sendRaw("test.Nobody", payload, SendOptions())

        assertEquals(Reason.NO_HANDLER, assertIs<RawOutcome.Permanent>(outcome).reason)
    }

    @Test fun `a handler that fails is FAILED, remote and local`() = runBlocking {
        val remote = remoteOwner(FailingPingHandler()).sendRaw("test.Ping", payload, SendOptions())
        val localRegistry = HandlerRegistry(resolver).also { it.register(FailingPingHandler()) }
        val local = bus(connection("c", "no-such-hub-" + UUID.randomUUID()) { error("unexpected") }, localRegistry).sendRaw("test.Ping", payload, SendOptions())

        for (outcome in listOf(remote, local)) {
            val failed = assertIs<RawOutcome.Permanent>(outcome)
            assertEquals(Reason.FAILED, failed.reason)
            assertTrue("printer on fire" in failed.message, failed.message)
        }
    }

    @Test fun `a payload the handler cannot read is INVALID`() = runBlocking {
        val callerBus = remoteOwner(ContextCapturingHandlers())

        val outcome = callerBus.sendRaw("test.Ping", """{"nope":1}""".toByteArray(), SendOptions())

        assertEquals(Reason.INVALID, assertIs<RawOutcome.Permanent>(outcome).reason)
    }
}
