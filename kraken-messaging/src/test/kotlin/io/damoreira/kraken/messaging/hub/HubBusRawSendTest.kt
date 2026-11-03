package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.RequiresAuthentication
import io.damoreira.kraken.messaging.saga.RawOutcome
import io.damoreira.kraken.messaging.saga.SagaCommandFailed.Reason
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.test.InProcessHub
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import io.damoreira.kraken.messaging.tracing.TraceHeaders
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
        heartbeatInterval = Duration.ofSeconds(10),
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

    private fun connection(instanceId: String, hubName: String = hub.name, inbound: suspend (io.damoreira.kraken.hub.v1.Request) -> io.damoreira.kraken.hub.v1.Reply) =
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

    @Test fun `a type nobody handles is NO_HANDLER`() = runBlocking {
        val callerBus = remoteOwner(ContextCapturingHandlers())

        val outcome = callerBus.sendRaw("test.Nobody", payload, SendOptions())

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
