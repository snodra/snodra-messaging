package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.ClientMessage
import io.damoreira.kraken.hub.v1.Error
import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.hub.v1.HubGrpcKt
import io.damoreira.kraken.hub.v1.Register
import io.damoreira.kraken.hub.v1.Reply
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.hub.v1.RequestMode
import io.damoreira.kraken.hub.v1.ServerMessage
import io.damoreira.kraken.messaging.QueryUpdateEmitter
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.complete
import io.damoreira.kraken.messaging.completeExceptionally
import io.damoreira.kraken.messaging.emit
import io.damoreira.kraken.messaging.fixtures.Balance
import io.damoreira.kraken.messaging.fixtures.BalanceChanged
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.WatchAccount
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.test.InProcessHub
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.consumeAsFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.AbstractPlatformTransactionManager
import org.springframework.transaction.support.DefaultTransactionStatus
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import io.damoreira.kraken.hub.v1.ReplyEnd
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.ScatterOptions
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.UnsupportedByHubException
import io.damoreira.kraken.messaging.fixtures.ExportRows
import io.damoreira.kraken.messaging.fixtures.Row
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.withContext
import org.springframework.security.core.context.SecurityContextHolder
import org.springframework.security.oauth2.server.resource.authentication.JwtAuthenticationToken
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertIs
import kotlin.test.assertNull
import kotlin.test.assertTrue

/** Answers the initial result of `test.WatchAccount`: the balance is `100 + account`. */
class AccountWatcher {
    @QueryHandler fun handle(query: WatchAccount): Balance = Balance(100 + query.account)
}

class SharedAccountWatcher(private val base: Int = 100) {
    @QueryHandler(shared = true) fun handle(query: WatchAccount): Balance = Balance(base + query.account)
}

/** A plain (unary) query handler, for the mode mismatch. */
class PlainCountOwner {
    @QueryHandler fun handle(query: GetCount): Count = Count(1)
}

/** Never answers the initial request. */
class HangingWatcher {
    @QueryHandler(shared = true) suspend fun handle(query: WatchAccount): Balance = awaitCancellation()
}

/** A streaming handler, so a sender can prove streams keep working next to subscriptions. */
class RowsStreamerForSubscriptions {
    @QueryHandler fun handle(query: ExportRows): Flow<Row> = flow { repeat(query.count) { emit(Row(it + 1)) } }
}

/** A transaction manager that does nothing but run the synchronizations, as a test double for `@Transactional`. */
class NoopTransactionManager : AbstractPlatformTransactionManager() {
    override fun doGetTransaction(): Any = Any()
    override fun doBegin(transaction: Any, definition: TransactionDefinition) = Unit
    override fun doCommit(status: DefaultTransactionStatus) = Unit
    override fun doRollback(status: DefaultTransactionStatus) = Unit
}

/** Subscription queries (P4c, ADR 0017) over in-process hubs: the handler side (registration, emitter), then the sender side. */
class SubscriptionQueryTest {
    private val hubs = mutableListOf<InProcessHub>()
    private val hub = hub()
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val connections = mutableListOf<HubConnection>()
    private val multi = mutableListOf<HubConnections>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val registry = HandlerRegistry(resolver)
    private val invoker = HandlerInvoker(registry, codec)
    private val settings = HubSettings(
        requestTimeout = Duration.ofSeconds(2),
        heartbeatInterval = Duration.ofSeconds(10),
        backoffMin = Duration.ofMillis(20),
        backoffMax = Duration.ofMillis(200),
    )
    private val wait = Duration.ofSeconds(5)

    @AfterEach fun tearDown() = runBlocking {
        SecurityContextHolder.clearContext()
        connections.forEach { it.stop() }
        multi.forEach { it.stop() }
        scope.cancel()
        hubs.forEach { it.close() }
    }

    private fun hub(configure: InProcessHub.() -> Unit = {}) = InProcessHub().apply(configure).start().also { hubs += it }

    /** An instance of [service] serving the handlers of [beans] through the real registry and dispatcher, with its own emitter. */
    private inner class Owner(
        val connection: HubConnection,
        val subscriptions: SubscriptionRegistry,
        val emitter: QueryUpdateEmitter,
    ) {
        val instanceId: String get() = connection.instanceId
    }

    private suspend fun owner(
        service: String,
        instanceId: String,
        vararg beans: Any,
        hubName: String = hub.name,
        maxConcurrentHandlers: Int = 256,
        maxSubscriptions: Int = 1024,
    ): Owner {
        val ownerRegistry = HandlerRegistry(resolver)
        beans.forEach { ownerRegistry.register(it) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(ownerRegistry, codec), ForwardedIdentityResolver(null), codec)
        val subscriptions = SubscriptionRegistry(maxSubscriptions)
        val conn = HubConnection(
            channelFactory = { InProcessChannelBuilder.forName(hubName).build() },
            serviceName = service,
            instanceId = instanceId,
            settings = settings,
            inbound = dispatcher::dispatch,
            scope = scope,
            maxConcurrentHandlers = maxConcurrentHandlers,
            inboundStream = dispatcher::dispatchStream,
            subscriptions = subscriptions,
            inboundSubscription = dispatcher::dispatchSubscription,
        ).also { connections += it }
        conn.register(emptySet(), ownerRegistry.exclusiveQueryTypes(), ownerRegistry.sharedQueryTypes())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        return Owner(conn, subscriptions, HubQueryUpdateEmitter(subscriptions, codec, resolver))
    }

    private suspend fun awaitUntil(ms: Long = 5_000, condition: () -> Boolean) = withTimeout(ms) { while (!condition()) delay(10) }

    // --- the handler side on the wire ---

    @Test fun `register declares subscription and the connection reports the capability`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        assertEquals(listOf("flow-control", "instance-status", "scatter-gather", "streaming", "subscription"), hub.lastRegisterFrom("x-1")!!.capabilitiesList)
        assertTrue(x.connection.supportsSubscription)
        hub.subscription = false
        val old = owner("svc-y", "y-1", PlainCountOwner())
        assertFalse(old.connection.supportsSubscription)
    }

    @Test fun `a SUBSCRIPTION request registers a subscription, the handler answers the initial request and emits follow in call order`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1")
        sender.next() // ack
        sender.send(subscribeRequest("w1", account = 7))
        val initial = sender.next().part
        assertEquals(listOf("w1", 1L, "x-1"), listOf(initial.requestId, initial.seq, initial.responderInstance))
        assertEquals(Balance(107), codec.decode(initial.payload.toByteArray(), Balance::class))
        awaitUntil { x.subscriptions.size == 1 }
        assertEquals(1, hub.subscriptionTargetsOf("x-1"))

        repeat(3) { n -> x.emitter.emit(WatchAccount::class, { it.account == 7 }, BalanceChanged(7, n)) }
        val parts = (1..3).map { sender.next().part }
        assertEquals(listOf(2L, 3L, 4L), parts.map { it.seq }) // the hub renumbers: the initial part was 1
        assertEquals(listOf(0, 1, 2), parts.map { codec.decode(it.payload.toByteArray(), BalanceChanged::class).value })
        assertEquals("x-1", parts.first().responderInstance)
        // The subscription holds neither a permit nor a handler slot.
        awaitUntil { x.connection.inFlightHandlers.value == 0 }
        awaitUntil { hub.permitsAvailable("x-1") == 256 }
    }

    @Test fun `the emitter filter picks the subscriptions, one emit reaches every subscriber, no match is a no-op`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val s7 = RawSender("s-7").also { it.next() }
        val s8 = RawSender("s-8").also { it.next() }
        s7.send(subscribeRequest("w7", account = 7)); s7.next()
        s8.send(subscribeRequest("w8", account = 8)); s8.next()
        val s7b = RawSender("s-7b").also { it.next() }
        s7b.send(subscribeRequest("w7b", account = 7)); s7b.next()
        awaitUntil { x.subscriptions.size == 3 }

        x.emitter.emit<WatchAccount, BalanceChanged>({ it.account == 7 }, BalanceChanged(7, 1)) // the reified overload
        assertEquals(1, codec.decode(s7.next().part.payload.toByteArray(), BalanceChanged::class).value)
        assertEquals(1, codec.decode(s7b.next().part.payload.toByteArray(), BalanceChanged::class).value)
        s8.nothingWithin(200)
        x.emitter.emit(WatchAccount::class, { it.account == 99 }, BalanceChanged(99, 1)) // nobody matches
        x.emitter.emit(WatchAccount::class, { false }, BalanceChanged(0, 0))
        s7.nothingWithin(100); s8.nothingWithin(50); s7b.nothingWithin(50)
        assertEquals(3, x.subscriptions.size)
    }

    @Test fun `complete ends the matching subscriptions cleanly and completeExceptionally with HANDLER_ERROR, both removing them`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val done = RawSender("s-1").also { it.next() }
        val failed = RawSender("s-2").also { it.next() }
        val kept = RawSender("s-3").also { it.next() }
        done.send(subscribeRequest("w1", account = 1)); done.next()
        failed.send(subscribeRequest("w2", account = 2)); failed.next()
        kept.send(subscribeRequest("w3", account = 3)); kept.next()
        awaitUntil { x.subscriptions.size == 3 }

        x.emitter.emit(WatchAccount::class, { it.account == 1 }, BalanceChanged(1, 1))
        x.emitter.complete<WatchAccount> { it.account == 1 }
        assertEquals(2L, done.next().part.seq)
        val end = done.next().end
        assertEquals(listOf("w1", 2), listOf(end.requestId, end.parts)) // the hub's count: the initial part and one update
        assertEquals(false, end.hasError())
        x.emitter.completeExceptionally(WatchAccount::class, { it.account == 2 }, IllegalStateException("ledger closed"))
        val failure = failed.next().end
        assertEquals(ErrorCode.HANDLER_ERROR to "IllegalStateException: ledger closed", failure.error.code to failure.error.message)
        awaitUntil { x.subscriptions.size == 1 }
        assertEquals(1, hub.openSubscriptions()) // only w3 is left on the hub
        kept.nothingWithin(100)
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(3, 1))
        assertEquals(2L, kept.next().part.seq)
    }

    @Test fun `inside a transaction emits and completes wait for the commit and a rollback drops them`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7)); sender.next()
        awaitUntil { x.subscriptions.size == 1 }
        val tx = TransactionTemplate(NoopTransactionManager())

        tx.execute {
            assertTrue(TransactionSynchronizationManager.isActualTransactionActive())
            x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
            x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 2))
            runBlocking { sender.nothingWithin(300) } // not before the commit
        }
        // After the commit, in call order.
        assertEquals(listOf(1, 2), (1..2).map { codec.decode(sender.next().part.payload.toByteArray(), BalanceChanged::class).value })

        // A rollback drops them, the completion included.
        runCatching {
            tx.execute {
                x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 3))
                x.emitter.complete(WatchAccount::class) { true }
                throw IllegalStateException("rollback")
            }
        }
        sender.nothingWithin(300)
        assertEquals(1, x.subscriptions.size)

        // setRollbackOnly drops them too; a committed completion ends the subscription after its updates.
        tx.execute { status ->
            x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 4))
            status.setRollbackOnly()
        }
        sender.nothingWithin(200)
        tx.execute {
            x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 5))
            x.emitter.complete(WatchAccount::class) { true }
        }
        assertEquals(5, codec.decode(sender.next().part.payload.toByteArray(), BalanceChanged::class).value)
        assertEquals(false, sender.next().end.hasError())
        assertEquals(0, x.subscriptions.size)
        // Outside a transaction they are immediate.
        assertFalse(TransactionSynchronizationManager.isActualTransactionActive())
    }

    @Test fun `a hub Cancel removes the subscription, nothing is sent for it afterwards`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7)); sender.next()
        awaitUntil { x.subscriptions.size == 1 }
        sender.send(ClientMessage.newBuilder().setCancel(io.damoreira.kraken.hub.v1.Cancel.newBuilder().setRequestId("w1").setReason("user left")).build())
        awaitUntil { x.subscriptions.size == 0 }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        sender.nothingWithin(200)
        assertEquals(0, hub.openSubscriptions())
    }

    @Test fun `a lost hub stream removes every subscription it carried`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val a = RawSender("s-1").also { it.next() }
        val b = RawSender("s-2").also { it.next() }
        a.send(subscribeRequest("w1", account = 1)); a.next()
        b.send(subscribeRequest("w2", account = 2)); b.next()
        awaitUntil { x.subscriptions.size == 2 }
        hub.dropInstance("x-1")
        awaitUntil { x.subscriptions.size == 0 }
        assertEquals(ErrorCode.HANDLER_DISCONNECTED, a.next().end.error.code) // the hub's: the last target left
        assertEquals(ErrorCode.HANDLER_DISCONNECTED, b.next().end.error.code)
        awaitUntil { x.connection.connected.value } // it reconnects, but the subscriptions are not restored
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(1, 1))
        a.nothingWithin(200)
        assertEquals(0, x.subscriptions.size)
    }

    @Test fun `the subscription limit refuses the next one with RESOURCE_EXHAUSTED and registers nothing`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher(), maxSubscriptions = 1)
        val first = RawSender("s-1").also { it.next() }
        first.send(subscribeRequest("w1", account = 1)); first.next()
        awaitUntil { x.subscriptions.size == 1 }
        val second = RawSender("s-2").also { it.next() }
        second.send(subscribeRequest("w2", account = 2))
        // The target's refusal ends the subscription (the hub relays it); the initial part may precede it.
        val end = second.nextEnd()
        assertEquals(ErrorCode.RESOURCE_EXHAUSTED to "subscription limit (1) reached", end.error.code to end.error.message)
        assertEquals(1, x.subscriptions.size)
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(1, 1))
        assertEquals(2L, first.next().part.seq)
        second.nothingWithin(100)
    }

    @Test fun `a unary request for a subscription type is INVALID_REQUEST, a SUBSCRIPTION request for a plain query type ends INVALID_REQUEST`() = runBlocking {
        owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        sender.send(
            ClientMessage.newBuilder().setRequest(
                Request.newBuilder().setId("u1").setKind(RequestKind.QUERY).setType("test.WatchAccount").setTimeoutMs(5_000)
                    .setPayload(ByteString.copyFromUtf8("{\"account\":7}")),
            ).build(),
        )
        val unary = sender.next().reply.error
        assertEquals(ErrorCode.INVALID_REQUEST to "test.WatchAccount is a subscription query: send it with QueryBus.subscribe", unary.code to unary.message)

        owner("svc-y", "y-1", PlainCountOwner())
        sender.send(subscribeRequest("w2", account = 1, type = "test.GetCount"))
        val end = sender.nextEnd()
        assertEquals(ErrorCode.INVALID_REQUEST, end.error.code)
        assertTrue(end.error.message.contains("no subscription handler") || end.error.message.contains("not a subscription query handler"), end.error.message)
    }

    @Test fun `a subscription with a payload that does not decode ends INVALID_REQUEST`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7, payload = "[1]"))
        assertEquals(ErrorCode.INVALID_REQUEST, sender.nextEnd().error.code)
        assertEquals(0, x.subscriptions.size)
    }

    @Test fun `shutting down ends every subscription with a clean ReplyEnd`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7)); sender.next()
        awaitUntil { x.subscriptions.size == 1 }
        x.subscriptions.endAll()
        val end = sender.next().end
        assertEquals(listOf("w1", 1, false), listOf(end.requestId, end.parts, end.hasError()))
        assertEquals(0, x.subscriptions.size)
    }

    @Test fun `a Cancel that overtakes the registration leaves no subscription behind`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        repeat(20) { n ->
            sender.send(subscribeRequest("race-$n", account = n))
            sender.send(ClientMessage.newBuilder().setCancel(io.damoreira.kraken.hub.v1.Cancel.newBuilder().setRequestId("race-$n").setReason("gone")).build())
        }
        awaitUntil { hub.openSubscriptions() == 0 }
        delay(300)
        assertEquals(0, x.subscriptions.size)
    }

    // --- helpers ---

    /** A raw sender: the generated stub, so the test sees exactly what the hub relays. */
    private inner class RawSender(instanceId: String) {
        val out = Channel<ClientMessage>(Channel.UNLIMITED)
        val inbox = Channel<ServerMessage>(Channel.UNLIMITED)

        init {
            scope.launch {
                runCatching { HubGrpcKt.HubCoroutineStub(hub.channel()).connect(out.consumeAsFlow()).collect { inbox.send(it) } }
            }
            send(
                ClientMessage.newBuilder().setRegister(
                    Register.newBuilder().setServiceName("svc-raw").setInstanceId(instanceId).setProtocolVersion(2)
                        .addCapabilities("subscription").addCapabilities("streaming"),
                ).build(),
            )
        }

        fun send(message: ClientMessage) = out.trySend(message).getOrThrow()

        suspend fun next(): ServerMessage = withTimeout(5_000) { inbox.receive() }

        /** The next `ReplyEnd`, skipping the parts before it. */
        suspend fun nextEnd(): io.damoreira.kraken.hub.v1.ReplyEnd {
            while (true) {
                val message = next()
                if (message.hasEnd()) return message.end
            }
        }

        suspend fun nothingWithin(ms: Long) = assertNull(withTimeoutOrNull(ms) { inbox.receive() })
    }

    private fun subscribeRequest(id: String, account: Int, type: String = "test.WatchAccount", payload: String? = null, initialCredit: Int = 0, maxResults: Int = 0) =
        ClientMessage.newBuilder().setRequest(
            Request.newBuilder().setId(id).setKind(RequestKind.QUERY).setType(type).setMode(RequestMode.SUBSCRIPTION)
                .setInitialCredit(initialCredit).setMaxResults(maxResults).setTimeoutMs(5_000)
                .setPayload(ByteString.copyFromUtf8(payload ?: "{\"account\":$account}")),
        ).build()

    private fun json(text: String) = ByteString.copyFromUtf8(text)

    private fun count(id: String, value: Int): Reply = Reply.newBuilder().setRequestId(id).setPayload(json("{\"value\":$value}")).build()

    private fun error(id: String, code: ErrorCode, message: String): Reply = Reply.newBuilder().setRequestId(id)
        .setError(Error.newBuilder().setRequestId(id).setCode(code).setMessage(message)).build()
}
