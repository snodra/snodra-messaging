package com.damoreira.snodra.messaging.hub

import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.ClientMessage
import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.HubGrpcKt
import com.damoreira.snodra.server.v1.Register
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.server.v1.RequestMode
import com.damoreira.snodra.server.v1.ServerMessage
import com.damoreira.snodra.messaging.QueryUpdateEmitter
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.complete
import com.damoreira.snodra.messaging.completeExceptionally
import com.damoreira.snodra.messaging.emit
import com.damoreira.snodra.messaging.fixtures.Balance
import com.damoreira.snodra.messaging.fixtures.BalanceChanged
import com.damoreira.snodra.messaging.fixtures.Count
import com.damoreira.snodra.messaging.fixtures.GetCount
import com.damoreira.snodra.messaging.fixtures.WatchAccount
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.QueryHandler
import com.damoreira.snodra.messaging.handler.RequiresAuthentication
import com.damoreira.snodra.messaging.security.ForwardedIdentityResolver
import com.damoreira.snodra.messaging.test.InProcessHub
import io.grpc.inprocess.InProcessChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
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
import com.damoreira.snodra.server.v1.ReplyEnd
import com.damoreira.snodra.messaging.AccessDeniedException
import com.damoreira.snodra.messaging.HandlerBusyException
import com.damoreira.snodra.messaging.HandlerDisconnectedException
import com.damoreira.snodra.messaging.HubConnectionLostException
import com.damoreira.snodra.messaging.InvalidMessageException
import com.damoreira.snodra.messaging.MessageTimeoutException
import com.damoreira.snodra.messaging.NoHandlerException
import com.damoreira.snodra.messaging.RemoteHandlerException
import com.damoreira.snodra.messaging.RetryPolicy
import com.damoreira.snodra.messaging.ScatterOptions
import com.damoreira.snodra.messaging.StreamCancelledException
import com.damoreira.snodra.messaging.StreamOptions
import com.damoreira.snodra.messaging.SubscriptionOptions
import com.damoreira.snodra.messaging.UnauthenticatedException
import com.damoreira.snodra.messaging.fixtures.ExportRows
import com.damoreira.snodra.messaging.fixtures.Row
import com.damoreira.snodra.messaging.fixtures.TestJwt
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.first
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

/** A subscription query handler only an administrator may use. */
class ProtectedAccountWatcher {
    @QueryHandler @RequiresAuthentication("hasRole('snodra-admin')") fun handle(query: WatchAccount): Balance = Balance(1)
}

/** Never answers the initial request. */
class HangingWatcher {
    @QueryHandler(shared = true) suspend fun handle(query: WatchAccount): Balance = awaitCancellation()
}

/** A transaction manager that does nothing but run the synchronizations, as a test double for `@Transactional`. */
class NoopTransactionManager : AbstractPlatformTransactionManager() {
    override fun doGetTransaction(): Any = Any()
    override fun doBegin(transaction: Any, definition: TransactionDefinition) = Unit
    override fun doCommit(status: DefaultTransactionStatus) = Unit
    override fun doRollback(status: DefaultTransactionStatus) = Unit
}

/** Subscription queries over in-process hubs: the handler side (registration, emitter), then the sender side. */
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
        /** Runs before the real admission of a `SUBSCRIPTION` request (a slow identity check, in a test). */
        beforeAdmission: (suspend () -> Unit)? = null,
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
            inboundSubscription = { req, register ->
                beforeAdmission?.invoke()
                dispatcher.dispatchSubscription(req, register)
            },
        ).also { connections += it }
        conn.register(emptySet(), ownerRegistry.exclusiveQueryTypes(), ownerRegistry.sharedQueryTypes(), ownerRegistry.subscriptionQueryTypes())
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        return Owner(conn, subscriptions, HubQueryUpdateEmitter(subscriptions, codec, resolver))
    }

    private suspend fun awaitUntil(ms: Long = 5_000, condition: () -> Boolean) = withTimeout(ms) { while (!condition()) delay(10) }

    // --- the handler side on the wire ---

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
        sender.send(ClientMessage.newBuilder().setCancel(com.damoreira.snodra.server.v1.Cancel.newBuilder().setRequestId("w1").setReason("user left")).build())
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
        sender.send(subscribeRequest("w2", account = 1, type = "test.GetCount", payload = "{\"key\":\"x\"}"))
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

    @Test fun `stopping the connection delivers the updates and ends it queued, even to a hub that has not read them yet`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7)); sender.next()
        awaitUntil { x.subscriptions.size == 1 }
        // The hub stops reading: it holds the update, the end waits in the client until the hub reads again.
        hub.pauseReading = true
        x.emitter.emit(WatchAccount::class, { it.account == 7 }, BalanceChanged(7, 1))
        x.subscriptions.endAll()
        coroutineScope {
            launch { delay(200); hub.pauseReading = false }
            x.connection.stop()
        }
        val received = mutableListOf<ServerMessage>()
        do received += sender.next() while (!received.last().hasEnd())
        assertEquals(listOf(true, false), received.map { it.hasPart() })
        assertFalse(received.last().end.hasError(), "a clean end, not ${received.last().end.error.code}")
    }

    @Test fun `RequiresAuthentication is checked when the subscription is admitted`() = runBlocking {
        val x = owner("svc-x", "x-1", ProtectedAccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7)) // no forwarded token: anonymous
        assertEquals(ErrorCode.UNAUTHENTICATED, sender.nextEnd().error.code) // the target's refusal or the initial request's
        awaitUntil { hub.openSubscriptions() == 0 }
        assertEquals(0, x.subscriptions.size)
    }

    @Test fun `a Cancel that overtakes the registration leaves no subscription behind`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val sender = RawSender("s-1").also { it.next() }
        repeat(20) { n ->
            sender.send(subscribeRequest("race-$n", account = n))
            sender.send(ClientMessage.newBuilder().setCancel(com.damoreira.snodra.server.v1.Cancel.newBuilder().setRequestId("race-$n").setReason("gone")).build())
        }
        awaitUntil { hub.openSubscriptions() == 0 }
        delay(300)
        assertEquals(0, x.subscriptions.size)
    }


    // --- registered when the request is read: an emit during a slow admission is neither lost nor reordered ---

    @Test fun `an update emitted after the initial result was answered but before admission completes is delivered after it, in order`() = runBlocking {
        val admit = CompletableDeferred<Unit>()
        val x = owner("svc-x", "x-1", AccountWatcher(), beforeAdmission = { admit.await() })
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7)) // the hub writes w1#1 (admission, held) and then w1#0 (answered at once)
        val initial = sender.next().part
        assertEquals(1L, initial.seq)
        assertEquals(Balance(107), codec.decode(initial.payload.toByteArray(), Balance::class))

        repeat(3) { n -> x.emitter.emit(WatchAccount::class, { it.account == 7 }, BalanceChanged(7, n + 1)) } // admission still running
        x.emitter.emit(WatchAccount::class, { it.account == 99 }, BalanceChanged(99, 0)) // not this subscription's
        assertEquals(1, x.subscriptions.size) // registered when the request was read
        sender.nothingWithin(200) // held back until admitted
        admit.complete(Unit)
        x.emitter.emit(WatchAccount::class, { it.account == 7 }, BalanceChanged(7, 4)) // a later emit goes after the buffered ones
        val parts = (1..4).map { sender.next().part }
        assertEquals(listOf(2L, 3L, 4L, 5L), parts.map { it.seq })
        assertEquals(listOf(1, 2, 3, 4), parts.map { codec.decode(it.payload.toByteArray(), BalanceChanged::class).value })
        sender.nothingWithin(100)
    }

    @Test fun `a hub Cancel during admission removes the pending subscription and nothing is delivered after the admission ends`() = runBlocking {
        val admit = CompletableDeferred<Unit>()
        val x = owner("svc-x", "x-1", AccountWatcher(), beforeAdmission = { admit.await() })
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7))
        sender.next() // the initial part
        assertEquals(1, x.subscriptions.size)
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1)) // buffered
        sender.send(ClientMessage.newBuilder().setCancel(com.damoreira.snodra.server.v1.Cancel.newBuilder().setRequestId("w1").setReason("user left")).build())
        awaitUntil { x.subscriptions.size == 0 }
        admit.complete(Unit)
        delay(300)
        assertEquals(0, x.subscriptions.size)
        sender.nothingWithin(200)
        assertEquals(0, hub.openSubscriptions())
    }

    @Test fun `a lost hub stream during admission leaves nothing open when the admission ends`() = runBlocking {
        val admit = CompletableDeferred<Unit>()
        val x = owner("svc-x", "x-1", AccountWatcher(), beforeAdmission = { admit.await() })
        val sender = RawSender("s-1").also { it.next() }
        sender.send(subscribeRequest("w1", account = 7))
        sender.next()
        assertEquals(1, x.subscriptions.size)
        hub.dropInstance("x-1")
        awaitUntil { x.subscriptions.size == 0 }
        awaitUntil { x.connection.connected.value } // reconnected on a new stream
        admit.complete(Unit)
        delay(300) // the admission of the old stream's request ends now
        assertEquals(0, x.subscriptions.size)
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        assertEquals(0, x.subscriptions.size)
    }

    // --- the sender side ---

    private fun bus(transport: HubTransport, retry: RetryPolicy? = null, requestTimeout: Duration? = null) =
        HubBus(transport, registry, invoker, codec, resolver, true, TraceHeaders(null, null), requestTimeout, emptyMap(), retry)

    private suspend fun caller(hubName: String = hub.name, retry: RetryPolicy? = null): HubBus {
        val conn = HubConnection(
            channelFactory = { InProcessChannelBuilder.forName(hubName).build() },
            serviceName = "svc-caller",
            instanceId = "caller-" + UUID.randomUUID().toString().take(6),
            settings = settings,
            inbound = { req -> error(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
            scope = scope,
        ).also { connections += it }
        conn.start()
        assertTrue(conn.awaitRegistered(wait))
        return bus(conn, retry)
    }

    private fun callerId() = connections.first { it.instanceId.startsWith("caller-") }.instanceId

    private suspend fun multiCaller(vararg hubNames: String): HubConnections {
        val conns = HubConnections(
            targets = HubTargets.Static(hubNames.toList()),
            channelFactory = { target -> InProcessChannelBuilder.forName(target).build() },
            serviceName = "svc-caller",
            instanceId = "caller",
            settings = settings,
            inbound = { req -> error(req.id, ErrorCode.HANDLER_ERROR, "unexpected") },
            scope = scope,
        ).also { multi += it }
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        awaitUntil { conns.liveHubIds().size == hubNames.size }
        return conns
    }

    private fun changed(update: BalanceChanged) = update.value

    @Test fun `subscribe returns the initial result, then the updates of both owners in order, and completes when both complete`() = runBlocking {
        val x = owner("svc-x", "x-1", SharedAccountWatcher(base = 100))
        val y = owner("svc-y", "y-1", SharedAccountWatcher(base = 200))
        val result = withTimeout(5_000) { caller().subscribe(WatchAccount(7)) }
        assertTrue(result.initial == Balance(107) || result.initial == Balance(207), result.initial.toString()) // one of the owners answered
        awaitUntil { x.subscriptions.size == 1 && y.subscriptions.size == 1 } // every instance is a target
        val seen = CopyOnWriteArrayList<Int>()
        val collecting = async { result.updates.collect { seen += it.value } }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        awaitUntil { seen.size == 1 }
        y.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 2))
        awaitUntil { seen.size == 2 }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 3))
        awaitUntil { seen.size == 3 }
        assertEquals(listOf(1, 2, 3), seen.toList())
        x.emitter.complete(WatchAccount::class) { true }
        delay(150)
        assertFalse(collecting.isCompleted) // y still serves
        y.emitter.complete(WatchAccount::class) { true }
        withTimeout(5_000) { collecting.await() }
        assertEquals(0, hub.openSubscriptions())
    }

    @Test fun `every instance of an exclusive owner is a target`() = runBlocking {
        val x1 = owner("svc-x", "x-1", AccountWatcher())
        val x2 = owner("svc-x", "x-2", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7))
        assertEquals(Balance(107), result.initial)
        awaitUntil { x1.subscriptions.size == 1 && x2.subscriptions.size == 1 }
        val update = async { result.updates.take(1).toList() }
        x2.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 5)) // only the second instance learns of the change
        assertEquals(listOf(BalanceChanged(7, 5)), withTimeout(5_000) { update.await() })
    }

    @Test fun `SubscriptionOptions become the request fields`() = runBlocking {
        owner("svc-x", "x-1", AccountWatcher())
        val bus = caller()
        bus.subscribe(WatchAccount(7), SubscriptionOptions(timeout = Duration.ofMillis(1_234), maxResults = 5, priority = 7, initialCredit = 3)).cancel()
        val sent = hub.lastRequest()!!
        assertEquals(RequestMode.SUBSCRIPTION, sent.mode)
        assertEquals(RequestKind.QUERY, sent.kind)
        assertEquals("test.WatchAccount", sent.type)
        assertEquals(1_234, sent.timeoutMs)
        assertEquals(5, sent.maxResults)
        assertEquals(7, sent.priority)
        assertEquals(3, sent.initialCredit)
        assertEquals("", sent.routingKey)
        assertEquals("{\"account\":7}", sent.payload.toStringUtf8())

        bus.subscribe(WatchAccount(7)).cancel()
        val defaults = hub.lastRequest()!!
        assertEquals(listOf(0, 0, 0, 64), listOf(defaults.timeoutMs, defaults.maxResults, defaults.priority, defaults.initialCredit))
        assertFailsWith<IllegalArgumentException> { SubscriptionOptions(initialCredit = 0) }
        assertFailsWith<IllegalArgumentException> { SubscriptionOptions(maxResults = -1) }
        Unit
    }

    @Test fun `a slow collector holds the hub's updates at the window and the refill, counting the initial part, lets them go on`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7), SubscriptionOptions(initialCredit = 4))
        awaitUntil { x.subscriptions.size == 1 }
        val gate = CompletableDeferred<Unit>()
        val seen = CopyOnWriteArrayList<Int>()
        val collecting = async {
            result.updates.take(10).collect { update ->
                if (update.value == 1) gate.await() // the first update is held: only the initial part was consumed
                seen += update.value
            }
        }
        repeat(10) { n -> x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, n + 1)) }
        // Window 4: the initial part took one credit, three updates were relayed, the other seven wait at the hub.
        awaitUntil { hub.bufferedUpdatesOf(hub.lastRequest()!!.id) == 7 }
        delay(200)
        assertEquals(emptyList(), seen.toList())
        assertEquals(emptyList(), hub.flowsFrom(callerId()))
        gate.complete(Unit)
        withTimeout(5_000) { collecting.await() }
        assertEquals((1..10).toList(), seen.toList())
        // One Flow{2} per two parts consumed (the initial part counts: it took a credit), none for a part never consumed.
        assertTrue(hub.flowsFrom(callerId()).all { it == 2 }, hub.flowsFrom(callerId()).toString())
        assertTrue(hub.flowsFrom(callerId()).size in 4..5, hub.flowsFrom(callerId()).toString())
    }

    @Test fun `the initial part counts as consumed when subscribe returns it, a caller that collects late has its first Flow out already`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7), SubscriptionOptions(initialCredit = 2)) // refill: 1 part
        awaitUntil { x.subscriptions.size == 1 }
        // Nobody collects yet: the initial part was handed over by subscribe, which is one part consumed.
        awaitUntil { hub.flowsFrom(callerId()) == listOf(1) }
        repeat(3) { n -> x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, n + 1)) }
        awaitUntil { hub.bufferedUpdatesOf(hub.lastRequest()!!.id) == 1 } // credit 2 + 1 granted = 3 parts relayed: the initial and two updates
        delay(150)
        assertEquals(listOf(1), hub.flowsFrom(callerId())) // the updates relayed are not consumed yet
        val seen = CopyOnWriteArrayList<Int>()
        val collecting = async { result.updates.take(3).collect { seen += it.value } }
        withTimeout(5_000) { collecting.await() }
        assertEquals(listOf(1, 2, 3), seen.toList())
        // The first update consumed released the next Flow.
        assertEquals(listOf(1, 1, 1), hub.flowsFrom(callerId()).take(3))
    }

    @Test fun `a window of 1 does not deadlock, the initial part is returned at once`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7), SubscriptionOptions(initialCredit = 1))
        awaitUntil { x.subscriptions.size == 1 }
        val update = async { result.updates.take(3).toList() }
        repeat(3) { n -> x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, n + 1)) }
        assertEquals(listOf(1, 2, 3), withTimeout(5_000) { update.await() }.map { it.value })
    }

    @Test fun `a subscriber that falls more than the hub's buffer behind loses the subscription with HandlerBusyException`() = runBlocking {
        hub.subscriptionBuffer = 2
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7), SubscriptionOptions(initialCredit = 2))
        awaitUntil { x.subscriptions.size == 1 }
        awaitUntil { hub.flowsFrom(callerId()) == listOf(1) } // the initial part is consumed once subscribe returned it
        // Nobody collects: credit 2 + 1, the initial part took one, two updates are relayed, two are buffered, the next ends it.
        repeat(6) { n -> x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, n + 1)) }
        awaitUntil { hub.openSubscriptions() == 0 }
        val seen = mutableListOf<Int>()
        val e = assertFailsWith<HandlerBusyException> { result.updates.collect { seen += it.value } }
        assertEquals("test.WatchAccount", e.type)
        assertTrue(e.message!!.contains("subscriber buffer (2) exceeded"), e.message)
        assertEquals(listOf(1, 2), seen) // what was relayed before stays delivered, the buffered updates were dropped
        awaitUntil { x.subscriptions.size == 0 } // the hub cancelled the target
    }

    @Test fun `maxResults counts updates, ends the subscription cleanly and cancels the targets`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7), SubscriptionOptions(maxResults = 3))
        awaitUntil { x.subscriptions.size == 1 }
        val updates = async { result.updates.toList() }
        repeat(10) { n -> x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, n + 1)) }
        assertEquals(listOf(1, 2, 3), withTimeout(5_000) { updates.await() }.map { it.value })
        awaitUntil { x.subscriptions.size == 0 }
        assertEquals(0, hub.openSubscriptions())
    }

    @Test fun `cancelling the collector sends Cancel once and the owner drops the subscription`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7))
        awaitUntil { x.subscriptions.size == 1 }
        val update = async { result.updates.take(1).toList() }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        assertEquals(1, withTimeout(5_000) { update.await() }.single().value)
        awaitUntil { hub.cancelsFrom(callerId()).size == 1 }
        assertEquals("collector cancelled", hub.cancelsFrom(callerId()).single().reason)
        awaitUntil { x.subscriptions.size == 0 }
        assertEquals(0, hub.openSubscriptions())
        delay(200)
        assertEquals(1, hub.cancelsFrom(callerId()).size)
    }

    @Test fun `an exception in the collector sends Cancel and reaches the caller unchanged`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7))
        awaitUntil { x.subscriptions.size == 1 }
        val failing = async { runCatching { result.updates.collect { throw IllegalStateException("stop") } } }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        assertEquals("stop", assertIs<IllegalStateException>(withTimeout(5_000) { failing.await() }.exceptionOrNull()).message)
        awaitUntil { hub.cancelsFrom(callerId()).size == 1 }
        assertTrue(hub.cancelsFrom(callerId()).single().reason.contains("stop"), hub.cancelsFrom(callerId()).toString())
        awaitUntil { x.subscriptions.size == 0 }
    }

    @Test fun `cancel completes the updates, sends Cancel once and the owner drops the subscription`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7))
        awaitUntil { x.subscriptions.size == 1 }
        val seen = CopyOnWriteArrayList<Int>()
        val collecting = async { result.updates.collect { seen += it.value } }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        awaitUntil { seen.size == 1 }
        result.cancel()
        result.cancel() // idempotent
        withTimeout(5_000) { collecting.await() } // completes normally
        awaitUntil { x.subscriptions.size == 0 }
        assertEquals(listOf("subscriber cancelled"), hub.cancelsFrom(callerId()).map { it.reason })
        // Cancelled before any collection: the flow is empty.
        val second = caller().subscribe(WatchAccount(8))
        second.cancel()
        assertEquals(emptyList(), second.updates.toList())
    }

    @Test fun `the owner losing its hub stream is HandlerDisconnectedException, the subscriber losing it HubConnectionLostException`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val first = caller().subscribe(WatchAccount(7))
        awaitUntil { x.subscriptions.size == 1 }
        val owned = async { runCatching { first.updates.toList() } }
        hub.dropInstance("x-1")
        val e = assertIs<HandlerDisconnectedException>(withTimeout(5_000) { owned.await() }.exceptionOrNull())
        assertEquals("test.WatchAccount", e.type)

        // A new stream (connected may still be the stale true of the old one), acknowledged, before the next subscribe.
        awaitUntil { hub.streamsOpenedBy("x-1") >= 2 }
        assertTrue(x.connection.awaitRegistered(wait))
        awaitUntil { x.subscriptions.size == 0 && hub.openSubscriptions() == 0 }
        val bus = caller()
        val second = bus.subscribe(WatchAccount(7))
        awaitUntil { x.subscriptions.size == 1 }
        val lost = async { runCatching { second.updates.toList() } }
        hub.dropInstance(connections.last { it.instanceId.startsWith("caller-") }.instanceId)
        assertIs<HubConnectionLostException>(withTimeout(5_000) { lost.await() }.exceptionOrNull())
        awaitUntil { x.subscriptions.size == 0 } // the hub cancelled the target: "sender disconnected"
    }

    @Test fun `the owner's subscription limit ends the subscription with HandlerBusyException`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher(), maxSubscriptions = 1)
        val bus = caller()
        val first = bus.subscribe(WatchAccount(1))
        awaitUntil { x.subscriptions.size == 1 }
        // The refusal is the target's ReplyEnd; the hub relays it, before or after the initial part.
        val outcome = runCatching { bus.subscribe(WatchAccount(2)).updates.first() }
        val e = assertIs<HandlerBusyException>(outcome.exceptionOrNull(), outcome.toString())
        assertTrue(e.message!!.contains("subscription limit (1) reached"), e.message)
        assertEquals(1, x.subscriptions.size)
        val update = async { first.updates.take(1).toList() }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(1, 1))
        assertEquals(1, withTimeout(5_000) { update.await() }.single().value)
    }

    @Test fun `a subscription goes to one hub, round-robin over the live hubs`() = runBlocking {
        val other = hub()
        owner("svc-x", "x-1", AccountWatcher(), hubName = hub.name)
        owner("svc-x", "x-2", AccountWatcher(), hubName = other.name)
        val bus = bus(multiCaller(hub.name, other.name))
        repeat(4) { assertEquals(Balance(107), bus.subscribe(WatchAccount(7)).also { it.cancel() }.initial) }
        assertEquals(2, hub.requestsFrom("caller"))
        assertEquals(2, other.requestsFrom("caller"))
    }

    @Test fun `errors before the initial result map as for a unary query`() = runBlocking {
        val bus = caller()
        assertEquals("test.WatchAccount", assertFailsWith<NoHandlerException> { bus.subscribe(WatchAccount(1)) }.type)
        // The initial request never answering: TIMEOUT from the hub, the targets cancelled.
        val x = owner("svc-x", "x-1", HangingWatcher())
        assertEquals("test.WatchAccount", assertFailsWith<MessageTimeoutException> { bus.subscribe(WatchAccount(1), SubscriptionOptions(timeout = Duration.ofMillis(400))) }.type)
        awaitUntil { x.subscriptions.size == 0 }
        assertEquals(0, hub.openSubscriptions())
    }

    @Test fun `a ReplyEnd with an error maps like a unary Error with the same code`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val bus = caller()
        val expectations = listOf<Pair<ErrorCode, (Throwable) -> Unit>>(
            ErrorCode.NO_HANDLER to { assertEquals("test.WatchAccount", assertIs<NoHandlerException>(it).type) },
            ErrorCode.TIMEOUT to { assertEquals("test.WatchAccount", assertIs<MessageTimeoutException>(it).type) },
            ErrorCode.HANDLER_DISCONNECTED to { assertEquals("test.WatchAccount", assertIs<HandlerDisconnectedException>(it).type) },
            ErrorCode.HANDLER_ERROR to { assertEquals("HANDLER_ERROR" to "HANDLER_ERROR says", assertIs<RemoteHandlerException>(it).let { e -> e.code to e.message }) },
            ErrorCode.UNAUTHENTICATED to { assertEquals("UNAUTHENTICATED says", assertIs<UnauthenticatedException>(it).message) },
            ErrorCode.INVALID_REQUEST to { assertEquals("INVALID_REQUEST says", assertIs<InvalidMessageException>(it).message) },
            ErrorCode.PERMISSION_DENIED to { assertEquals("PERMISSION_DENIED says", assertIs<AccessDeniedException>(it).message) },
            ErrorCode.RESOURCE_EXHAUSTED to { assertTrue(assertIs<HandlerBusyException>(it).message!!.contains("RESOURCE_EXHAUSTED says"), it.message) },
            ErrorCode.CANCELLED to { assertEquals("CANCELLED says", assertIs<StreamCancelledException>(it).message) },
        )
        for ((code, check) in expectations) {
            val result = bus.subscribe(WatchAccount(1))
            awaitUntil { x.subscriptions.size == 1 }
            val id = hub.lastRequest()!!.id
            val collecting = async { runCatching { result.updates.toList() } }
            val error = Error.newBuilder().setRequestId(id).setCode(code).setMessage("$code says")
            hub.inject(callerId(), ServerMessage.newBuilder().setEnd(ReplyEnd.newBuilder().setRequestId(id).setParts(1).setError(error)).build())
            check(assertNotNull(withTimeout(5_000) { collecting.await() }.exceptionOrNull(), "$code should fail the updates"))
            // The end was injected: the hub still has the subscription open, and the library rightly sends no Cancel after a terminal message.
            x.emitter.complete(WatchAccount::class) { true }
            awaitUntil { x.subscriptions.size == 0 && hub.openSubscriptions() == 0 }
        }
    }

    @Test fun `the max subscription duration is a StreamCancelledException`() = runBlocking {
        hub.maxSubscriptionDuration = Duration.ofMillis(500)
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7))
        val e = assertFailsWith<StreamCancelledException> { result.updates.toList() }
        assertTrue(e.message!!.startsWith("max subscription duration"), e.message)
        awaitUntil { x.subscriptions.size == 0 }
    }

    @Test fun `the subscription is retried before its initial result, never after`() = runBlocking {
        val retry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(300), maxBackoff = Duration.ofMillis(300))
        val bus = caller(retry = retry)
        // NO_HANDLER is retried, and the owner that appeared during the backoff answers the second attempt.
        val early = async { bus.subscribe(WatchAccount(7), SubscriptionOptions(timeout = Duration.ofSeconds(4))) }
        delay(100)
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = withTimeout(5_000) { early.await() }
        assertEquals(Balance(107), result.initial)
        assertEquals(2, hub.requestsFrom(callerId()))

        // After the initial part nothing is retried: the lost hub stream ends the updates.
        awaitUntil { x.subscriptions.size == 1 }
        val late = async { runCatching { result.updates.toList() } }
        hub.dropInstance(callerId())
        assertIs<HubConnectionLostException>(withTimeout(3_000) { late.await() }.exceptionOrNull())
        delay(700) // longer than the backoff: a retry would have sent another request
        assertEquals(2, hub.requestsFrom(callerId()))
    }

    @Test fun `a retryable ReplyEnd error before the initial result is retried with a fresh id`() = runBlocking {
        val retry = RetryPolicy(maxAttempts = 3, initialBackoff = Duration.ofMillis(200), maxBackoff = Duration.ofMillis(200))
        val bus = caller(retry = retry)
        val hanging = owner("svc-x", "x-1", HangingWatcher())
        val subscribing = async { bus.subscribe(WatchAccount(7), SubscriptionOptions(timeout = Duration.ofSeconds(4))) }
        awaitUntil { hub.openSubscriptions() == 1 } // pinned to x-1, whose initial request never answers
        val first = hub.lastRequest()!!
        owner("svc-y", "y-1", SharedAccountWatcher()) // not a target of the first subscription: it registered after the open
        hanging.connection.stop() // the last target left: HANDLER_DISCONNECTED before any part
        connections.remove(hanging.connection)
        val result = withTimeout(6_000) { subscribing.await() }
        assertEquals(Balance(107), result.initial)
        assertEquals(2, hub.requestsFrom(callerId())) // one retry, a new request
        assertTrue(hub.lastRequest()!!.id != first.id)
        result.cancel()
    }

    @Test fun `an update that does not decode fails the updates and cancels, an initial that does not decode is InvalidMessageException`() = runBlocking {
        // An owner whose initial answer has the wrong shape: raw handler on the wire.
        val raw = HubConnection(
            channelFactory = { InProcessChannelBuilder.forName(hub.name).build() },
            serviceName = "svc-raw", instanceId = "raw-1", settings = settings,
            inbound = { req -> Reply.newBuilder().setRequestId(req.id).setPayload(json("{\"value\":\"not a number\"}")).build() },
            scope = scope,
            subscriptions = SubscriptionRegistry(),
            inboundSubscription = { _, register -> register(Any()) },
        ).also { connections += it }
        raw.register(emptySet(), setOf("test.WatchAccount"))
        raw.start()
        assertTrue(raw.awaitRegistered(wait))
        val bus = caller()
        val e = assertFailsWith<InvalidMessageException> { bus.subscribe(WatchAccount(1)) }
        assertTrue(e.message!!.contains("test.WatchAccount"), e.message)
        awaitUntil { hub.openSubscriptions() == 0 } // the half-open subscription was cancelled
        assertEquals(1, hub.cancelsFrom(callerId()).size)
    }

    @Test fun `a gap in the parts' seq fails the updates as lost parts and cancels the subscription`() = runBlocking {
        hub.dropParts = setOf(3L) // the hub dropped update 2 (a full sender buffer): part 4 follows part 2
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7))
        awaitUntil { x.subscriptions.size == 1 }
        repeat(3) { n -> x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, n + 1)) }
        val seen = mutableListOf<Int>()
        val e = assertFailsWith<InvalidMessageException> { result.updates.collect { seen += it.value } }
        assertEquals("subscription test.WatchAccount lost 1 part(s)", e.message)
        assertEquals(listOf(1), seen)
        awaitUntil { hub.cancelsFrom(callerId()).size == 1 }
        assertTrue(hub.cancelsFrom(callerId()).single().reason.contains("lost"), hub.cancelsFrom(callerId()).toString())
        awaitUntil { x.subscriptions.size == 0 }
    }

    @Test fun `an end whose parts count exceeds the parts received fails the updates as lost parts`() = runBlocking {
        hub.dropParts = setOf(3L) // the last part was dropped: only the end's count can tell
        val x = owner("svc-x", "x-1", AccountWatcher())
        val result = caller().subscribe(WatchAccount(7))
        awaitUntil { x.subscriptions.size == 1 }
        repeat(2) { n -> x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, n + 1)) }
        x.emitter.complete(WatchAccount::class) { true }
        val seen = mutableListOf<Int>()
        val e = assertFailsWith<InvalidMessageException> { result.updates.collect { seen += it.value } }
        assertEquals("subscription test.WatchAccount lost 1 part(s)", e.message)
        assertEquals(listOf(1), seen)
        assertEquals(0, hub.openSubscriptions())
        delay(200)
        assertEquals(emptyList(), hub.cancelsFrom(callerId())) // the subscription had ended: nothing to cancel
    }

    @Test fun `an owner on two hubs loses only the subscriptions of the hub whose stream broke`() = runBlocking {
        val other = hub()
        val ownerRegistry = HandlerRegistry(resolver).apply { register(SharedAccountWatcher()) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(ownerRegistry, codec), ForwardedIdentityResolver(null), codec)
        val subscriptions = SubscriptionRegistry()
        val conns = HubConnections(
            targets = HubTargets.Static(listOf(hub.name, other.name)),
            channelFactory = { target -> InProcessChannelBuilder.forName(target).build() },
            serviceName = "svc-x",
            instanceId = "x-1",
            settings = settings,
            inbound = dispatcher::dispatch,
            scope = scope,
            inboundStream = dispatcher::dispatchStream,
            subscriptions = subscriptions,
            inboundSubscription = dispatcher::dispatchSubscription,
        ).also { multi += it }
        conns.register(emptySet(), emptySet(), ownerRegistry.sharedQueryTypes(), ownerRegistry.subscriptionQueryTypes())
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        val emitter = HubQueryUpdateEmitter(subscriptions, codec, resolver)
        val viaA = caller(hub.name).subscribe(WatchAccount(1))
        val viaB = caller(other.name).subscribe(WatchAccount(2))
        awaitUntil { subscriptions.size == 2 } // one shared registry over both hubs' streams

        other.dropInstance("x-1")
        awaitUntil { subscriptions.size == 1 }
        // The subscription on the hub that is still up keeps receiving; the other subscriber was told its last target left.
        val update = async { viaA.updates.take(1).toList() }
        emitter.emit(WatchAccount::class, { true }, BalanceChanged(1, 1))
        assertEquals(1, withTimeout(5_000) { update.await() }.single().value)
        assertIs<HandlerDisconnectedException>(runCatching { withTimeout(5_000) { viaB.updates.toList() } }.exceptionOrNull())
        Unit // a test method must return nothing: JUnit skips one that returns a value
    }

    @Test fun `the bearer token is read when subscribe is called, not when the updates are collected`() = runBlocking {
        val x = owner("svc-x", "x-1", AccountWatcher())
        val bus = caller()
        val token = TestJwt.token("user-42", listOf("member"))
        SecurityContextHolder.getContext().authentication = JwtAuthenticationToken(TestJwt.decoder().decode(token))
        val result = bus.subscribe(WatchAccount(7))
        SecurityContextHolder.clearContext()
        assertEquals("Bearer $token", assertNotNull(hub.lastRequestHeaders())["authorization"])
        awaitUntil { x.subscriptions.size == 1 }
        val update = async(Dispatchers.IO) { result.updates.take(1).toList() }
        x.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        assertEquals(1, withTimeout(5_000) { update.await() }.single().value)
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
                    Register.newBuilder().setServiceName("svc-raw").setInstanceId(instanceId).setInitialPermits(64),
                ).build(),
            )
        }

        fun send(message: ClientMessage) = out.trySend(message).getOrThrow()

        suspend fun next(): ServerMessage = withTimeout(5_000) { inbox.receive() }

        /** The next `ReplyEnd`, skipping the parts before it. */
        suspend fun nextEnd(): com.damoreira.snodra.server.v1.ReplyEnd {
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
