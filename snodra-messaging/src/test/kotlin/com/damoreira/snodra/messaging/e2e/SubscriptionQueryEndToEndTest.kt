package com.damoreira.snodra.messaging.e2e

import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.Ports
import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.server.v1.HubGrpcKt
import com.damoreira.snodra.server.v1.Reply
import com.damoreira.snodra.server.v1.TopologyRequest
import com.damoreira.snodra.messaging.HandlerBusyException
import com.damoreira.snodra.messaging.QueryUpdateEmitter
import com.damoreira.snodra.messaging.SubscriptionOptions
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.fixtures.Balance
import com.damoreira.snodra.messaging.fixtures.BalanceChanged
import com.damoreira.snodra.messaging.fixtures.WatchAccount
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.hub.HubBus
import com.damoreira.snodra.messaging.hub.HubChannelFactory
import com.damoreira.snodra.messaging.hub.HubConnections
import com.damoreira.snodra.messaging.hub.HubQueryUpdateEmitter
import com.damoreira.snodra.messaging.hub.HubSettings
import com.damoreira.snodra.messaging.hub.HubTargets
import com.damoreira.snodra.messaging.hub.InboundRequestDispatcher
import com.damoreira.snodra.messaging.hub.SharedAccountWatcher
import com.damoreira.snodra.messaging.hub.SubscriptionRegistry
import com.damoreira.snodra.messaging.security.ForwardedIdentityResolver
import com.damoreira.snodra.messaging.test.SnodraContainers
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assumptions
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.testcontainers.containers.GenericContainer
import java.net.ServerSocket
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.TimeUnit
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Subscription queries against two containers of the hub image under test (`SNODRA_SERVER_IMAGE`) in the
 * multi-hub configuration: every instance is connected to both hubs. Two services own `test.WatchAccount` (a shared type)
 * through the real handler stack and their own [QueryUpdateEmitter]; a third subscribes through `QueryBus.subscribe`,
 * connected to the first hub only so that every subscription lands on it.
 *
 * The hub exports its metrics over OTLP only (`snodra_server_subscriptions_open` is not readable over HTTP) and `/api/v1/topology`
 * does not show subscriptions, so what the hub holds open is observed through its limit: the hubs run with
 * `SNODRA_SERVER_MAX_SUBSCRIPTIONS_PER_INSTANCE=1`, a second subscription is refused with `RESOURCE_EXHAUSTED` while the
 * first is open, and accepted again once the hub has released it (the owners' registries emptying shows the hub's `Cancel`
 * fan-out reached them). Skips when the image is not built.
 */
class SubscriptionQueryEndToEndTest {
    private val log = LoggerFactory.getLogger(SubscriptionQueryEndToEndTest::class.java)
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val clients = mutableListOf<HubConnections>()
    private val containers = mutableListOf<GenericContainer<*>>()
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver("test")
    private val defaults = HubSettings(
        requestTimeout = null,
        backoffMin = Duration.ofMillis(500),
        backoffMax = Duration.ofSeconds(5),
    )
    private val wait = Duration.ofSeconds(15)

    private class Hub(val container: GenericContainer<*>, val id: String, val grpcPort: Int) {
        val target get() = "localhost:$grpcPort"
    }

    /** One service instance owning `test.WatchAccount`: its connections, its subscriptions and its emitter. */
    private class Owner(val connections: HubConnections, val subscriptions: SubscriptionRegistry, val emitter: QueryUpdateEmitter)

    @AfterEach fun tearDown() = runBlocking {
        clients.forEach { it.stop() }
        scope.cancel()
        containers.forEach { runCatching { it.stop() } }
    }

    @Test fun `a subscriber gets the initial result, the updates of both owner instances in order, then the end when both complete`() = runBlocking {
        val hubs = startHubs()
        val a = owner(hubs, "svc-acc-a", base = 100)
        val b = owner(hubs, "svc-acc-b", base = 200)
        val bus = caller(hubs.take(1))

        val result = withTimeout(30_000) { bus.subscribe(WatchAccount(7), SubscriptionOptions(timeout = Duration.ofSeconds(10))) }
        log.info("Initial result {}", result.initial)
        assertTrue(result.initial == Balance(107) || result.initial == Balance(207), result.initial.toString())
        // Both owners are targets, on the hub the subscription went to.
        awaitUntil { a.subscriptions.size == 1 && b.subscriptions.size == 1 }

        val seen = CopyOnWriteArrayList<BalanceChanged>()
        val collecting = async { result.updates.collect { seen += it } }
        a.emitter.emit(WatchAccount::class, { it.account == 7 }, BalanceChanged(7, 1))
        awaitUntil { seen.size == 1 }
        b.emitter.emit(WatchAccount::class, { it.account == 7 }, BalanceChanged(7, 2))
        awaitUntil { seen.size == 2 }
        a.emitter.emit(WatchAccount::class, { it.account == 7 }, BalanceChanged(7, 3))
        a.emitter.emit(WatchAccount::class, { it.account == 8 }, BalanceChanged(8, 99)) // another account: nobody is told
        awaitUntil { seen.size == 3 }
        assertEquals(listOf(BalanceChanged(7, 1), BalanceChanged(7, 2), BalanceChanged(7, 3)), seen.toList())

        a.emitter.complete(WatchAccount::class) { true }
        delay(300)
        assertTrue(!collecting.isCompleted, "b still serves")
        b.emitter.complete(WatchAccount::class) { true }
        withTimeout(10_000) { collecting.await() }
        assertEquals(listOf(BalanceChanged(7, 1), BalanceChanged(7, 2), BalanceChanged(7, 3)), seen.toList())
        awaitUntil { a.subscriptions.size == 0 && b.subscriptions.size == 0 }
        // Both completed cleanly: the hub released its targets, the next subscription is accepted again.
        withTimeout(30_000) { bus.subscribe(WatchAccount(7)) }.cancel()
    }

    @Test fun `the hub serves a declared subscription type in mode SUBSCRIPTION only - a point query, a stream and a gather of it are INVALID_REQUEST`() = runBlocking {
        val hubs = startHubs()
        val a = owner(hubs, "svc-acc-a", base = 100)
        // The typed API cannot send a SubscriptionQuery any other way: a raw sender on the transport does.
        val raw = connections(hubs.take(1), "svc-raw").apply { start(); assertTrue(awaitRegistered(wait)) }
        fun request() = com.damoreira.snodra.server.v1.Request.newBuilder()
            .setId(UUID.randomUUID().toString()).setKind(com.damoreira.snodra.server.v1.RequestKind.QUERY).setType("test.WatchAccount")
            .setPayload(com.google.protobuf.ByteString.copyFrom(codec.encode(WatchAccount(7)))).setTimeoutMs(5_000).build()

        val answers = listOf(
            withTimeout(30_000) { raw.send(request()) },
            withTimeout(30_000) { raw.stream(request()) { } },
            withTimeout(30_000) { raw.gather(request()) { } },
        )
        for (answer in answers) {
            assertTrue(answer.hasError(), answer.toString())
            assertEquals(ErrorCode.INVALID_REQUEST, answer.error.code, answer.toString())
            assertTrue("subscription type" in answer.error.message, answer.error.message)
        }
        assertEquals(0, a.subscriptions.size, "nothing reached the owner")
        // The declared type still subscribes.
        withTimeout(30_000) { caller(hubs.take(1)).subscribe(WatchAccount(7)) }.cancel()
    }

    @Test fun `a cancel mid-way frees what the hub holds open for the subscription, maxResults does the same`() = runBlocking {
        val hubs = startHubs()
        val a = owner(hubs, "svc-acc-a", base = 100)
        val b = owner(hubs, "svc-acc-b", base = 200)
        val bus = caller(hubs.take(1))

        val result = withTimeout(30_000) { bus.subscribe(WatchAccount(7)) }
        awaitUntil { a.subscriptions.size == 1 && b.subscriptions.size == 1 }
        // The hub counts the open targets per instance: with a limit of 1 a second subscription is refused while this one is open.
        val refused = assertFailsWith<HandlerBusyException> { bus.subscribe(WatchAccount(8)) }
        assertTrue(refused.message!!.contains("already serves 1 subscription"), refused.message)
        assertEquals(1, a.subscriptions.size)

        val first = async { result.updates.take(1).toList() }
        a.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        assertEquals(listOf(BalanceChanged(7, 1)), withTimeout(10_000) { first.await() }) // take(1) cancels the collector
        awaitUntil { a.subscriptions.size == 0 && b.subscriptions.size == 0 } // the hub's Cancel reached both owners
        assertNoRequestsInFlight(hubs)

        // The slot is free: cancel() completes the flow of a collector that is waiting, and frees it again.
        val second = withTimeout(30_000) { bus.subscribe(WatchAccount(7)) }
        awaitUntil { a.subscriptions.size == 1 && b.subscriptions.size == 1 }
        val waiting = async { second.updates.toList() }
        delay(200)
        second.cancel()
        assertEquals(emptyList(), withTimeout(10_000) { waiting.await() })
        awaitUntil { a.subscriptions.size == 0 && b.subscriptions.size == 0 }

        // maxResults ends it by the hub's count, cancels the owners and frees the slot.
        val capped = withTimeout(30_000) { bus.subscribe(WatchAccount(7), SubscriptionOptions(maxResults = 2)) }
        awaitUntil { a.subscriptions.size == 1 && b.subscriptions.size == 1 }
        val updates = async { capped.updates.toList() }
        a.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
        a.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 2))
        a.emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 3))
        assertEquals(listOf(BalanceChanged(7, 1), BalanceChanged(7, 2)), withTimeout(10_000) { updates.await() })
        awaitUntil { a.subscriptions.size == 0 && b.subscriptions.size == 0 }
        withTimeout(30_000) { bus.subscribe(WatchAccount(7)) }.cancel() // accepted: nothing is held open any more
    }

    // --- helpers ---

    private fun startHubs(): List<Hub> {
        Assumptions.assumeTrue(SnodraContainers.hubImageAvailable(), "hub image under test not built")
        val suffix = UUID.randomUUID().toString().take(8)
        return listOf(start("e2e-p4c-1-$suffix"), start("e2e-p4c-2-$suffix"))
    }

    private fun start(id: String): Hub {
        val grpc = ServerSocket(0).use { it.localPort }
        val web = ServerSocket(0).use { it.localPort }
        val container = SnodraContainers.hub()
            .withEnv("SNODRA_SERVER_OTLP_ENDPOINT", "")
            .withEnv("SNODRA_SERVER_ID", id)
            .withEnv("SNODRA_SERVER_MAX_SUBSCRIPTIONS_PER_INSTANCE", "1")
            .withCreateContainerCmdModifier { cmd ->
                cmd.hostConfig!!.withPortBindings(
                    Ports().apply {
                        bind(ExposedPort.tcp(7000), Ports.Binding.bindPort(grpc))
                        bind(ExposedPort.tcp(7001), Ports.Binding.bindPort(web))
                    },
                )
            }
        containers += container
        container.start()
        return Hub(container, id, grpc).also(::awaitReachable)
    }

    /** Waits until the fixed host port reaches the hub (port forwarding may lag the "listening" log line). */
    private fun awaitReachable(hub: Hub) = runBlocking {
        withTimeout(30_000) {
            while (runCatching { topology(hub) }.isFailure) delay(100)
        }
    }

    private suspend fun topology(hub: Hub) = ManagedChannelBuilder.forTarget(hub.target).usePlaintext().build().let { channel ->
        try {
            HubGrpcKt.HubCoroutineStub(channel).withDeadlineAfter(2, TimeUnit.SECONDS).topology(TopologyRequest.getDefaultInstance())
        } finally {
            channel.shutdownNow()
        }
    }

    private suspend fun assertNoRequestsInFlight(hubs: List<Hub>) {
        for (hub in hubs) {
            withTimeout(10_000) {
                while (topology(hub).servicesList.flatMap { it.instancesList }.any { it.inFlightAsHandler > 0 || it.inFlightAsSender > 0 }) delay(50)
            }
        }
    }

    private suspend fun awaitUntil(ms: Long = 15_000, condition: () -> Boolean) = withTimeout(ms) { while (!condition()) delay(25) }

    private fun connections(
        hubs: List<Hub>,
        service: String,
        dispatcher: InboundRequestDispatcher? = null,
        subscriptions: SubscriptionRegistry = SubscriptionRegistry(),
    ) = HubConnections(
        targets = HubTargets.parse(hubs.joinToString(",") { it.target }),
        channelFactory = HubChannelFactory { target ->
            ManagedChannelBuilder.forTarget(target).usePlaintext()
                .keepAliveTime(10, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS).keepAliveWithoutCalls(true).build()
        },
        serviceName = service,
        instanceId = "$service-" + UUID.randomUUID(),
        settings = defaults,
        inbound = dispatcher?.let { it::dispatch } ?: { req ->
            Reply.newBuilder().setRequestId(req.id)
                .setError(Error.newBuilder().setRequestId(req.id).setCode(ErrorCode.HANDLER_ERROR).setMessage("unexpected")).build()
        },
        scope = scope,
        subscriptions = subscriptions,
        inboundSubscription = dispatcher?.let { it::dispatchSubscription } ?: { req, _ ->
            Error.newBuilder().setRequestId(req.id).setCode(ErrorCode.INVALID_REQUEST).setMessage("unexpected").build()
        },
    ).also { clients += it }

    /** [service]'s instance owning the shared `test.WatchAccount` (initial balance `base + account`) on both hubs. */
    private suspend fun owner(hubs: List<Hub>, service: String, base: Int): Owner {
        val registry = HandlerRegistry(resolver).apply { register(SharedAccountWatcher(base)) }
        val dispatcher = InboundRequestDispatcher(HandlerInvoker(registry, codec), ForwardedIdentityResolver(null), codec)
        val subscriptions = SubscriptionRegistry()
        val conns = connections(hubs, service, dispatcher, subscriptions)
        conns.register(emptySet(), registry.exclusiveQueryTypes(), registry.sharedQueryTypes(), registry.subscriptionQueryTypes())
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        return Owner(conns, subscriptions, HubQueryUpdateEmitter(subscriptions, codec, resolver))
    }

    /** A subscriber connected to [hubs] only (one hub: every subscription lands on it). */
    private suspend fun caller(hubs: List<Hub>): HubBus {
        val conns = connections(hubs, "svc-web")
        conns.start()
        assertTrue(conns.awaitRegistered(wait))
        withTimeout(10_000) { while (conns.liveHubIds().size < hubs.size) delay(25) }
        val registry = HandlerRegistry(resolver)
        return HubBus(conns, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), null)
    }
}
