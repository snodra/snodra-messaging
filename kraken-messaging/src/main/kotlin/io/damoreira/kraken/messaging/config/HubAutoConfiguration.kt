package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.HubSettings
import io.damoreira.kraken.messaging.hub.HubTargets
import io.damoreira.kraken.messaging.hub.InboundRequestDispatcher
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.ManagedChannelBuilder
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

/**
 * The hub transport: [HubConnections] (one [io.damoreira.kraken.messaging.hub.HubConnection] per hub named by
 * `kraken.messaging.hub.address`), started by [HubLifecycle] once the context is refreshed, and [HubBus] as the default
 * [CommandBus] and [QueryBus]. Off with `kraken.messaging.hub.enabled=false`.
 *
 * Channels are built by the `krakenHubChannelFactory` bean ([HubChannelFactory], target -> channel), called once per
 * connection attempt; define a bean with that name to override it (tests point it at in-process hubs).
 */
@AutoConfiguration(after = [KrakenMessagingAutoConfiguration::class])
@ConditionalOnProperty(prefix = "kraken.messaging.hub", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class HubAutoConfiguration {

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean(name = [CHANNEL_FACTORY])
    fun krakenHubChannelFactory(): HubChannelFactory = HubChannelFactory { target ->
        ManagedChannelBuilder.forTarget(target).usePlaintext()
            // HTTP/2 pings detect a dead peer at the transport level even while the stream is idle.
                // 10 s / 5 s: a one-way partition is caught by the transport about as fast as by the hub's 10 s heartbeat timeout.
                .keepAliveTime(10, TimeUnit.SECONDS)
                .keepAliveTimeout(5, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .build()
    }

    @Bean
    @ConditionalOnMissingBean
    fun inboundRequestDispatcher(invoker: HandlerInvoker, identities: ForwardedIdentityResolver, codec: JsonCodec) =
        InboundRequestDispatcher(invoker, identities, codec)

    @Bean
    @ConditionalOnMissingBean
    fun hubLifecycle(
        properties: KrakenMessagingProperties,
        @Qualifier(CHANNEL_FACTORY) channelFactory: HubChannelFactory,
        dispatcher: InboundRequestDispatcher,
        registry: HandlerRegistry,
    ): HubLifecycle {
        val hub = properties.hub
        // The connect loop, stream collector and heartbeats stay on Default; handlers get virtual threads (see HubConnection).
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("kraken-hub"))
        val handlerDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
        val connections = HubConnections(
            targets = HubTargets.parse(hub.address),
            channelFactory = channelFactory,
            serviceName = properties.serviceName,
            instanceId = UUID.randomUUID().toString(),
            settings = HubSettings(hub.requestTimeout, hub.heartbeatInterval, hub.reconnectBackoff.min, hub.reconnectBackoff.max),
            inbound = dispatcher::dispatch,
            scope = scope,
            handlerDispatcher = handlerDispatcher,
            maxConcurrentHandlers = hub.maxConcurrentHandlers,
            loadFactor = hub.loadFactor,
            tags = hub.tags,
            statusInterval = hub.statusInterval,
        )
        return HubLifecycle(connections, registry, scope, handlerDispatcher)
    }

    @Bean
    fun hubConnections(lifecycle: HubLifecycle): HubConnections = lifecycle.connections

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean
    fun hubBus(
        connections: HubConnections,
        registry: HandlerRegistry,
        invoker: HandlerInvoker,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        traceHeaders: TraceHeaders,
        properties: KrakenMessagingProperties,
    ) = HubBus(
        connections, registry, invoker, codec, typeResolver,
        forwardJwt = properties.security.forwardJwt,
        traceHeaders = traceHeaders,
        requestTimeout = properties.hub.requestTimeout,
        priorities = properties.hub.priorities,
        retry = properties.hub.retry.policy(),
    )

    // Facades delegate instead of returning hubBus itself: an object implementing both interfaces would make each
    // facade bean a by-type match for the other one and clash with a user bean that overrides only one of them.
    @Bean
    @ConditionalOnMissingBean(CommandBus::class)
    fun commandBus(@Qualifier("hubBus") hubBus: HubBus): CommandBus = object : CommandBus by hubBus {}

    @Bean
    @ConditionalOnMissingBean(QueryBus::class)
    fun queryBus(@Qualifier("hubBus") hubBus: HubBus): QueryBus = object : QueryBus by hubBus {}

    companion object {
        const val CHANNEL_FACTORY = "krakenHubChannelFactory"
    }
}

/**
 * Starts the [connections] after every singleton is initialised (so every handler bean is in the registry), announcing
 * the registry's command, exclusive query and shared query types and re-announcing them whenever handlers are added
 * later.
 *
 * Phase [PHASE] is below the web server's start/stop phase (`WebServerApplicationContext.START_STOP_LIFECYCLE_PHASE`,
 * `DEFAULT_PHASE - 2048` in Boot 4.1), so the hub connection starts before the server accepts traffic and stops only
 * after the graceful HTTP drain (`GRACEFUL_SHUTDOWN_PHASE`, `DEFAULT_PHASE - 1024`) and the server stop.
 *
 * On stop it first announces an empty registration so the hubs stop routing to this instance, waits up to
 * [drainTimeout] for incoming handlers still running, then closes the stream, cancels [scope] and closes
 * [handlerDispatcher].
 */
class HubLifecycle(
    val connections: HubConnections,
    private val registry: HandlerRegistry,
    private val scope: CoroutineScope,
    private val handlerDispatcher: ExecutorCoroutineDispatcher? = null,
    private val drainTimeout: Duration = Duration.ofSeconds(5),
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(HubLifecycle::class.java)
    @Volatile private var running = false

    @Synchronized
    override fun start() {
        if (running) return
        running = true // before the first announce: announce() is a no-op once stopped
        announce()
        registry.onChange(::announce)
        connections.start()
    }

    @Synchronized
    override fun stop() {
        if (!running) return
        running = false
        try {
            runBlocking {
                connections.register(emptySet(), emptySet())
                val deregistered = runCatching { connections.awaitRegistered(Duration.ofSeconds(1)) }.getOrDefault(false)
                if (!deregistered) log.debug("Not every hub confirmed the empty registration before shutdown")
                if (!connections.awaitHandlersIdle(drainTimeout)) {
                    log.warn("{} incoming hub request(s) still running after {}; stopping anyway", connections.inFlightHandlers.value, drainTimeout)
                }
                connections.stop()
            }
        } finally {
            scope.cancel()
            handlerDispatcher?.close()
        }
    }

    override fun isRunning(): Boolean = running
    override fun getPhase(): Int = PHASE

    private fun announce() {
        if (running) connections.register(registry.commandTypes(), registry.exclusiveQueryTypes(), registry.sharedQueryTypes())
    }

    companion object {
        /** `WebServerApplicationContext.START_STOP_LIFECYCLE_PHASE - 1` (the class is not on the starter's classpath). */
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 2049
    }
}
