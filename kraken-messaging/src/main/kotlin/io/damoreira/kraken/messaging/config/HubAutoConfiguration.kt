package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.DefaultHubChannelFactory
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubTls
import io.damoreira.kraken.messaging.hub.HubToken
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.HubSettings
import io.damoreira.kraken.messaging.hub.HubTargets
import io.damoreira.kraken.messaging.hub.InboundRequestDispatcher
import io.damoreira.kraken.messaging.hub.SubscriptionRegistry
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.nio.file.Path

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
    fun krakenHubChannelFactory(properties: KrakenMessagingProperties): HubChannelFactory {
        val tls = properties.hub.tls
        if (!tls.enabled && (tls.caFile != null || tls.serverName != null)) {
            log.warn("kraken.messaging.hub.tls.ca-file / server-name are set but kraken.messaging.hub.tls.enabled is false: talking plain text to the hub")
        }
        // A bad CA file fails the startup here, with the setting's name, instead of failing every connection attempt.
        return DefaultHubChannelFactory(HubTls(tls.enabled, tls.caFile, tls.serverName)).also { it.validate() }
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
        subscriptions: SubscriptionRegistry,
        environment: Environment,
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
            settings = HubSettings(hub.requestTimeout, hub.heartbeatInterval, hub.reconnectBackoff.min, hub.reconnectBackoff.max, hub.streamCreditTimeout, hubToken(hub, environment)),
            inbound = dispatcher::dispatch,
            scope = scope,
            handlerDispatcher = handlerDispatcher,
            maxConcurrentHandlers = hub.maxConcurrentHandlers,
            loadFactor = hub.loadFactor,
            tags = hub.tags,
            statusInterval = hub.statusInterval,
            inboundStream = dispatcher::dispatchStream,
            subscriptions = subscriptions,
            inboundSubscription = dispatcher::dispatchSubscription,
        )
        return HubLifecycle(connections, registry, scope, handlerDispatcher, subscriptions = subscriptions)
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

    private val log = LoggerFactory.getLogger(HubAutoConfiguration::class.java)

    /**
     * The hub token: `hub.token-file` (the only source when set, re-read on every reconnect), else `hub.token`; each falls
     * back to the plain environment variable `KRAKEN_HUB_TOKEN_FILE` / `KRAKEN_HUB_TOKEN` (the name the hub's own
     * documentation uses; relaxed binding alone only maps `KRAKEN_MESSAGING_HUB_TOKEN`) when its property is unset.
     */
    private fun hubToken(hub: KrakenMessagingProperties.Hub, environment: Environment): HubToken {
        val file = hub.tokenFile ?: environment.getProperty(TOKEN_FILE_ENV)?.takeIf { it.isNotBlank() }?.let { Path.of(it) }
        val inline = hub.token ?: environment.getProperty(TOKEN_ENV)
        if (file != null && !inline.isNullOrBlank()) {
            log.info("Both a hub token and a hub token file are configured: using the token file {}", file)
        }
        return HubToken.of(inline, file)
    }

    companion object {
        const val CHANNEL_FACTORY = "krakenHubChannelFactory"
        const val TOKEN_ENV = "KRAKEN_HUB_TOKEN"
        const val TOKEN_FILE_ENV = "KRAKEN_HUB_TOKEN_FILE"
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
 * On stop it first announces an empty registration so the hubs stop routing to this instance, then ends every
 * subscription it serves with a clean `ReplyEnd` ([subscriptions], best effort: the hub treats it as that target's emitter
 * completing), waits up to [drainTimeout] for incoming handlers still running, then closes the stream, cancels [scope]
 * and closes [handlerDispatcher].
 */
class HubLifecycle(
    val connections: HubConnections,
    private val registry: HandlerRegistry,
    private val scope: CoroutineScope,
    private val handlerDispatcher: ExecutorCoroutineDispatcher? = null,
    private val drainTimeout: Duration = Duration.ofSeconds(5),
    private val subscriptions: SubscriptionRegistry? = null,
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
                // Subscribers are told now, not after a slow handler drained; the pause lets the ends leave before the stream closes.
                if (subscriptions != null && subscriptions.endAll() > 0) delay(SUBSCRIPTION_END_FLUSH.toMillis())
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

        /** How long shutdown waits after ending subscriptions, so the ends are written before the stream is closed. */
        private val SUBSCRIPTION_END_FLUSH: Duration = Duration.ofMillis(200)
    }
}
