package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubConnection
import io.damoreira.kraken.messaging.hub.HubSettings
import io.damoreira.kraken.messaging.hub.InboundRequestDispatcher
import io.damoreira.kraken.messaging.security.ForwardedJwtAuthenticator
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.grpc.ManagedChannel
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
 * The hub transport: one [HubConnection] per instance, started by [HubLifecycle] once the context is refreshed, and
 * [HubBus] as the default [CommandBus] and [QueryBus]. Off with `kraken.messaging.hub.enabled=false`.
 *
 * The channel is built by the `krakenHubChannelFactory` bean (`() -> ManagedChannel`), called once per connection
 * attempt; define a bean with that name to override it (tests point it at an in-process hub).
 */
@AutoConfiguration(after = [KrakenMessagingAutoConfiguration::class])
@ConditionalOnProperty(prefix = "kraken.messaging.hub", name = ["enabled"], havingValue = "true", matchIfMissing = true)
class HubAutoConfiguration {

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean(name = [CHANNEL_FACTORY])
    fun krakenHubChannelFactory(properties: KrakenMessagingProperties): () -> ManagedChannel {
        val target = properties.hub.address.removePrefix("static://")
        return {
            ManagedChannelBuilder.forTarget(target).usePlaintext()
                // HTTP/2 pings detect a dead peer at the transport level even while the stream is idle.
                .keepAliveTime(20, TimeUnit.SECONDS)
                .keepAliveTimeout(10, TimeUnit.SECONDS)
                .keepAliveWithoutCalls(true)
                .build()
        }
    }

    @Bean
    @ConditionalOnMissingBean
    fun inboundRequestDispatcher(invoker: HandlerInvoker, authenticator: ForwardedJwtAuthenticator, codec: JsonCodec) =
        InboundRequestDispatcher(invoker, authenticator, codec)

    @Bean
    @ConditionalOnMissingBean
    fun hubLifecycle(
        properties: KrakenMessagingProperties,
        @Qualifier(CHANNEL_FACTORY) channelFactory: () -> ManagedChannel,
        dispatcher: InboundRequestDispatcher,
        registry: HandlerRegistry,
    ): HubLifecycle {
        val hub = properties.hub
        // The connect loop, stream collector and heartbeats stay on Default; handlers get virtual threads (see HubConnection).
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("kraken-hub"))
        val handlerDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
        val connection = HubConnection(
            channelFactory = channelFactory,
            serviceName = properties.serviceName,
            instanceId = UUID.randomUUID().toString(),
            settings = HubSettings(hub.requestTimeout, hub.heartbeatInterval, hub.reconnectBackoff.min, hub.reconnectBackoff.max),
            inbound = dispatcher::dispatch,
            scope = scope,
            handlerDispatcher = handlerDispatcher,
            maxConcurrentHandlers = hub.maxConcurrentHandlers,
        )
        return HubLifecycle(connection, registry, scope, handlerDispatcher)
    }

    @Bean
    fun hubConnection(lifecycle: HubLifecycle): HubConnection = lifecycle.connection

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean
    fun hubBus(
        connection: HubConnection,
        registry: HandlerRegistry,
        invoker: HandlerInvoker,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        traceHeaders: TraceHeaders,
        properties: KrakenMessagingProperties,
    ) = HubBus(
        connection, registry, invoker, codec, typeResolver,
        forwardJwt = properties.security.forwardJwt,
        traceHeaders = traceHeaders,
        requestTimeout = properties.hub.requestTimeout,
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
 * Starts the [connection] after every singleton is initialised (so every handler bean is in the registry), announcing
 * the registry's command and query types and re-announcing them whenever handlers are added later.
 *
 * Phase [PHASE] is below the web server's start/stop phase (`WebServerApplicationContext.START_STOP_LIFECYCLE_PHASE`,
 * `DEFAULT_PHASE - 2048` in Boot 4.1), so the hub connection starts before the server accepts traffic and stops only
 * after the graceful HTTP drain (`GRACEFUL_SHUTDOWN_PHASE`, `DEFAULT_PHASE - 1024`) and the server stop.
 *
 * On stop it first announces an empty registration so the hub stops routing to this instance, waits up to
 * [drainTimeout] for incoming handlers still running, then closes the stream, cancels [scope] and closes
 * [handlerDispatcher].
 */
class HubLifecycle(
    val connection: HubConnection,
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
        connection.start()
    }

    @Synchronized
    override fun stop() {
        if (!running) return
        running = false
        try {
            runBlocking {
                connection.register(emptySet(), emptySet())
                val deregistered = runCatching { connection.awaitRegistered(Duration.ofSeconds(1)) }.getOrDefault(false)
                if (!deregistered) log.debug("Hub did not confirm the empty registration before shutdown")
                if (!connection.awaitHandlersIdle(drainTimeout)) {
                    log.warn("{} incoming hub request(s) still running after {}; stopping anyway", connection.inFlightHandlers.value, drainTimeout)
                }
                connection.stop()
            }
        } finally {
            scope.cancel()
            handlerDispatcher?.close()
        }
    }

    override fun isRunning(): Boolean = running
    override fun getPhase(): Int = PHASE

    private fun announce() {
        if (running) connection.register(registry.commandTypes(), registry.queryTypes())
    }

    companion object {
        /** `WebServerApplicationContext.START_STOP_LIFECYCLE_PHASE - 1` (the class is not on the starter's classpath). */
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 2049
    }
}
