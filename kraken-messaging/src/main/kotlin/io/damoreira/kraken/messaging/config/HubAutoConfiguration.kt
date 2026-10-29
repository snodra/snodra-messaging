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
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import java.util.UUID

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
        return { ManagedChannelBuilder.forTarget(target).usePlaintext().build() }
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
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("kraken-hub"))
        val connection = HubConnection(
            channelFactory = channelFactory,
            serviceName = properties.serviceName,
            instanceId = UUID.randomUUID().toString(),
            settings = HubSettings(hub.requestTimeout, hub.heartbeatInterval, hub.reconnectBackoff.min, hub.reconnectBackoff.max),
            inbound = dispatcher::dispatch,
            scope = scope,
        )
        return HubLifecycle(connection, registry, scope)
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
 * the registry's command and query types and re-announcing them whenever handlers are added later; stops it and
 * cancels its [scope] on shutdown.
 */
class HubLifecycle(val connection: HubConnection, private val registry: HandlerRegistry, private val scope: CoroutineScope) : SmartLifecycle {
    @Volatile private var running = false

    @Synchronized
    override fun start() {
        if (running) return
        announce()
        registry.onChange(::announce)
        connection.start()
        running = true
    }

    @Synchronized
    override fun stop() {
        if (!running) return
        running = false
        runBlocking { connection.stop() }
        scope.cancel()
    }

    override fun isRunning(): Boolean = running
    override fun getPhase(): Int = Int.MAX_VALUE - 200

    private fun announce() = connection.register(registry.commandTypes(), registry.queryTypes())
}
