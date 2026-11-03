package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.hub.HubAuthMetrics
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.DefaultHubChannelFactory
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubClientCertificate
import io.damoreira.kraken.messaging.hub.HubTls
import io.damoreira.kraken.messaging.hub.HubToken
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.HubOAuth2
import io.damoreira.kraken.messaging.hub.HubTokenProvider
import io.damoreira.kraken.messaging.hub.TokenEndpoint
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
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.env.Environment
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Executors
import java.net.URI
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

    /**
     * The client certificate of `hub.tls.client-cert-file` / `client-key-file` (mTLS, P5c), or null without them: read and checked here, so a
     * file that cannot be used, a key that does not match, a certificate that does not name this service, or a static token or OAuth2 next
     * to it fails the startup naming the settings. One instance, shared by the channel factory (which presents it) and the connections (which
     * roll over to a renewed pair).
     */
    @Bean
    @ConditionalOnMissingBean
    fun krakenHubClientCertificate(properties: KrakenMessagingProperties, environment: Environment): HubClientCertificate? {
        val tls = properties.hub.tls
        val certFile = tls.clientCertFile ?: return null
        val keyFile = checkNotNull(tls.clientKeyFile) { "kraken.messaging.hub.tls.client-key-file is not set" }
        rejectOtherCredentials(properties.hub, environment)
        return HubClientCertificate(certFile, keyFile, properties.serviceName)
    }

    @Bean(defaultCandidate = false)
    @ConditionalOnMissingBean(name = [CHANNEL_FACTORY])
    fun krakenHubChannelFactory(properties: KrakenMessagingProperties, certificate: ObjectProvider<HubClientCertificate>): HubChannelFactory {
        val tls = properties.hub.tls
        val clientCertificate = certificate.getIfAvailable()
        if (!tls.enabled && clientCertificate == null && (tls.caFile != null || tls.serverName != null)) {
            log.warn("kraken.messaging.hub.tls.ca-file / server-name are set but kraken.messaging.hub.tls.enabled is false: talking plain text to the hub")
        }
        // A bad CA file fails the startup here, with the setting's name, instead of failing every connection attempt.
        return DefaultHubChannelFactory(HubTls(tls.enabled, tls.caFile, tls.serverName, clientCertificate)).also { it.validate() }
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
        certificate: ObjectProvider<HubClientCertificate>,
    ): HubLifecycle {
        val hub = properties.hub
        // First: a client certificate next to a token or OAuth2 fails with that message, before any token file is read.
        certificate.getIfAvailable()
        // The connect loop, stream collector and heartbeats stay on Default; handlers get virtual threads (see HubConnection).
        val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default + CoroutineName("kraken-hub"))
        val token = hubToken(hub, environment)
        // One provider for every hub connection: N hubs, one access token (a failure here is a startup error, before any thread runs).
        val provider = hubOAuth2(properties, environment, token)?.let { HubTokenProvider(TokenEndpoint(it), scope) }
        val settings = HubSettings(
            hub.requestTimeout, hub.heartbeatInterval, hub.reconnectBackoff.min, hub.reconnectBackoff.max, hub.streamCreditTimeout, token, provider,
        )
        val handlerDispatcher = Executors.newVirtualThreadPerTaskExecutor().asCoroutineDispatcher()
        val connections = HubConnections(
            targets = HubTargets.parse(hub.address),
            channelFactory = channelFactory,
            serviceName = properties.serviceName,
            instanceId = UUID.randomUUID().toString(),
            settings = settings,
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

    /** Counters of the hub credential, when Micrometer's `MeterBinder` is on the classpath (Boot binds the bean to its registry). */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnClass(name = ["io.micrometer.core.instrument.binder.MeterBinder"])
    class HubAuthMetricsConfiguration {
        @Bean
        @ConditionalOnMissingBean
        fun krakenHubAuthMetrics(connections: HubConnections): HubAuthMetrics = HubAuthMetrics(connections)
    }

    private val log = LoggerFactory.getLogger(HubAutoConfiguration::class.java)

    /** A client certificate is the service's one credential: a static token or OAuth2 next to it would be refused by the hub (ADR 0020 ruling 6). */
    private fun rejectOtherCredentials(hub: KrakenMessagingProperties.Hub, environment: Environment) {
        fun env(name: String) = environment.getProperty(name)?.takeIf { it.isNotBlank() }
        val other = listOfNotNull(
            "kraken.messaging.hub.token".takeIf { !hub.token.isNullOrBlank() },
            "kraken.messaging.hub.token-file".takeIf { hub.tokenFile != null },
            TOKEN_ENV.takeIf { env(it) != null },
            TOKEN_FILE_ENV.takeIf { env(it) != null },
            "kraken.messaging.hub.oauth2.token-uri".takeIf { hub.oauth2.tokenUri != null },
            OAUTH2_TOKEN_URI_ENV.takeIf { env(it) != null },
        )
        if (other.isNotEmpty()) {
            throw IllegalStateException(
                "kraken.messaging.hub.tls.client-cert-file and ${other.joinToString(", ")} are both set: a service authenticates with one credential kind " +
                    "(a certificate, a static token or OAuth2), remove one",
            )
        }
    }

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

    /**
     * The Keycloak client-credentials settings (P5b): `hub.oauth2.*`, each falling back to the plain `KRAKEN_HUB_OAUTH2_*` variable
     * when its property is unset; null when no token URI is set. Checked here so a mistake fails the startup naming the setting:
     * a static token as well, an OAuth2 setting without a token URI, no secret, a secret file that cannot be read, a CA file that cannot be used, a URI that is not http(s).
     */
    private fun hubOAuth2(properties: KrakenMessagingProperties, environment: Environment, token: HubToken): HubOAuth2? {
        val o = properties.hub.oauth2
        fun env(name: String) = environment.getProperty(name)?.takeIf { it.isNotBlank() }
        val tokenUri = o.tokenUri ?: env(OAUTH2_TOKEN_URI_ENV)?.let {
            try { URI.create(it.trim()) } catch (e: IllegalArgumentException) { throw IllegalStateException("$OAUTH2_TOKEN_URI_ENV is not a URI: ${e.message}", e) }
        }
        val secret = o.clientSecret?.takeIf { it.isNotBlank() } ?: env(OAUTH2_CLIENT_SECRET_ENV)
        val secretFile = o.clientSecretFile ?: env(OAUTH2_CLIENT_SECRET_FILE_ENV)?.let { Path.of(it.trim()) }
        if (tokenUri == null) {
            // As the Rust library: a stray setting is a misconfigured service that would run unauthenticated, so it fails the startup.
            val stray = listOfNotNull(
                "kraken.messaging.hub.oauth2.client-id".takeIf { !o.clientId.isNullOrBlank() },
                "kraken.messaging.hub.oauth2.client-secret".takeIf { !o.clientSecret.isNullOrBlank() },
                "kraken.messaging.hub.oauth2.client-secret-file".takeIf { o.clientSecretFile != null },
                "kraken.messaging.hub.oauth2.scope".takeIf { !o.scope.isNullOrBlank() },
                "kraken.messaging.hub.oauth2.ca-file".takeIf { o.caFile != null },
                OAUTH2_CLIENT_ID_ENV.takeIf { env(it) != null },
                OAUTH2_CLIENT_SECRET_ENV.takeIf { env(it) != null },
                OAUTH2_CLIENT_SECRET_FILE_ENV.takeIf { env(it) != null },
                OAUTH2_SCOPE_ENV.takeIf { env(it) != null },
                OAUTH2_CA_ENV.takeIf { env(it) != null },
            )
            if (stray.isNotEmpty()) {
                throw IllegalStateException(
                    "${stray.joinToString(", ")} set but neither kraken.messaging.hub.oauth2.token-uri nor $OAUTH2_TOKEN_URI_ENV is: " +
                        "set the token URI to authenticate with a JWT, or remove the OAuth2 settings",
                )
            }
            return null
        }
        if (token !== HubToken.NONE) {
            throw IllegalStateException(
                "kraken.messaging.hub.oauth2.token-uri and a static hub token (kraken.messaging.hub.token / token-file, " +
                    "KRAKEN_HUB_TOKEN / KRAKEN_HUB_TOKEN_FILE) are both set: a service authenticates with one of them, remove one",
            )
        }
        if (secret == null && secretFile == null) {
            throw IllegalStateException("kraken.messaging.hub.oauth2.token-uri is set but neither kraken.messaging.hub.oauth2.client-secret nor client-secret-file is")
        }
        val source = HubOAuth2.secretSource(secret, secretFile)
        try {
            source() // an unreadable or empty secret file fails the startup, not every token fetch
        } catch (e: java.io.IOException) {
            throw IllegalStateException("kraken.messaging.hub.oauth2.client-secret-file: ${e.message}", e)
        }
        val oauth2 = try {
            HubOAuth2(tokenUri, o.clientId ?: env(OAUTH2_CLIENT_ID_ENV) ?: properties.serviceName, source, o.scope ?: env(OAUTH2_SCOPE_ENV),
                o.caFile ?: env(OAUTH2_CA_ENV)?.let { Path.of(it.trim()) })
        } catch (e: IllegalArgumentException) {
            throw IllegalStateException("kraken.messaging.hub.oauth2: ${e.message}", e)
        }
        if (tokenUri.scheme == "http" && !isLocalName(tokenUri.host)) {
            log.warn(
                "The OAuth2 token endpoint {} is plain http over a routed name: the client secret and the access token cross the network in the clear. Use https unless this is a trusted compose network",
                "http://${tokenUri.host}",
            )
        }
        return oauth2
    }

    /** Loopback or a single-label name (a compose service): no dot in it, or `localhost`, `127.x`, `::1`. */
    private fun isLocalName(host: String): Boolean =
        '.' !in host && ':' !in host || host == "localhost" || host.startsWith("127.") || host == "::1" || host == "[::1]"

    companion object {
        const val CHANNEL_FACTORY = "krakenHubChannelFactory"
        const val TOKEN_ENV = "KRAKEN_HUB_TOKEN"
        const val TOKEN_FILE_ENV = "KRAKEN_HUB_TOKEN_FILE"
        const val OAUTH2_TOKEN_URI_ENV = "KRAKEN_HUB_OAUTH2_TOKEN_URI"
        const val OAUTH2_CLIENT_ID_ENV = "KRAKEN_HUB_OAUTH2_CLIENT_ID"
        const val OAUTH2_CLIENT_SECRET_ENV = "KRAKEN_HUB_OAUTH2_CLIENT_SECRET"
        const val OAUTH2_CLIENT_SECRET_FILE_ENV = "KRAKEN_HUB_OAUTH2_CLIENT_SECRET_FILE"
        const val OAUTH2_SCOPE_ENV = "KRAKEN_HUB_OAUTH2_SCOPE"
        const val OAUTH2_CA_ENV = "KRAKEN_HUB_OAUTH2_CA"
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
                // Subscribers are told now, not after a slow handler drained; stopping the connections writes the ends before it closes the stream.
                subscriptions?.endAll()
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
