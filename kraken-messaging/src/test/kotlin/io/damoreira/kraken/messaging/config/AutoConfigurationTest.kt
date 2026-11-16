package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.QueryUpdateEmitter
import io.damoreira.kraken.messaging.SubscriptionOptions
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.control.ProcessorControl
import io.damoreira.kraken.messaging.deadletter.DeadLetters
import io.damoreira.kraken.messaging.events.EventConsumer
import io.damoreira.kraken.messaging.events.EventProcessors
import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.damoreira.kraken.messaging.events.OutboxEventBus
import io.damoreira.kraken.messaging.events.OutboxPublisher
import io.damoreira.kraken.messaging.events.ProcessorStatusSource
import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.fixtures.Balance
import io.damoreira.kraken.messaging.fixtures.BalanceChanged
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.GetCount
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.fixtures.TestJwt
import io.damoreira.kraken.messaging.fixtures.WatchAccount
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.hub.AccountWatcher
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.hub.HubChannelFactory
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.HubQueryUpdateEmitter
import io.damoreira.kraken.messaging.hub.SubscriptionRegistry
import io.damoreira.kraken.messaging.kafka.KafkaEventStore
import io.damoreira.kraken.messaging.kafka.KafkaEventStoreAutoConfiguration
import io.damoreira.kraken.messaging.security.ForwardedIdentityResolver
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.test.InProcessHub
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.mockk.every
import io.mockk.mockk
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.until
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.context.SmartLifecycle
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.security.oauth2.jwt.JwtDecoder
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class PingHandlers {
    @CommandHandler fun handle(command: Ping): Pong = Pong(command.text, Instant.EPOCH)
}

@Configuration(proxyBeanMethods = false)
class HandlersConfig {
    @Bean fun pingHandlers() = PingHandlers()
}

class SharedCountHandlers {
    @QueryHandler(shared = true) fun handle(query: GetCount): Count = Count(1)
}

@Configuration(proxyBeanMethods = false)
class AccountWatcherConfig {
    @Bean fun accountWatcher() = AccountWatcher()
}

@Configuration(proxyBeanMethods = false)
class SharedHandlersConfig {
    @Bean fun sharedCountHandlers() = SharedCountHandlers()
}

@Configuration(proxyBeanMethods = false)
class JwtDecoderConfig {
    @Bean fun jwtDecoder(): JwtDecoder = TestJwt.decoder()
}

class UserCommandBus : CommandBus {
    override suspend fun <R> send(command: Command<R>, routingKey: String?): R = throw UnsupportedOperationException("user bus")
}

@Configuration(proxyBeanMethods = false)
class UserCommandBusConfig {
    @Bean fun userCommandBus(): CommandBus = UserCommandBus()
}

/** Autowires both facades the way application code does. */
@org.springframework.stereotype.Component
class BusClient(val commandBus: CommandBus, val queryBus: QueryBus)

/** A message store (ADR 0028) that answers everything with nothing: what the events configuration needs to exist. */
@Configuration(proxyBeanMethods = false)
class MessageStoreMockConfig {
    @Bean fun messageStore(): MessageStore = mockk(relaxed = true)
}

/** A sender-only client of [hub], for what an application context under test is the other end of. */
class HubConnectionsClient(hub: InProcessHub) {
    private val scope = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.SupervisorJob() + Dispatchers.Default)
    private val resolver = MessageTypeResolver("test")
    private val codec = JsonCodec()
    private val registry = HandlerRegistry(resolver)
    private val connection = io.damoreira.kraken.messaging.hub.HubConnection(
        channelFactory = { hub.channel() },
        serviceName = "subscriber",
        instanceId = "subscriber-1",
        settings = io.damoreira.kraken.messaging.hub.HubSettings(null, Duration.ofSeconds(10), Duration.ofMillis(20), Duration.ofMillis(200)),
        inbound = { req -> io.damoreira.kraken.hub.v1.Reply.newBuilder().setRequestId(req.id).build() },
        scope = scope,
    ).also {
        it.start()
        runBlocking { check(it.awaitRegistered(Duration.ofSeconds(5))) }
    }
    val bus = HubBus(connection, registry, HandlerInvoker(registry, codec), codec, resolver, true, TraceHeaders(null, null), null, emptyMap(), null)

    fun close() {
        runBlocking { connection.stop() }
        scope.cancel()
    }
}

class AutoConfigurationTest {
    private val hub = InProcessHub().start()

    @AfterEach fun tearDown() {
        subscriptionHub?.close()
        hub.close()
    }

    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                EventsAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues(
            "kraken.messaging.service-name=orders",
            "kraken.messaging.type-prefix=test",
            "kraken.messaging.hub.enabled=false",
            "kraken.messaging.events.consumer.enabled=false",
            "kraken.messaging.outbox.enabled=false",
        )

    /** Hub enabled and pointed at the in-process hub through the channel factory override. */
    private val inProcessChannel = HubChannelFactory { hub.channel() }

    private fun withHub() = runner
        .withPropertyValues("kraken.messaging.hub.enabled=true")
        .withBean("krakenHubChannelFactory", HubChannelFactory::class.java, { inProcessChannel })

    @Test fun `core beans present`() {
        runner.run { context ->
            assertEquals(null, context.startupFailure)
            assertEquals(1, context.getBeansOfType(HandlerRegistry::class.java).size)
            assertEquals(1, context.getBeansOfType(HandlerInvoker::class.java).size)
            assertEquals(1, context.getBeansOfType(JsonCodec::class.java).size)
            assertEquals(1, context.getBeansOfType(ForwardedIdentityResolver::class.java).size)
            assertEquals(1, context.getBeansOfType(MessageTypeResolver::class.java).size)
            assertEquals(1, context.getBeansOfType(TraceHeaders::class.java).size)
            // The emitter does not need the hub: emitting with no subscription is a no-op, so a service starts in any mode.
            assertIs<HubQueryUpdateEmitter>(context.getBean(QueryUpdateEmitter::class.java))
            assertEquals(1024, context.getBean(SubscriptionRegistry::class.java).maxSubscriptions)
            // no message store in this context: the events configuration backs off
            assertTrue(context.getBeansOfType(EventBus::class.java).isEmpty())
        }
    }

    @Test fun `user CommandBus bean overrides HubBus`() {
        withHub().withUserConfiguration(UserCommandBusConfig::class.java, BusClient::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            assertIs<UserCommandBus>(context.getBean(CommandBus::class.java))
            assertTrue(context.containsBean("queryBus"))
            assertTrue(context.getBean(QueryBus::class.java) !is CommandBus)
            assertIs<UserCommandBus>(context.getBean(BusClient::class.java).commandBus)
        }
        withHub().withUserConfiguration(BusClient::class.java).run { context ->
            assertEquals(setOf("commandBus"), context.getBeansOfType(CommandBus::class.java).keys - "hubBus")
            assertEquals(setOf("queryBus"), context.getBeansOfType(QueryBus::class.java).keys - "hubBus")
            assertTrue(context.getBean(BusClient::class.java).commandBus !is UserCommandBus)
        }
    }

    @Test fun `hub disabled leaves no HubConnections`() {
        runner.run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(context.getBeansOfType(HubConnections::class.java).isEmpty())
            assertTrue(context.getBeansOfType(HubBus::class.java).isEmpty())
            assertTrue(context.getBeansOfType(CommandBus::class.java).isEmpty())
            assertTrue(context.getBeansOfType(QueryBus::class.java).isEmpty())
        }
    }

    @Test fun `handlers in user beans are registered`() {
        runner.withUserConfiguration(HandlersConfig::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue("test.Ping" in context.getBean(HandlerRegistry::class.java).commandTypes())
        }
    }

    @Test fun `hub lifecycle registers handler types with the hub and stops on close`() {
        withHub().withUserConfiguration(HandlersConfig::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            val connection = context.getBean(HubConnections::class.java)
            await atMost Duration.ofSeconds(5) until { connection.connected.value && "test.Ping" in hub.registeredTypes() }
            val registrations = hub.registrationsFrom(connection.instanceId)
            context.close()
            await atMost Duration.ofSeconds(5) until { hub.connectedInstances().isEmpty() }
            // stop() announced an empty registration (no more routing) before closing the stream
            assertEquals(registrations + 1, hub.registrationsFrom(connection.instanceId))
        }
    }

    @Test fun `the lifecycle announces shared query handlers as shared query types`() {
        withHub().withUserConfiguration(HandlersConfig::class.java, SharedHandlersConfig::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            val connection = context.getBean(HubConnections::class.java)
            await atMost Duration.ofSeconds(5) until { connection.connected.value && "test.GetCount" in hub.registeredTypes() }
            val register = hub.lastRegisterFrom(connection.instanceId)!!
            assertEquals(listOf("test.Ping"), register.commandTypesList)
            assertEquals(emptyList(), register.queryTypesList)
            assertEquals(listOf("test.GetCount"), register.sharedQueryTypesList)
        }
    }

    @Test fun `max-subscriptions sets the registry limit, and a user QueryUpdateEmitter bean replaces ours`() {
        runner.withPropertyValues("kraken.messaging.hub.max-subscriptions=7").run { context ->
            assertEquals(7, context.getBean(SubscriptionRegistry::class.java).maxSubscriptions)
        }
        val mine = object : QueryUpdateEmitter {
            override fun <Q : io.damoreira.kraken.messaging.api.SubscriptionQuery<*, U>, U : Any> emit(queryType: kotlin.reflect.KClass<Q>, filter: (Q) -> Boolean, update: U) = Unit
            override fun <Q : io.damoreira.kraken.messaging.api.SubscriptionQuery<*, *>> complete(queryType: kotlin.reflect.KClass<Q>, filter: (Q) -> Boolean) = Unit
            override fun <Q : io.damoreira.kraken.messaging.api.SubscriptionQuery<*, *>> completeExceptionally(queryType: kotlin.reflect.KClass<Q>, filter: (Q) -> Boolean, cause: Throwable) = Unit
        }
        runner.withBean(QueryUpdateEmitter::class.java, { mine }).run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(context.getBean(QueryUpdateEmitter::class.java) === mine)
        }
    }

    @Test fun `the lifecycle serves subscriptions, the emitter pushes updates, and closing the context ends them with a clean end`() = runBlocking {
        val connection = InProcessHub().start().also { subscriptionHub = it }
        val owner = ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(KrakenMessagingAutoConfiguration::class.java, HubAutoConfiguration::class.java, EventsAutoConfiguration::class.java))
            .withPropertyValues(
                "kraken.messaging.service-name=accounts", "kraken.messaging.type-prefix=test", "kraken.messaging.hub.enabled=true",
                "kraken.messaging.events.consumer.enabled=false", "kraken.messaging.outbox.enabled=false",
            )
            .withBean("krakenHubChannelFactory", HubChannelFactory::class.java, { HubChannelFactory { connection.channel() } })
            .withUserConfiguration(AccountWatcherConfig::class.java)
        owner.run { context ->
            assertEquals(null, context.startupFailure)
            val connections = context.getBean(HubConnections::class.java)
            await atMost Duration.ofSeconds(5) until { connections.connected.value && "test.WatchAccount" in connection.registeredTypes() }

            // The QueryBus bean applications inject is a facade over HubBus: it forwards subscribe.
            val facade = runBlocking { context.getBean(QueryBus::class.java).subscribe(WatchAccount(8), SubscriptionOptions(timeout = Duration.ofSeconds(5))) }
            assertEquals(Balance(108), facade.initial)
            facade.cancel()
            await atMost Duration.ofSeconds(5) until { context.getBean(SubscriptionRegistry::class.java).size == 0 }

            val subscriber = HubConnectionsClient(connection)
            val result = runBlocking { subscriber.bus.subscribe(WatchAccount(7), SubscriptionOptions(timeout = Duration.ofSeconds(5))) }
            assertEquals(Balance(107), result.initial)
            val registry = context.getBean(SubscriptionRegistry::class.java)
            await atMost Duration.ofSeconds(5) until { registry.size == 1 }
            val emitter = context.getBean(QueryUpdateEmitter::class.java)
            emitter.emit(WatchAccount::class, { true }, BalanceChanged(7, 1))
            val updates = kotlinx.coroutines.CoroutineScope(Dispatchers.Default).async { result.updates.toList() }
            await atMost Duration.ofSeconds(5) until { connection.openSubscriptions() == 1 }
            context.close() // the lifecycle ends the subscription before it closes the stream
            val seen = runBlocking { kotlinx.coroutines.withTimeout(5_000) { updates.await() } }
            assertEquals(listOf(BalanceChanged(7, 1)), seen) // completed normally: a clean ReplyEnd, not HANDLER_DISCONNECTED
            subscriber.close()
        }
        Unit // a test method must return nothing: JUnit skips one that returns a value (run() returns the runner)
    }

    private var subscriptionHub: InProcessHub? = null

    @Test fun `lifecycle phases stop after the web server drain`() {
        val gracefulShutdown = SmartLifecycle.DEFAULT_PHASE - 1024 // WebServerApplicationContext.GRACEFUL_SHUTDOWN_PHASE
        val webServerStartStop = gracefulShutdown - 1024 // WebServerApplicationContext.START_STOP_LIFECYCLE_PHASE
        assertEquals(SmartLifecycle.DEFAULT_PHASE - 1900, EventConsumer.PHASE)
        assertEquals(SmartLifecycle.DEFAULT_PHASE - 1950, OutboxPublisher.PHASE)
        assertTrue(HubLifecycle.PHASE < webServerStartStop) // starts before the server, stops after it
        // stop order (highest phase first): drain, consumer, publisher, web server, hub
        assertTrue(gracefulShutdown > EventConsumer.PHASE && EventConsumer.PHASE > OutboxPublisher.PHASE)
        withHub().run { context ->
            assertEquals(HubLifecycle.PHASE, context.getBean(HubLifecycle::class.java).phase)
        }
    }

    @Test fun `events wiring with a message store and the Kafka event store add-on`() {
        runner.withUserConfiguration(MessageStoreMockConfig::class.java)
            .withConfiguration(AutoConfigurations.of(TransactionAutoConfiguration::class.java, KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9")
            .run { context ->
            assertEquals(null, context.startupFailure)
            assertIs<KafkaEventStore>(context.getBean(EventStore::class.java))
            assertIs<OutboxEventBus>(context.getBean(EventBus::class.java))
            assertTrue(context.getBeansOfType(OutboxPublisher::class.java).isEmpty()) // outbox.enabled=false
            assertTrue(context.getBeansOfType(EventProcessors::class.java).isEmpty()) // events.consumer.enabled=false
            assertTrue(context.getBeansOfType(ProcessorControl::class.java).isEmpty())
            // The admin API exists without the consumer (an admin-only instance), in mode collection by default.
            assertNotNull(context.getBean(DeadLetters::class.java))
        }
    }

    @Test fun `without an event store there is no event bus, relay or processor, but the dead letters and the dispatcher stay`() {
        runner.withUserConfiguration(MessageStoreMockConfig::class.java)
            .withConfiguration(AutoConfigurations.of(TransactionAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.events.consumer.enabled=true", "kraken.messaging.outbox.enabled=true")
            .run { context ->
                assertEquals(null, context.startupFailure, "commands and queries need no event store")
                assertTrue(context.getBeansOfType(EventBus::class.java).isEmpty())
                assertTrue(context.getBeansOfType(OutboxPublisher::class.java).isEmpty())
                assertTrue(context.getBeansOfType(EventProcessors::class.java).isEmpty())
                assertNotNull(context.getBean(DeadLetters::class.java))
                assertNotNull(context.getBean(LocalEventDispatcher::class.java))
            }
    }

    @Test fun `dead-letter mode event-store needs the capability native-dead-letter`() {
        val plain = mockk<EventStore>(relaxed = true)
        every { plain.name } returns "plain"
        every { plain.capabilities } returns setOf(Capability.REPLAY)
        every { plain.supports(any()) } answers { firstArg<Capability>() in setOf(Capability.REPLAY) }
        runner.withUserConfiguration(MessageStoreMockConfig::class.java)
            .withConfiguration(AutoConfigurations.of(TransactionAutoConfiguration::class.java))
            .withBean(EventStore::class.java, { plain })
            .withPropertyValues("kraken.messaging.events.consumer.enabled=true", "kraken.messaging.events.dead-letter.mode=event-store")
            .run { context ->
                val message = generateSequence(context.startupFailure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
                assertTrue(message.contains("the dead-letter mode event-store needs an event store with the capability native-dead-letter: plain lacks it"), message)
            }
    }

    @Test fun `event processors, their control and dead letters are wired, and the hub reads their status (P6a, P6b)`() {
        runner.withUserConfiguration(MessageStoreMockConfig::class.java)
            .withConfiguration(AutoConfigurations.of(TransactionAutoConfiguration::class.java, KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9", "kraken.messaging.events.consumer.enabled=true")
            .run { context ->
                assertEquals(null, context.startupFailure)
                val processors = context.getBean(EventProcessors::class.java)
                assertTrue(processors.isRunning)
                assertEquals(emptyList(), processors.consumers(), "no @EventHandler and no saga: no processor starts")
                assertEquals(emptyList(), context.getBean(ProcessorStatusSource::class.java).processorStatuses())
                // P6b: the processors' control, which the hub connection asks per control request.
                assertNotNull(context.getBean(ProcessorControl::class.java))
            }
        runner.withUserConfiguration(MessageStoreMockConfig::class.java)
            .withConfiguration(AutoConfigurations.of(TransactionAutoConfiguration::class.java, KafkaEventStoreAutoConfiguration::class.java))
            .withPropertyValues("kraken.messaging.kafka.bootstrap-servers=localhost:9", "kraken.messaging.events.dead-letter.mode=log")
            .run { context ->
                assertEquals(null, context.startupFailure)
                val e = kotlin.runCatching { context.getBean(DeadLetters::class.java).count() }.exceptionOrNull()
                assertIs<IllegalStateException>(e)
                assertTrue(e.message!!.contains("mode log"), e.message)
            }
    }

    @Test fun `starts without JwtDecoder`() {
        runner.run { context ->
            assertNotNull(context.getBean(ForwardedIdentityResolver::class.java))
        }
    }

    @Test fun `require-jwt property fails startup with removal hint`() {
        runner.withPropertyValues("kraken.messaging.security.require-jwt=true").run { context ->
            assertTrue(context.startupFailure != null, "expected startup failure")
            val message = generateSequence(context.startupFailure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
            assertTrue(message.contains("kraken.messaging.security.require-jwt was removed"), message)
        }
        runner
            .withInitializer { ctx ->
                ctx.environment.propertySources.addFirst(
                    org.springframework.core.env.SystemEnvironmentPropertySource(
                        "test-systemEnvironment",
                        mapOf("KRAKEN_MESSAGING_SECURITY_REQUIRE_JWT" to "false"),
                    ),
                )
            }
            .run { context ->
                assertTrue(context.startupFailure != null, "expected startup failure from the env-style property")
                val message = generateSequence(context.startupFailure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
                assertTrue(message.contains("kraken.messaging.security.require-jwt was removed"), message)
            }
    }

    @Test fun `every removed kafka setting still set fails the startup in one message naming each replacement`() {
        runner
            .withPropertyValues(
                "kraken.messaging.kafka.topic=orders.events",
                "kraken.messaging.kafka.group-id=orders",
                "kraken.messaging.kafka.consumer.enabled=false",
                "kraken.messaging.kafka.consumer.max-attempts=3",
                "kraken.messaging.kafka.dead-letter.mode=log",
                "kraken.messaging.kafka.dead-letter.retention=1d",
            )
            .run { context ->
                val message = startupFailureMessage(context.startupFailure)
                listOf(
                    "kraken.messaging.kafka.topic" to "kraken.messaging.events.source",
                    "kraken.messaging.kafka.group-id" to "kraken.messaging.events.subscription-prefix",
                    "kraken.messaging.kafka.consumer.enabled" to "kraken.messaging.events.consumer.enabled",
                    "kraken.messaging.kafka.consumer.max-attempts" to "kraken.messaging.events.consumer.max-attempts",
                    "kraken.messaging.kafka.dead-letter.mode" to "kraken.messaging.events.dead-letter.mode",
                    "kraken.messaging.kafka.dead-letter.retention" to "kraken.messaging.events.dead-letter.retention",
                ).forEach { (removed, replacement) -> assertTrue(message.contains("$removed was removed, use $replacement"), message) }
            }
    }

    @Test fun `a removed setting in its environment form fails the startup`() {
        runner
            .withInitializer { ctx ->
                ctx.environment.propertySources.addFirst(
                    org.springframework.core.env.SystemEnvironmentPropertySource(
                        "test-systemEnvironment",
                        mapOf("KRAKEN_MESSAGING_KAFKA_GROUPID" to "orders"),
                    ),
                )
            }
            .run { context ->
                val message = startupFailureMessage(context.startupFailure)
                assertTrue(message.contains("kraken.messaging.kafka.group-id was removed, use kraken.messaging.events.subscription-prefix"), message)
            }
    }

    @Test fun `the removed dead-letter mode topic fails the startup naming event-store`() {
        runner.withPropertyValues("kraken.messaging.events.dead-letter.mode=topic").run { context ->
            val message = startupFailureMessage(context.startupFailure)
            assertTrue(message.contains("kraken.messaging.events.dead-letter.mode=topic was removed, use event-store"), message)
        }
        runner.withPropertyValues("kraken.messaging.kafka.dead-letter.mode=topic").run { context ->
            val message = startupFailureMessage(context.startupFailure)
            assertTrue(message.contains("kraken.messaging.kafka.dead-letter.mode was removed, use kraken.messaging.events.dead-letter.mode"), message)
            assertTrue(message.contains("event-store"), message)
        }
    }

    @Test fun `removed settings and require-jwt are reported together`() {
        runner.withPropertyValues("kraken.messaging.security.require-jwt=true", "kraken.messaging.kafka.topic=t").run { context ->
            val message = startupFailureMessage(context.startupFailure)
            assertTrue(message.contains("kraken.messaging.security.require-jwt was removed"), message)
            assertTrue(message.contains("kraken.messaging.kafka.topic was removed"), message)
        }
    }

    private fun startupFailureMessage(failure: Throwable?): String {
        assertNotNull(failure, "expected startup failure")
        return generateSequence(failure) { it.cause }.joinToString(" | ") { it.message.orEmpty() }
    }

    @Test fun `require-jwt check also runs with a user supplied identity resolver bean`() {
        runner
            .withPropertyValues("kraken.messaging.security.require-jwt=true")
            .withBean(ForwardedIdentityResolver::class.java, { ForwardedIdentityResolver(null) })
            .run { context -> assertTrue(context.startupFailure != null, "expected startup failure despite the user bean") }
    }
}
