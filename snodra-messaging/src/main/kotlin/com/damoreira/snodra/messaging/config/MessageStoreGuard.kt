package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.EventBus
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.saga.Saga
import com.damoreira.snodra.messaging.saga.RawEventPublisher
import com.damoreira.snodra.messaging.store.MessageStore
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.beans.factory.ListableBeanFactory
import org.springframework.beans.factory.NoSuchBeanDefinitionException
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.SmartInitializingSingleton
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer
import org.springframework.boot.diagnostics.FailureAnalysis
import org.springframework.core.Ordered
import org.springframework.core.annotation.Order
import org.springframework.core.env.Environment
import org.springframework.core.io.ResourceLoader

/** What to do about a missing message store: the end of every message about one. */
internal const val ADD_A_MESSAGE_STORE =
    "add a message store add-on (snodra-messaging-mongo for MongoDB, snodra-messaging-jdbc for PostgreSQL or MySQL) to the dependencies, or declare a MessageStore bean " +
        "(snodra.messaging.store.type=memory runs the in-memory one, not durable: tests and local runs)"

/**
 * Fails the startup when the application's message store is missing but something needs one, or is ambiguous:
 *
 * - none, and there is an `@EventHandler`, a `@Saga` (unless `snodra.messaging.sagas.enabled` is false) or an
 *   `@IdempotentHandler`. Command, query and subscription-query handlers need none.
 * - two or more when `snodra.messaging.store.type` does not choose one;
 * - `snodra.messaging.store.type` names one the application does not have.
 *
 * Runs once every singleton exists, so the [registry] holds every handler.
 */
class MessageStoreGuard(
    private val beanFactory: ListableBeanFactory,
    private val registry: HandlerRegistry,
    private val environment: Environment,
    private val resourceLoader: ResourceLoader,
) : SmartInitializingSingleton {
    override fun afterSingletonsInstantiated() {
        val stores = beanFactory.getBeansOfType(MessageStore::class.java).values
        val chosen = environment.getProperty(STORE)?.takeIf { it.isNotBlank() }
        if (stores.size > 1) {
            throw IllegalStateException(
                "${stores.size} message stores are configured (${stores.joinToString { it.name }}): set $STORE to the one this service uses",
            )
        }
        val store = stores.firstOrNull()
        if (chosen != null && store?.name != chosen) {
            throw IllegalStateException(
                "$STORE is '$chosen', but " + (store?.let { "the message store is '${it.name}'" } ?: "no message store is configured") +
                    ": $ADD_A_MESSAGE_STORE",
            )
        }
        if (store != null) return
        val needs = buildList {
            registry.eventTypes().sorted().flatMap(registry::eventHandlers).forEach { add("@EventHandler ${it.label}") }
            if (environment.getProperty("snodra.messaging.sagas.enabled", Boolean::class.java, true)) {
                ApplicationClassScanning.sagaClasses(beanFactory, environment, resourceLoader)
                    .map { it.getAnnotation(Saga::class.java).name.ifBlank { it.name } }.sorted().forEach { add("@Saga $it") }
            }
            registry.idempotentHandlers().forEach { add("@IdempotentHandler ${it.label}") }
        }
        if (needs.isEmpty()) return
        throw IllegalStateException(needs.distinct().joinToString("; ") { "$it needs a message store" } + ": " + ADD_A_MESSAGE_STORE)
    }

    companion object {
        /** The message store's name, when two are available or to run the in-memory one. */
        const val STORE = "snodra.messaging.store.type"
    }
}

/**
 * Fails the startup, before any bean is created, when two message stores are defined by auto-configurations (two store
 * add-ons) and `snodra.messaging.store.type` does not choose one: every add-on's store steps back for the application's own
 * store ([ConditionalOnNoApplicationMessageStore]), never for another add-on's, so without this check one of them would win
 * silently. Stores the application defines itself are [MessageStoreGuard]'s.
 */
class MessageStoreAddOnsCheck(private val environment: Environment) : BeanFactoryPostProcessor {
    override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
        if (!environment.getProperty(MessageStoreGuard.STORE).isNullOrBlank()) return
        val addOns = MessageStoreBeans.names(beanFactory).filter { MessageStoreBeans.autoConfigured(beanFactory, it) }
            .mapNotNull { name -> beanFactory.getType(name, false)?.let { name to it } }
        if (addOns.size < 2) return
        throw IllegalStateException(
            "${addOns.size} message store add-ons are configured (${addOns.joinToString { (_, type) -> "${type.simpleName}: ${addOnName(type)}" }}): " +
                "set ${MessageStoreGuard.STORE} to the one this service uses (${addOns.joinToString(", ") { addOnName(it.second) }}), or remove the " +
                "others from the dependencies",
        )
    }
}

/** What to do about a missing event store: the end of every message about one. */
internal const val ADD_AN_EVENT_STORE =
    "add an event store add-on (snodra-messaging-kafka for Kafka, snodra-messaging-rabbitmq for RabbitMQ Streams) to the dependencies, or declare an EventStore bean " +
        "(snodra.messaging.event-store=memory runs the in-memory one, not durable: tests and local runs)"

/**
 * Fails the startup when the application's event store is missing but something needs one, or is ambiguous:
 *
 * - none, and there is an `@EventHandler` or a `@Saga` (unless `snodra.messaging.sagas.enabled` is false): they read events.
 *   Command, query and subscription-query handlers need none. (Without a message store either, [MessageStoreGuard] reports
 *   that first.)
 * - two or more (two event-store add-ons, each providing its own) when `snodra.messaging.event-store` does not choose one;
 * - `snodra.messaging.event-store` names one the application does not have.
 *
 * Runs once every singleton exists, so the [registry] holds every handler.
 */
class EventStoreGuard(
    private val beanFactory: ListableBeanFactory,
    private val registry: HandlerRegistry,
    private val environment: Environment,
    private val resourceLoader: ResourceLoader,
) : SmartInitializingSingleton {
    override fun afterSingletonsInstantiated() {
        val stores = beanFactory.getBeansOfType(EventStore::class.java).values
        val chosen = environment.getProperty(EVENT_STORE)?.takeIf { it.isNotBlank() }
        if (stores.size > 1) {
            throw IllegalStateException(
                "${stores.size} event stores are configured (${stores.joinToString { it.name }}): set $EVENT_STORE to the one this service uses",
            )
        }
        val store = stores.firstOrNull()
        if (chosen != null && store?.name != chosen) {
            throw IllegalStateException(
                "$EVENT_STORE is '$chosen', but " + (store?.let { "the event store is '${it.name}'" } ?: "no event store is configured") +
                    ": $ADD_AN_EVENT_STORE",
            )
        }
        if (store != null) return
        if (!beanFactory.getBeanProvider(MessageStore::class.java).iterator().hasNext()) return // MessageStoreGuard reports first
        val needs = buildList {
            registry.eventTypes().sorted().flatMap(registry::eventHandlers).forEach { add("@EventHandler ${it.label}") }
            if (environment.getProperty("snodra.messaging.sagas.enabled", Boolean::class.java, true)) {
                ApplicationClassScanning.sagaClasses(beanFactory, environment, resourceLoader)
                    .map { it.getAnnotation(Saga::class.java).name.ifBlank { it.name } }.sorted().forEach { add("@Saga $it") }
            }
        }
        if (needs.isEmpty()) return
        throw IllegalStateException(needs.distinct().joinToString("; ") { "$it needs an event store" } + ": " + ADD_AN_EVENT_STORE)
    }

    companion object {
        /** The event store's name, when two add-ons are on the classpath. */
        const val EVENT_STORE = "snodra.messaging.event-store"
    }
}

/**
 * Applies the message store's schema at startup ([MessageStore.initSchema]: its indexes or tables), or, when
 * `snodra.messaging.store.init-schema` is false (the service applies it itself), checks it is there ([MessageStore.checkSchema]),
 * whichever store the application has (an add-on's or its own). Runs once every singleton exists (after any bean that migrates
 * the schema while it is created) and before any lifecycle (the outbox relay, the consumers) starts.
 */
class MessageStoreSchema(private val store: ObjectProvider<MessageStore>, private val properties: SnodraMessagingProperties) : SmartInitializingSingleton {
    override fun afterSingletonsInstantiated() {
        store.ifAvailable { if (properties.store.initSchema) it.initSchema() else it.checkSchema() }
    }
}

/**
 * Explains a startup that failed because a bean needs an [EventBus] (or the saga relay's [RawEventPublisher]) and there is
 * none: without a message store there is no outbox, and without an event store nothing relays it, so no
 * event bus.
 */
@Order(Ordered.HIGHEST_PRECEDENCE) // before Boot's generic NoSuchBeanDefinitionException analyzer, which would explain it first
class MissingMessageStoreFailureAnalyzer : AbstractFailureAnalyzer<NoSuchBeanDefinitionException>() {
    override fun analyze(rootFailure: Throwable, cause: NoSuchBeanDefinitionException): FailureAnalysis? {
        val type = cause.beanType?.takeIf { it == EventBus::class.java || it == RawEventPublisher::class.java } ?: return null
        return FailureAnalysis(
            "A bean needs an ${type.simpleName}, and the ${type.simpleName} needs a message store and an event store: events are " +
                "written to the outbox of the service's own database (a MessageStore) and relayed to an EventStore, and this " +
                "application lacks one of them.",
            "To publish events, $ADD_A_MESSAGE_STORE; and $ADD_AN_EVENT_STORE. A service with only command and query handlers " +
                "needs neither and no ${type.simpleName}.",
            cause,
        )
    }
}
