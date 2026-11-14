package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.RawEventPublisher
import io.damoreira.kraken.messaging.store.MessageStore
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
    "add a message store add-on (kraken-messaging-mongo for MongoDB) to the dependencies, or declare a MessageStore bean " +
        "(kraken.messaging.store.type=memory runs the in-memory one, not durable: tests and local runs)"

/**
 * Fails the startup when the application's message store (ADR 0028) is missing but something needs one, or is ambiguous:
 *
 * - none, and there is an `@EventHandler`, a `@Saga` (unless `kraken.messaging.sagas.enabled` is false) or an
 *   `@IdempotentHandler`. Command, query and subscription-query handlers need none.
 * - two or more when `kraken.messaging.store.type` does not choose one;
 * - `kraken.messaging.store.type` names one the application does not have.
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
            if (environment.getProperty("kraken.messaging.sagas.enabled", Boolean::class.java, true)) {
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
        const val STORE = "kraken.messaging.store.type"
    }
}

/** What to do about a missing event store: the end of every message about one. */
internal const val ADD_AN_EVENT_STORE =
    "add an event store add-on (kraken-messaging-kafka for Kafka) to the dependencies, or declare an EventStore bean " +
        "(kraken.messaging.event-store=memory runs the in-memory one, not durable: tests and local runs)"

/**
 * Fails the startup when the application's event store (ADR 0027) is missing but something needs one, or is ambiguous:
 *
 * - none, and there is an `@EventHandler` or a `@Saga` (unless `kraken.messaging.sagas.enabled` is false): they read events.
 *   Command, query and subscription-query handlers need none. (Without a message store either, [MessageStoreGuard] reports
 *   that first.)
 * - two or more (two event-store add-ons, each providing its own) when `kraken.messaging.event-store` does not choose one;
 * - `kraken.messaging.event-store` names one the application does not have.
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
            if (environment.getProperty("kraken.messaging.sagas.enabled", Boolean::class.java, true)) {
                ApplicationClassScanning.sagaClasses(beanFactory, environment, resourceLoader)
                    .map { it.getAnnotation(Saga::class.java).name.ifBlank { it.name } }.sorted().forEach { add("@Saga $it") }
            }
        }
        if (needs.isEmpty()) return
        throw IllegalStateException(needs.distinct().joinToString("; ") { "$it needs an event store" } + ": " + ADD_AN_EVENT_STORE)
    }

    companion object {
        /** The event store's name, when two add-ons are on the classpath. */
        const val EVENT_STORE = "kraken.messaging.event-store"
    }
}

/**
 * Applies the message store's schema at startup ([MessageStore.initSchema]: its indexes or tables) unless
 * `kraken.messaging.store.init-schema` is false, whichever store the application has (an add-on's or its own). Runs once
 * every singleton exists and before any lifecycle (the outbox relay, the consumers) starts.
 */
class MessageStoreSchema(private val store: ObjectProvider<MessageStore>, private val environment: Environment) : SmartInitializingSingleton {
    override fun afterSingletonsInstantiated() {
        if (!environment.getProperty(INIT_SCHEMA, Boolean::class.java, true)) return
        store.ifAvailable { it.initSchema() }
    }

    companion object {
        /** On by default: false when the service applies the store's schema itself (a migration tool, a DBA). */
        const val INIT_SCHEMA = "kraken.messaging.store.init-schema"
    }
}

/**
 * Explains a startup that failed because a bean needs an [EventBus] (or the saga relay's [RawEventPublisher]) and there is
 * none: without a message store there is no outbox (ADR 0028), and without an event store nothing relays it (ADR 0027), so no
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
