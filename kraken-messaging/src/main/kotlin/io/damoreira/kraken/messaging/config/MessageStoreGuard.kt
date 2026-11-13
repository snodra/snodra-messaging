package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.RawEventPublisher
import io.damoreira.kraken.messaging.store.MessageStore
import org.springframework.beans.factory.BeanFactory
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
    "add a message store add-on (kraken-messaging-mongo for MongoDB) to the dependencies, or declare a MessageStore bean"

/**
 * Fails the startup when the application has no [MessageStore] (ADR 0028) but something needs one: an `@EventHandler`, a
 * `@Saga` (unless `kraken.messaging.sagas.enabled` is false) or an `@IdempotentHandler`. Command, query and subscription-query
 * handlers need none. Runs once every singleton exists, so the [registry] holds every handler.
 */
class MessageStoreGuard(
    private val beanFactory: BeanFactory,
    private val registry: HandlerRegistry,
    private val environment: Environment,
    private val resourceLoader: ResourceLoader,
) : SmartInitializingSingleton {
    override fun afterSingletonsInstantiated() {
        if (beanFactory.getBeanProvider(MessageStore::class.java).iterator().hasNext()) return
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
 * none: without a message store there is no outbox, so no event bus (ADR 0028).
 */
@Order(Ordered.HIGHEST_PRECEDENCE) // before Boot's generic NoSuchBeanDefinitionException analyzer, which would explain it first
class MissingMessageStoreFailureAnalyzer : AbstractFailureAnalyzer<NoSuchBeanDefinitionException>() {
    override fun analyze(rootFailure: Throwable, cause: NoSuchBeanDefinitionException): FailureAnalysis? {
        val type = cause.beanType?.takeIf { it == EventBus::class.java || it == RawEventPublisher::class.java } ?: return null
        return FailureAnalysis(
            "A bean needs an ${type.simpleName}, and the ${type.simpleName} needs a message store: events are written to the outbox " +
                "of the service's own database, and this application has no MessageStore.",
            "To publish events, $ADD_A_MESSAGE_STORE. A service with only command and query handlers needs no store and no ${type.simpleName}.",
            cause,
        )
    }
}
