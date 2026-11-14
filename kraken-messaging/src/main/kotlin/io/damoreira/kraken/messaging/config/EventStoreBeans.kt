package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.eventstore.EventStore
import org.springframework.beans.factory.config.BeanFactoryPostProcessor
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.boot.autoconfigure.condition.ConditionOutcome
import org.springframework.boot.autoconfigure.condition.SpringBootCondition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.core.env.Environment
import org.springframework.core.type.AnnotatedTypeMetadata

/**
 * Where the [EventStore] beans come from, as [MessageStoreBeans] tells it for message stores: the application (its own `@Bean`
 * or a registered bean, of any type) or an auto-configuration (an event-store add-on's). An add-on's event store steps back for
 * the application's ([ConditionalOnNoApplicationEventStore]), never for another add-on's: two add-ons without
 * `kraken.messaging.event-store` fail the startup ([EventStoreAddOnsCheck]).
 *
 * **The convention for an event-store add-on** (a third party's included) is the store add-ons' one: declare the event store's
 * `@Bean` on the add-on's `@AutoConfiguration` class or on a class nested in it, annotated [ConditionalOnNoApplicationEventStore]
 * and `@ConditionalOnProperty("kraken.messaging.event-store", havingValue = <its name>, matchIfMissing = true)`.
 */
internal object EventStoreBeans {
    /** Every bean defined as an [EventStore], without creating any. */
    fun names(beanFactory: ConfigurableListableBeanFactory): List<String> = beanFactory.getBeanNamesForType(EventStore::class.java, true, false).toList()
}

/** Matches when the application defines no [EventStore] bean of its own (beans of auto-configurations do not count). */
class OnNoApplicationEventStoreCondition : SpringBootCondition() {
    override fun getMatchOutcome(context: ConditionContext, metadata: AnnotatedTypeMetadata): ConditionOutcome {
        val beanFactory = context.beanFactory ?: return ConditionOutcome.match("no bean factory")
        val own = EventStoreBeans.names(beanFactory).filterNot { MessageStoreBeans.autoConfigured(beanFactory, it) }
        return if (own.isEmpty()) ConditionOutcome.match("the application defines no EventStore")
        else ConditionOutcome.noMatch("the application defines its own EventStore: $own")
    }
}

/**
 * On an event-store add-on's event store bean: it is created only when the application defines no [EventStore] of its own (of
 * any type), whatever other add-ons define.
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@Conditional(OnNoApplicationEventStoreCondition::class)
annotation class ConditionalOnNoApplicationEventStore

/**
 * Fails the startup, before any bean is created, when two event stores are defined by auto-configurations (two event-store
 * add-ons, say Kafka and RabbitMQ Streams) and `kraken.messaging.event-store` does not choose one: each add-on's event store
 * steps back only for the application's own ([ConditionalOnNoApplicationEventStore]), so without this check one of them would win
 * silently. Event stores the application defines itself are [EventStoreGuard]'s.
 */
class EventStoreAddOnsCheck(private val environment: Environment) : BeanFactoryPostProcessor {
    override fun postProcessBeanFactory(beanFactory: ConfigurableListableBeanFactory) {
        if (!environment.getProperty(EventStoreGuard.EVENT_STORE).isNullOrBlank()) return
        val addOns = EventStoreBeans.names(beanFactory).filter { MessageStoreBeans.autoConfigured(beanFactory, it) }
            .mapNotNull { name -> beanFactory.getType(name, false)?.let { name to it } }
        if (addOns.size < 2) return
        throw IllegalStateException(
            "${addOns.size} event store add-ons are configured (${addOns.joinToString { (_, type) -> "${type.simpleName}: ${addOnName(type)}" }}): " +
                "set ${EventStoreGuard.EVENT_STORE} to the one this service uses (${addOns.joinToString(", ") { addOnName(it.second) }}), or remove the " +
                "others from the dependencies",
        )
    }
}

/** An add-on store's name: the `NAME` constant of its class (a Kotlin companion's `const val`), else the class's simple name. */
internal fun addOnName(type: Class<*>): String =
    runCatching { type.getField("NAME").get(null) as? String }.getOrNull() ?: type.simpleName
