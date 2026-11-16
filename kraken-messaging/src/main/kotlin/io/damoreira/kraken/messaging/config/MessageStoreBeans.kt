package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.store.MessageStore
import org.springframework.beans.factory.annotation.AnnotatedBeanDefinition
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionOutcome
import org.springframework.boot.autoconfigure.condition.SpringBootCondition
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.Conditional
import org.springframework.core.annotation.AnnotatedElementUtils
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.util.ClassUtils

/**
 * Where the [MessageStore] beans come from: the application (its own `@Bean`, a registered bean, of any type, an add-on's store
 * class included) or an auto-configuration (a store add-on's, the in-memory one's). An add-on's store steps back for the
 * application's ([ConditionalOnNoApplicationMessageStore]), never for another add-on's: two add-ons without
 * `kraken.messaging.store.type` fail the startup ([MessageStoreAddOnsCheck]).
 *
 * **The convention for a store add-on** (a third party's included): declare the store's `@Bean` on the add-on's
 * `@AutoConfiguration` class or on a class nested in it, annotated [ConditionalOnNoApplicationMessageStore]. A store declared
 * elsewhere, in a class the auto-configuration `@Import`s say, counts as the application's own: the other add-ons step back for it
 * and the two-add-ons check does not see it. (Imports are not followed: which class imported which is not known yet when the
 * add-ons' conditions run.)
 */
internal object MessageStoreBeans {
    /** Every bean defined as a [MessageStore], without creating any. */
    fun names(beanFactory: ConfigurableListableBeanFactory): List<String> = beanFactory.getBeanNamesForType(MessageStore::class.java, true, false).toList()

    /** Whether the bean [name] is defined by a `@Bean` method of an auto-configuration (or of a class nested in one). */
    fun autoConfigured(beanFactory: ConfigurableListableBeanFactory, name: String): Boolean {
        if (!beanFactory.containsBeanDefinition(name)) return false
        val declaring = (beanFactory.getBeanDefinition(name) as? AnnotatedBeanDefinition)?.factoryMethodMetadata?.declaringClassName ?: return false
        var type: Class<*>? = runCatching { ClassUtils.forName(declaring, beanFactory.beanClassLoader) }.getOrNull()
        while (type != null) {
            if (AnnotatedElementUtils.hasAnnotation(type, AutoConfiguration::class.java)) return true
            type = type.enclosingClass
        }
        return false
    }
}

/** Matches when the application defines no [MessageStore] bean of its own (beans of auto-configurations do not count). */
class OnNoApplicationMessageStoreCondition : SpringBootCondition() {
    override fun getMatchOutcome(context: ConditionContext, metadata: AnnotatedTypeMetadata): ConditionOutcome {
        val beanFactory = context.beanFactory ?: return ConditionOutcome.match("no bean factory")
        val own = MessageStoreBeans.names(beanFactory).filterNot { MessageStoreBeans.autoConfigured(beanFactory, it) }
        return if (own.isEmpty()) ConditionOutcome.match("the application defines no MessageStore")
        else ConditionOutcome.noMatch("the application defines its own MessageStore: $own")
    }
}

/**
 * On a store add-on's store bean: it is created only when the application defines no [MessageStore] of its own (of any type),
 * whatever other add-ons define.
 */
@Target(AnnotationTarget.CLASS, AnnotationTarget.FUNCTION)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@Conditional(OnNoApplicationMessageStoreCondition::class)
annotation class ConditionalOnNoApplicationMessageStore
