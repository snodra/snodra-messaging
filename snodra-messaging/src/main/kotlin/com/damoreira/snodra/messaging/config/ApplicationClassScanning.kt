package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.external.IdempotentHandler
import com.damoreira.snodra.messaging.saga.Saga
import org.springframework.beans.factory.BeanFactory
import org.springframework.boot.autoconfigure.AutoConfigurationPackages
import org.springframework.boot.context.properties.bind.Bindable
import org.springframework.boot.context.properties.bind.Binder
import org.springframework.context.annotation.ConditionContext
import org.springframework.context.annotation.ConfigurationCondition
import org.springframework.context.annotation.ConfigurationCondition.ConfigurationPhase
import org.springframework.core.type.classreading.MetadataReader
import org.springframework.core.type.classreading.MetadataReaderFactory
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider
import org.springframework.core.env.Environment
import org.springframework.core.io.ResourceLoader
import org.springframework.core.type.AnnotatedTypeMetadata
import org.springframework.core.type.filter.AnnotationTypeFilter
import org.springframework.core.type.filter.TypeFilter
import org.springframework.util.ClassUtils

/**
 * Finds the application's own classes by scanning the packages registered with `AutoConfigurationPackages` (the packages of
 * the `@SpringBootApplication` class): the `@Saga` classes, and the classes with an `@IdempotentHandler` method. Sagas are
 * never beans, and an adapter may be created by a `@Bean` method, so bean definitions are not the place to look.
 *
 * A class that the scan cannot load is an error naming it (the same startup error the saga scanner would give later). Without
 * any registered package nothing is found.
 */
internal object ApplicationClassScanning {
    fun sagaClasses(beanFactory: BeanFactory, environment: Environment, resourceLoader: ResourceLoader): List<Class<*>> =
        find(beanFactory, environment, resourceLoader, AnnotationTypeFilter(Saga::class.java, false, false))

    fun idempotentHandlerClasses(beanFactory: BeanFactory, environment: Environment, resourceLoader: ResourceLoader): List<Class<*>> =
        find(beanFactory, environment, resourceLoader, object : TypeFilter {
            override fun match(reader: MetadataReader, factory: MetadataReaderFactory) =
                reader.annotationMetadata.hasAnnotatedMethods(IdempotentHandler::class.java.name)
        })

    private fun find(beanFactory: BeanFactory, environment: Environment, resourceLoader: ResourceLoader, filter: TypeFilter): List<Class<*>> {
        if (!AutoConfigurationPackages.has(beanFactory)) return emptyList()
        val provider = ClassPathScanningCandidateComponentProvider(false, environment)
        provider.setResourceLoader(resourceLoader)
        provider.addIncludeFilter(filter)
        val classLoader = resourceLoader.classLoader
        return AutoConfigurationPackages.get(beanFactory).flatMap { pkg -> provider.findCandidateComponents(pkg) }
            .mapNotNull { it.beanClassName }
            .distinct()
            .map { name ->
                try {
                    ClassUtils.forName(name, classLoader)
                } catch (e: ClassNotFoundException) {
                    throw IllegalStateException("Cannot load $name, found while scanning the application's packages: ${e.message}", e)
                }
            }
    }
}

/**
 * True when the application has a `@Saga` class. Evaluated when the bean definitions are registered, after the user's
 * configuration registered its packages (as `@ConditionalOnBean` is).
 */
internal class OnSagaClasses : ConfigurationCondition {
    override fun getConfigurationPhase() = ConfigurationPhase.REGISTER_BEAN

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean =
        ApplicationClassScanning.sagaClasses(context.beanFactory!!, context.environment, context.resourceLoader!!).isNotEmpty()
}

/**
 * True when `snodra.messaging.external.calls.enabled` is true, or is unset and a class in the application's packages has an
 * `@IdempotentHandler` method.
 */
internal class OnIdempotentHandlers : ConfigurationCondition {
    override fun getConfigurationPhase() = ConfigurationPhase.REGISTER_BEAN

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {
        val explicit = Binder.get(context.environment).bind("snodra.messaging.external.calls.enabled", Boolean::class.javaObjectType)
        if (explicit.isBound) return explicit.get() == true
        return ApplicationClassScanning.idempotentHandlerClasses(context.beanFactory!!, context.environment, context.resourceLoader!!).isNotEmpty()
    }
}

/** True when a saga runtime or an idempotency interceptor bean is defined: the toolkit is in use and its metrics make sense. */
internal class OnSagaToolkitBeans : ConfigurationCondition {
    override fun getConfigurationPhase() = ConfigurationPhase.REGISTER_BEAN

    override fun matches(context: ConditionContext, metadata: AnnotatedTypeMetadata): Boolean {
        val factory = context.beanFactory!!
        return listOf(com.damoreira.snodra.messaging.saga.SagaRuntime::class.java, com.damoreira.snodra.messaging.external.IdempotencyInterceptor::class.java)
            .any { factory.getBeanNamesForType(it, true, false).isNotEmpty() }
    }
}

