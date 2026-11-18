package com.damoreira.snodra.messaging.handler

import org.slf4j.LoggerFactory
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.config.BeanPostProcessor
import java.lang.reflect.Method

/**
 * Registers handler methods of every application bean; declare it as a `static @Bean`.
 *
 * The registry is obtained from [registryProvider] only for a bean that declares a handler annotation (checked with
 * plain Java reflection first), so the registry and its dependencies are not created while the context is still
 * registering post-processors, nor re-entered while they are being initialised themselves.
 */
class HandlerBeanPostProcessor(private val registryProvider: () -> HandlerRegistry) : BeanPostProcessor {
    constructor(registry: HandlerRegistry) : this({ registry })

    private val log = LoggerFactory.getLogger(HandlerBeanPostProcessor::class.java)

    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
        val target = AopUtils.getTargetClass(bean)
        if (SKIPPED_PREFIXES.any { target.name.startsWith(it) } || !declaresHandler(target)) return bean
        try {
            registryProvider().register(bean)
        } catch (e: Throwable) {
            // kotlin-reflect cannot introspect some synthetic classes; such a bean cannot carry handlers usable here.
            if (e !is UnsupportedOperationException && e::class.java.name != KOTLIN_REFLECTION_INTERNAL_ERROR) throw e
            log.warn("Skipping handler scan of bean {} ({}): {}", beanName, target.name, e.toString())
        }
        return bean
    }

    private fun declaresHandler(type: Class<*>): Boolean {
        if (type.methods.any(::annotated)) return true
        return generateSequence(type) { it.superclass }.any { klass -> klass.declaredMethods.any(::annotated) }
    }

    private fun annotated(method: Method) = HANDLER_ANNOTATIONS.any(method::isAnnotationPresent)

    private companion object {
        val SKIPPED_PREFIXES = listOf("java.", "kotlin.", "org.springframework.")
        val HANDLER_ANNOTATIONS = listOf(EventHandler::class.java, CommandHandler::class.java, QueryHandler::class.java, ResetHandler::class.java)
        const val KOTLIN_REFLECTION_INTERNAL_ERROR = "kotlin.reflect.jvm.internal.KotlinReflectionInternalError"
    }
}
