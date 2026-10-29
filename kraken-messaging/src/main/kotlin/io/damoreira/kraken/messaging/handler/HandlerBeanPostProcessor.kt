package io.damoreira.kraken.messaging.handler

import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.config.BeanPostProcessor

/** Registers handler methods of every application bean; declare it as a `static @Bean`. */
class HandlerBeanPostProcessor(private val registry: HandlerRegistry) : BeanPostProcessor {
    override fun postProcessAfterInitialization(bean: Any, beanName: String): Any {
        val name = AopUtils.getTargetClass(bean).name
        if (SKIPPED_PREFIXES.none { name.startsWith(it) }) registry.register(bean)
        return bean
    }

    private companion object {
        val SKIPPED_PREFIXES = listOf("java.", "kotlin.", "org.springframework.")
    }
}
