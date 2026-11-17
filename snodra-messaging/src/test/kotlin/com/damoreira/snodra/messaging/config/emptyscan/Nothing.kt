package com.damoreira.snodra.messaging.config.emptyscan

import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.context.annotation.Configuration

/** A package with neither a saga nor an idempotent handler. */
class Plain {
    fun hello() = "hello"
}

@Configuration(proxyBeanMethods = false)
@AutoConfigurationPackage
class EmptyScanConfig
