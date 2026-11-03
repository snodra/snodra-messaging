package io.damoreira.kraken.messaging.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

/**
 * The [Clock] of the saga runtime, the deadline scheduler and the idempotency interceptor: the application's own when it has
 * one, else the system UTC clock. Imported by the configurations that need it, so a service that uses none of them gets no
 * `Clock` bean from the library.
 */
@Configuration(proxyBeanMethods = false)
class KrakenClockConfiguration {
    @Bean
    @ConditionalOnMissingBean
    fun krakenClock(): Clock = Clock.systemUTC()
}
