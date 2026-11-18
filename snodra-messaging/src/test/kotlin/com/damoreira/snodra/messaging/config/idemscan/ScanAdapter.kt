package com.damoreira.snodra.messaging.config.idemscan

import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.external.IdempotentHandler
import com.damoreira.snodra.messaging.handler.CommandHandler
import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.context.annotation.Configuration

@MessageType("scan.Print")
data class ScanPrint(val jobId: String) : Command<Unit>

/** An adapter with an idempotent handler: its presence in the application's packages brings the interceptor. */
class ScanAdapter {
    @CommandHandler
    @IdempotentHandler
    fun handle(c: ScanPrint) = Unit
}

@Configuration(proxyBeanMethods = false)
@AutoConfigurationPackage
class IdemScanConfig

/** The adapter as a bean, so the registry holds its idempotent handler. */
@Configuration(proxyBeanMethods = false)
@AutoConfigurationPackage
class IdemBeanConfig {
    @org.springframework.context.annotation.Bean
    fun scanAdapter() = ScanAdapter()
}
