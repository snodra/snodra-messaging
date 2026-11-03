package io.damoreira.kraken.messaging.config.idemscan

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.external.IdempotentHandler
import io.damoreira.kraken.messaging.handler.CommandHandler
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
