package io.damoreira.kraken.messaging.config.sagascan

import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.StartSaga
import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.context.annotation.Configuration

@MessageType("scan.Started")
data class ScanStarted(val orderId: String) : Event

/** The one saga of the package the auto-configuration test registers as the application's. */
@Saga(name = "scan.Order")
class ScanSaga {
    var orderId: String? = null

    @StartSaga
    @SagaEventHandler(associationProperty = "orderId")
    fun on(e: ScanStarted) {
        orderId = e.orderId
    }
}

/** Registers this package as the application's (what `@SpringBootApplication` does for its own). */
@Configuration(proxyBeanMethods = false)
@AutoConfigurationPackage
class SagaScanConfig
