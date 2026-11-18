package com.damoreira.snodra.messaging.e2e.processors

import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.handler.EventHandler
import com.damoreira.snodra.messaging.handler.ProcessingGroup
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

@MessageType("p6a.OrderPlaced")
data class OrderPlaced(val orderId: String, val poison: Boolean = false) : Event

/** A service with a `default` handler and a named processor `orders` whose handler fails on a poisoned order. */
@SpringBootApplication(scanBasePackages = ["com.damoreira.snodra.messaging.e2e.processors"])
class ProcessorsService

@Component
class Audit {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: OrderPlaced) { seen += event.orderId }
}

@Component
@ProcessingGroup("orders")
class OrdersProjection {
    val seen = CopyOnWriteArrayList<String>()
    @EventHandler fun on(event: OrderPlaced) {
        seen += event.orderId
        if (event.poison) throw IllegalStateException("cannot project ${event.orderId}")
    }
}
