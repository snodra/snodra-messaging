package io.damoreira.kraken.examples.a

import io.damoreira.kraken.examples.api.GreetingDelivered
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.handler.EventHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.CopyOnWriteArrayList

/** Reacts to service-b's event and keeps what it saw, so the event leg can be checked over HTTP. */
@Component
class GreetingsListener {
    private val log = LoggerFactory.getLogger(GreetingsListener::class.java)
    private val received = CopyOnWriteArrayList<GreetingDelivered>()

    val delivered: List<GreetingDelivered> get() = received.toList()

    @EventHandler
    fun on(event: GreetingDelivered, ctx: MessageContext) {
        received += event
        log.info("A: received GreetingDelivered id={} name={} at={}", event.id, event.name, event.at)
    }
}
