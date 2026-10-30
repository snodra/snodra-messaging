package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.TextReversed
import io.damoreira.kraken.examples.api.TextShouted
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.handler.EventHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.util.concurrent.atomic.AtomicInteger

/** Service-b only watches the texts domain: it logs service-c's events. */
@Component
class TextEventsListener {
    private val log = LoggerFactory.getLogger(TextEventsListener::class.java)
    private val count = AtomicInteger()

    val received: Int get() = count.get()

    @EventHandler
    fun on(event: TextShouted, ctx: MessageContext) {
        count.incrementAndGet()
        log.info("B: received TextShouted text={} at={}", event.text, event.at)
    }

    @EventHandler
    fun on(event: TextReversed, ctx: MessageContext) {
        count.incrementAndGet()
        log.info("B: received TextReversed text={} at={}", event.text, event.at)
    }
}
