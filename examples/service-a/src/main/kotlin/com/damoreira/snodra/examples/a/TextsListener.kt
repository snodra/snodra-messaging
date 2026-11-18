package com.damoreira.snodra.examples.a

import com.damoreira.snodra.examples.api.TextReversed
import com.damoreira.snodra.examples.api.TextShouted
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.handler.EventHandler
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/** One received text event, as exposed on `GET /texts/delivered`. */
data class TextEvent(val kind: String, val text: String, val at: Instant)

/** Reacts to service-c's events and keeps what it saw. */
@Component
class TextsListener {
    private val log = LoggerFactory.getLogger(TextsListener::class.java)
    private val received = CopyOnWriteArrayList<TextEvent>()

    val delivered: List<TextEvent> get() = received.toList()

    @EventHandler
    fun on(event: TextShouted, ctx: MessageContext) {
        received += TextEvent("shouted", event.text, event.at)
        log.info("A: received TextShouted text={} at={}", event.text, event.at)
    }

    @EventHandler
    fun on(event: TextReversed, ctx: MessageContext) {
        received += TextEvent("reversed", event.text, event.at)
        log.info("A: received TextReversed text={} at={}", event.text, event.at)
    }
}
