package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.ProcessingGroup
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * An audit note, published by `POST /audit/notes` with [key] as its Kafka key (notes of one key are handled in order) and
 * handled by the `audit` processor. Only service-b knows this type.
 */
@MessageType("examples.AuditNote")
data class AuditNote(val key: String, val text: String, val at: Instant) : Event

/**
 * The sandbox's named event processor (P6a, ADR 0022): its own Kafka consumer, group `<group>.audit`, markers
 * `processor:audit:<eventId>`, its own dead letters. [failing] is example-only: while it is on, every note whose text starts
 * with `poison` fails (the others are handled), so the sandbox can show a record parked as a dead letter, the later records
 * of its key parked behind it while other keys flow, and the key draining once [failing] is off and the letters are retried
 * (`POST /audit/failing?on=...`, `/audit/dead-letters`). [handled] is what it handled, in order, in memory (lost on restart,
 * like the rest of service-b's state).
 */
@Component
@ProcessingGroup("audit")
class AuditHandler {
    private val log = LoggerFactory.getLogger(AuditHandler::class.java)
    private val notes = CopyOnWriteArrayList<AuditNote>()

    @Volatile
    var failing = false

    val handled: List<AuditNote> get() = notes.toList()

    companion object {
        /** While [failing] is on, a note whose text starts with this fails. */
        const val POISON = "poison"
    }

    @EventHandler
    fun on(note: AuditNote) {
        check(!(failing && note.text.startsWith(POISON))) { "the audit processor is failing on demand (key=${note.key} text=${note.text})" }
        notes += note
        log.info("B: audited key={} text={}", note.key, note.text)
    }
}
