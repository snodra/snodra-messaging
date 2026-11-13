package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.control.ResetContext
import io.damoreira.kraken.messaging.eventstore.ResetTarget
import io.damoreira.kraken.messaging.handler.DisallowReplay
import io.damoreira.kraken.messaging.handler.EventHandler
import io.damoreira.kraken.messaging.handler.ProcessingGroup
import io.damoreira.kraken.messaging.handler.ResetHandler
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
 *
 * P6b (ADR 0023): a reset of `audit` through the hub replays the notes in its range, so [handled] lists them again; [onReset]
 * (its `@ResetHandler`) records each reset in [resets] before the replay starts. A real projection would clear itself there.
 */
@Component
@ProcessingGroup("audit")
class AuditHandler {
    private val log = LoggerFactory.getLogger(AuditHandler::class.java)
    private val notes = CopyOnWriteArrayList<AuditNote>()
    private val resetLog = CopyOnWriteArrayList<AuditReset>()

    @Volatile
    var failing = false

    val handled: List<AuditNote> get() = notes.toList()

    /** The resets of the processor that replayed something, in order. */
    val resets: List<AuditReset> get() = resetLog.toList()

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

    @ResetHandler
    fun onReset(ctx: ResetContext) {
        val target = when (val t = ctx.target) {
            ResetTarget.Earliest -> "earliest"
            ResetTarget.Latest -> "latest"
            is ResetTarget.Timestamp -> "timestamp ${t.timestampMs}"
            is ResetTarget.Positions -> "offsets ${t.positions}"
        }
        resetLog += AuditReset(ctx.operationId, target, ctx.ranges.map { AuditRange(it.segment.toInt(), it.from.toLong(), it.until.toLong()) })
        log.info("B: audit reset {} to {}, replaying {}", ctx.operationId, target, ctx.ranges)
    }
}

/** One reset of the `audit` processor as `GET /audit/resets` shows it. */
data class AuditReset(val operationId: String, val target: String, val ranges: List<AuditRange>)

/** A replayed range: offsets `from <= offset < until` of [partition]. */
data class AuditRange(val partition: Int, val from: Long, val until: Long)

/**
 * The `audit` processor's side effect that must happen once (P6b): it would notify someone of each note, so it is
 * `@DisallowReplay`: a reset of `audit` replays [AuditHandler] but not this. [notified] is what it notified, in memory.
 */
@Component
@ProcessingGroup("audit")
class AuditNotifier {
    private val log = LoggerFactory.getLogger(AuditNotifier::class.java)
    private val notes = CopyOnWriteArrayList<AuditNote>()

    val notified: List<AuditNote> get() = notes.toList()

    @DisallowReplay
    @EventHandler
    fun on(note: AuditNote) {
        notes += note
        log.info("B: notified key={} text={}", note.key, note.text)
    }
}
