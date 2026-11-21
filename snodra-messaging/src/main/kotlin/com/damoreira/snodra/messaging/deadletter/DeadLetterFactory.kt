package com.damoreira.snodra.messaging.deadletter

import com.damoreira.snodra.messaging.eventstore.Delivery
import com.damoreira.snodra.messaging.store.DeadLetterStore
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction
import java.time.Clock
import java.time.Duration

/**
 * Builds the [DeadLetter] of a delivered record, ready for [DeadLetterStore.park]: the id
 * `<processor>:<source>:<segment>:<position>`, the record's key and headers (as the event store delivers them: UTF-8 text),
 * the `id` and `type` headers as `eventId` and `eventType` (a blank one is none), the value as text when it is valid UTF-8 and
 * as bytes otherwise (neither for an empty value), and `expireAt` = now + [retention].
 */
class DeadLetterFactory(private val retention: Duration, private val clock: Clock = Clock.systemUTC()) {
    init {
        require(retention >= Duration.ofSeconds(1)) { "dead-letter retention must be at least 1s, was $retention" }
    }

    /** The letter of [record], read from [source], in [processor]; [replay]: parked while the processor replays it. */
    fun letter(
        processor: String, source: String, record: Delivery, reason: DeadLetterReason, error: DeadLetterError,
        attempts: Int, blockedBy: String? = null, replay: Boolean = false,
    ): DeadLetter {
        val now = clock.instant()
        val headers = LinkedHashMap(record.headers)
        val value = record.payload
        val text = if (value.isEmpty()) null else strictUtf8(value)
        return DeadLetter(
            id = DeadLetterStore.id(processor, source, record.segment, record.position),
            processor = processor,
            source = source,
            segment = record.segment,
            position = record.position,
            key = record.key,
            // A blank id or type is no id or type (as the consumer reads it): absent, never "".
            eventId = headers[ID_HEADER]?.takeIf { it.isNotBlank() },
            eventType = headers[TYPE_HEADER]?.takeIf { it.isNotBlank() },
            headers = headers,
            payload = text,
            payloadBinary = if (value.isNotEmpty() && text == null) value else null,
            reason = reason,
            error = error,
            blockedBy = blockedBy,
            attempts = attempts,
            parkedAt = now,
            lastAttemptAt = now,
            expireAt = now.plus(retention),
            replay = replay,
        )
    }

    /** The SEQUENCED letter of [record] waiting behind [headId]. */
    fun sequenced(processor: String, source: String, record: Delivery, headId: String, replay: Boolean = false): DeadLetter =
        letter(processor, source, record, DeadLetterReason.SEQUENCED, DeadLetterError.sequenced(headId), attempts = 0, blockedBy = headId, replay = replay)

    private companion object {
        const val ID_HEADER = "id"
        const val TYPE_HEADER = "type"

        fun strictUtf8(bytes: ByteArray): String? = try {
            Charsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
                .decode(ByteBuffer.wrap(bytes)).toString()
        } catch (_: CharacterCodingException) {
            null
        }
    }
}
