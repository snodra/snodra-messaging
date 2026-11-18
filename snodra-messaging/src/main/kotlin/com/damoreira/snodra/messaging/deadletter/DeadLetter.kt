package com.damoreira.snodra.messaging.deadletter

import java.time.Instant

/** Why a record became a dead letter (ADR 0022). */
enum class DeadLetterReason {
    /** Stage 1 failed on every attempt (or once, for a payload its handler cannot decode). */
    HANDLER_FAILED,

    /** An earlier letter of its key was parked in the processor: it waits behind it without running stage 1. */
    SEQUENCED,

    /** It cannot be handled (no `type` header, or the value is not a JSON object): never blocks its key, discard only. */
    INVALID,
}

/** `error` of a letter: [type] is the exception's simple class name (or `Sequenced`, `InvalidMessage`), [message] at most 2000 code points. */
class DeadLetterError(type: String, message: String) {
    val type: String = type
    val message: String = cut(message, MAX_MESSAGE)

    override fun equals(other: Any?) = other is DeadLetterError && other.type == type && other.message == message
    override fun hashCode() = 31 * type.hashCode() + message.hashCode()
    override fun toString() = "DeadLetterError(type=$type, message=$message)"

    companion object {
        /** The longest `error.message` in code points (shared with the Rust library, as `snodra-dlt-error`). */
        const val MAX_MESSAGE = 2000

        /** The error of an INVALID letter. */
        fun invalid(message: String) = DeadLetterError("InvalidMessage", message)

        /** The error of a SEQUENCED letter waiting behind [headId]. */
        fun sequenced(headId: String) = DeadLetterError("Sequenced", "an earlier letter for this key is parked: $headId")

        /** The error of a handler failure: the exception's simple class name and its message (empty when it has none). */
        fun of(e: Throwable) = DeadLetterError(e::class.java.simpleName.ifEmpty { e::class.java.name }, e.message ?: "")

        /** [text] cut to at most [max] code points, never splitting a surrogate pair. */
        internal fun cut(text: String, max: Int): String =
            if (text.codePointCount(0, text.length) <= max) text else text.substring(0, text.offsetByCodePoints(0, max))
    }
}

/**
 * One document of `dead_letters` (ADR 0022), as [DeadLetters] returns it. [payload] is the record's value as text when it
 * is valid UTF-8, [payloadBinary] otherwise; both are null for an empty value ([value] gives the bytes either way).
 */
data class DeadLetter(
    /** `<processor>:<source>:<segment>:<position>`. */
    val id: String,
    val processor: String,
    /** The source the record was read from. */
    val source: String,
    /** The record's segment of [source] and its position there: the event store's opaque strings. */
    val segment: String,
    val position: String,
    /** The record's key (lossy UTF-8), null when it had none. */
    val key: String?,
    /** The `id` header, null when the record had none or a blank one. */
    val eventId: String?,
    /** The `type` header, null when the record had none or a blank one. */
    val eventType: String?,
    /** All record headers (lossy UTF-8; a repeated name keeps its last value). */
    val headers: Map<String, String>,
    val payload: String?,
    val payloadBinary: ByteArray?,
    val reason: DeadLetterReason,
    val error: DeadLetterError,
    /** SEQUENCED only: the key's head letter when it was parked (display only). */
    val blockedBy: String?,
    /** Handler attempts so far (0 for SEQUENCED and INVALID until a retry). */
    val attempts: Int,
    val parkedAt: Instant,
    val lastAttemptAt: Instant,
    /** When the TTL index deletes it (`parkedAt` + retention), which also unblocks its key. */
    val expireAt: Instant,
    /** It was parked while its processor replayed the record (P6b): its retry is a replay too. `replay: true`, else absent. */
    val replay: Boolean = false,
) {
    /** The record's value. */
    fun value(): ByteArray = payload?.toByteArray(Charsets.UTF_8) ?: payloadBinary ?: ByteArray(0)

    override fun equals(other: Any?): Boolean = other is DeadLetter && id == other.id && processor == other.processor &&
        source == other.source && segment == other.segment && position == other.position && key == other.key &&
        eventId == other.eventId && eventType == other.eventType && headers == other.headers && payload == other.payload &&
        (payloadBinary?.contentEquals(other.payloadBinary ?: return false) ?: (other.payloadBinary == null)) &&
        reason == other.reason && error == other.error && blockedBy == other.blockedBy && attempts == other.attempts &&
        parkedAt == other.parkedAt && lastAttemptAt == other.lastAttemptAt && expireAt == other.expireAt && replay == other.replay

    override fun hashCode(): Int = id.hashCode()
}

/** What [DeadLetters.retry] did with a letter. */
sealed interface RetryResult {
    /** Stage 1 ran and committed together with the letter's deletion. */
    data object Retried : RetryResult

    /** The processor's marker already existed (stage 1 had run): the letter was deleted. */
    data object Duplicate : RetryResult

    /** Stage 1 failed again: the letter stays, with `attempts + 1`, the new `error`, `lastAttemptAt` and reason HANDLER_FAILED. */
    data class Failed(val message: String) : RetryResult

    /** An earlier letter of its key ([headId], the earliest position) is still parked: retry that one first. */
    data class Blocked(val headId: String) : RetryResult

    /** An INVALID letter cannot be retried, only discarded. */
    data object NotRetriable : RetryResult

    /** No such letter (never parked, discarded, retried or expired). */
    data object NotFound : RetryResult
}

/** What [DeadLetters.retryAll] did: letters retried (or duplicates), failed, blocked behind a failure, skipped (INVALID or gone). */
data class RetryAllResult(val retried: Int, val failed: Int, val blocked: Int, val skipped: Int)
