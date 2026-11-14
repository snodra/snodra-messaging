package io.damoreira.kraken.messaging.jdbc

import java.security.MessageDigest

/**
 * The bounds of the SQL message store's columns (schema version 1), the same on both databases and in both libraries. A value
 * over its bound is refused before anything is written, with an [IllegalArgumentException] naming the field and its length
 * (never the value: it may be personal data); the database never truncates. Lengths are in characters (Unicode code points),
 * as `VARCHAR(n)` counts them on both databases.
 *
 * Values without a bound (marker ids, idempotency keys, saga start keys, record keys, associations) are found through the
 * SHA-256 of their UTF-8 bytes ([sha256]) and kept whole in a text column. Free text (error messages) is cut to
 * [FREE_TEXT_BYTES] bytes of UTF-8 ([cut]).
 */
internal object Limits {
    /** An event processor's name: the library validates at most 48 (`ProcessingGroup`); the hub keeps 64 bytes. */
    const val PROCESSOR = 64

    /** A source (a Kafka topic name is at most 249). */
    const val SOURCE = 255

    /** A segment of a source (a Kafka partition number). */
    const val SEGMENT = 128

    /** A position in a segment (a Kafka offset is at most 19 digits). */
    const val POSITION = 128

    /** A type name: an event or message type, a saga type, a deadline name. */
    const val NAME = 255

    /** An id the library builds: an outbox entry's, a saga's, a saga-dispatch entry's (`<sagaId>:<seq>`), a deadline's. */
    const val ID = 255

    /** A dead letter's id `<processor>:<source>:<segment>:<position>`: the longest those bounds build. */
    const val DEAD_LETTER_ID = PROCESSOR + SOURCE + SEGMENT + POSITION + 3

    /** Free text (an error message, a send's last error) is cut to this many bytes of UTF-8. */
    const val FREE_TEXT_BYTES = 16 * 1024

    /** Refuses [value] (when not null) longer than [max] characters, naming [field]. */
    fun check(field: String, value: String?, max: Int) {
        if (value == null) return
        val length = value.codePointCount(0, value.length)
        require(length <= max) { "The message store keeps $field of at most $max characters; this one has $length" }
    }

    /** [text] cut to at most [FREE_TEXT_BYTES] bytes of UTF-8, never inside a character. */
    fun cut(text: String): String {
        val bytes = text.toByteArray(Charsets.UTF_8)
        if (bytes.size <= FREE_TEXT_BYTES) return text
        var end = FREE_TEXT_BYTES
        while (end > 0 && (bytes[end].toInt() and 0xC0) == 0x80) end-- // a continuation byte: back to the character's start
        return String(bytes, 0, end, Charsets.UTF_8)
    }

    /** The lowercase hex SHA-256 of [value]'s UTF-8 bytes. */
    fun sha256(value: String): String = sha256(value.toByteArray(Charsets.UTF_8))

    /** The hash of an association: SHA-256 of [key]'s UTF-8 bytes, one zero byte, [value]'s UTF-8 bytes. */
    fun associationHash(key: String, value: String): String =
        sha256(key.toByteArray(Charsets.UTF_8) + byteArrayOf(0) + value.toByteArray(Charsets.UTF_8))

    private fun sha256(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
}
