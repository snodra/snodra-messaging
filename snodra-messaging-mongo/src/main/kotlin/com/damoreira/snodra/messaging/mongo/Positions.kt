package com.damoreira.snodra.messaging.mongo

/**
 * How segments and positions (the event store's opaque strings, ADR 0027) are stored: in the fields they always had
 * (`partition`, `offset`, and a replay range's `partition`, `from` and `until`), as an Int32 (a segment) or an Int64 (a position)
 * when the string is a canonical decimal number, as the library always wrote them for Kafka, else as the string itself.
 * Documents written before the event store became an add-on are read, found and deleted as before, without a migration.
 * Positions are compared in queries as MongoDB compares the stored values: numbers by value (every first-wave event store's
 * positions are decimal), strings by their bytes.
 */
internal object Positions {
    /** [segment] as stored: an Int32 when it is one written canonically, else the string. */
    fun segment(segment: String): Any = segment.toIntOrNull()?.takeIf { it.toString() == segment } ?: segment

    /** [position] as stored: an Int64 when it is one written canonically, else the string. */
    fun position(position: String): Any = position.toLongOrNull()?.takeIf { it.toString() == position } ?: position

    /** A stored segment or position as its string; null for another type. */
    fun text(value: Any?): String? = when (value) {
        is Int, is Long -> value.toString()
        is String -> value
        else -> null
    }
}
