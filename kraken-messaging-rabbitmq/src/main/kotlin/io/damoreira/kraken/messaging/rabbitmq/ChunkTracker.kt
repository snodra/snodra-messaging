package io.damoreira.kraken.messaging.rabbitmq

/**
 * Whether every message of a partition's chunks reached the subscription. The client announces a chunk (first offset, records)
 * before it checks and decodes it, and an error after that (a checksum, a codec, a listener) is only logged by the client, the
 * rest of the chunk dropped: when the next chunk starts, the previous one must have delivered all its records, else there is a
 * gap no acknowledgement may pass. Used by one thread at a time (the data connection's delivery thread).
 */
internal class ChunkTracker {
    private var first = -1L
    private var expected = 0L
    private var arrived = 0L

    /** A chunk from [first] with [records] records starts: the gap the previous one left, if any (a description), else null. */
    fun started(first: Long, records: Long): String? {
        val gap = if (expected > arrived) {
            "the chunk at offset ${this.first} delivered $arrived of its $expected messages (the client could not hand out the rest)"
        } else {
            null
        }
        this.first = first
        expected = records
        arrived = 0
        return gap
    }

    /** One message of the current chunk arrived (handed to the subscription or skipped by the client before its start). */
    fun arrived() {
        arrived++
    }
}
