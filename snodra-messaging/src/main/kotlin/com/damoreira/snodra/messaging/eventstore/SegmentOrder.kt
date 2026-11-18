package com.damoreira.snodra.messaging.eventstore

/**
 * Segment order, the one every list of segments is in ([Subscription.segments], [SubscriptionAdmin.plan], the segments of an
 * assignment), the hub's: names of ASCII digits only first, by numeric value (compared as digit strings, so no overflow:
 * segment 10 follows 9), then every other name by its UTF-8 bytes.
 */
object SegmentOrder : Comparator<String> {
    override fun compare(a: String, b: String): Int {
        val x = digits(a)
        val y = digits(b)
        return when {
            x != null && y != null -> if (x.length != y.length) x.length.compareTo(y.length) else x.compareTo(y).let { if (it != 0) it else a.compareTo(b) }
            x != null -> -1
            y != null -> 1
            else -> compareBytes(a.toByteArray(Charsets.UTF_8), b.toByteArray(Charsets.UTF_8))
        }
    }

    /** [name] without its leading zeros when it is only ASCII digits (and not empty), else null. */
    private fun digits(name: String): String? =
        if (name.isNotEmpty() && name.all { it in '0'..'9' }) name.trimStart('0') else null

    private fun compareBytes(a: ByteArray, b: ByteArray): Int {
        for (i in 0 until minOf(a.size, b.size)) {
            val d = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
            if (d != 0) return d
        }
        return a.size - b.size
    }
}
