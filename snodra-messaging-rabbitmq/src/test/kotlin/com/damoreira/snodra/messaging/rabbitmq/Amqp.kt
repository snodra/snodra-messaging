package com.damoreira.snodra.messaging.rabbitmq

/**
 * A reader of encoded AMQP 1.0 messages, section by section, with the type codes of what snodra's layout fixes: written for
 * the tests from the AMQP 1.0 specification (part 1, types; part 3, the message format), independent of the stream client's
 * codec.
 */
internal object Amqp {
    /** One AMQP value: its type and its body (a string's bytes, a list's or a map's elements). */
    sealed interface V {
        data object Null : V
        data class Str(val text: String) : V
        data class Sym(val text: String) : V
        data class Bin(val bytes: List<Byte>) : V
        data class Lst(val items: List<V>) : V
        data class MapV(val entries: List<Pair<V, V>>) : V
        data class Other(val code: Int) : V
    }

    const val ANNOTATIONS = 0x72
    const val PROPERTIES = 0x73
    const val APPLICATION_PROPERTIES = 0x74
    const val DATA = 0x75
    const val AMQP_VALUE = 0x77

    /** The sections of [bytes]: (descriptor code, value). */
    fun sections(bytes: ByteArray): List<Pair<Int, V>> {
        val r = Reader(bytes)
        val all = mutableListOf<Pair<Int, V>>()
        while (r.left() > 0) {
            check(r.u8() == 0x00 && r.u8() == 0x53) { "a small-ulong descriptor at ${r.at}" }
            val code = r.u8()
            all += code to r.value()
        }
        return all
    }

    fun section(bytes: ByteArray, code: Int): List<V> = sections(bytes).filter { it.first == code }.map { it.second }

    private class Reader(val b: ByteArray, var at: Int = 0, val end: Int = b.size) {
        fun left() = end - at
        fun u8(): Int = b[at++].toInt() and 0xFF
        fun u32(): Int = (u8() shl 24) or (u8() shl 16) or (u8() shl 8) or u8()
        fun take(n: Int): ByteArray = b.copyOfRange(at, at + n).also { at += n }

        fun value(): V = when (val code = u8()) {
            0x40 -> V.Null
            0xa1 -> V.Str(String(take(u8()), Charsets.UTF_8))
            0xb1 -> V.Str(String(take(u32()), Charsets.UTF_8))
            0xa3 -> V.Sym(String(take(u8()), Charsets.US_ASCII))
            0xb3 -> V.Sym(String(take(u32()), Charsets.US_ASCII))
            0xa0 -> V.Bin(take(u8()).toList())
            0xb0 -> V.Bin(take(u32()).toList())
            0x45 -> V.Lst(emptyList())
            0xc0, 0xc1, 0xd0, 0xd1 -> {
                val (size, count) = if (code < 0xd0) (u8() - 1) to u8() else (u32() - 4) to u32()
                val inner = Reader(b, at, at + size)
                at += size
                val items = (0 until count).map { inner.value() }
                if (code == 0xc1 || code == 0xd1) V.MapV(items.chunked(2).map { it[0] to it[1] }) else V.Lst(items)
            }
            0x83 -> V.Other(code).also { take(8) }
            else -> V.Other(code)
        }
    }
}
