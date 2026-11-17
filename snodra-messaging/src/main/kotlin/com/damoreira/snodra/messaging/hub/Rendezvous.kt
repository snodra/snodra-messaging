package com.damoreira.snodra.messaging.hub

/**
 * Rendezvous (highest random weight) hashing of a routing key over hub ids (ADR 0016): a keyed request goes to the live
 * hub with the highest `score(hubId, key)`, so a key keeps its hub while the set of hubs is stable and only the keys of
 * a hub that leaves move.
 *
 * `score` = `fmix64(fnv1a64(utf8(hubId + "\u0000" + key)))`, compared as an unsigned 64-bit number; equal scores go to
 * the smaller hub id (UTF-16 order, which equals byte order for ASCII ids).
 *
 * - `fnv1a64`: 64-bit FNV-1a, offset basis `0xcbf29ce484222325`, prime `0x100000001b3`, one byte at a time.
 * - `fmix64`: MurmurHash3's 64-bit finalizer (`k ^= k >>> 33; k *= 0xff51afd7ed558ccd; k ^= k >>> 33;
 *   k *= 0xc4ceb9fe1a85ec53; k ^= k >>> 33`). Plain FNV-1a spreads keys that differ only in their last characters
 *   (`order-1`, `order-2`, ...) unevenly (about 30/70 over two hubs); the finalizer evens that out.
 *
 * The Rust library implements the same function; `RendezvousTest` holds the shared reference vectors.
 */
object Rendezvous {
    private const val OFFSET_BASIS = -0x340d631b7bdddcdbL // 0xcbf29ce484222325
    private const val PRIME = 0x100000001b3L

    fun fnv1a64(text: String): Long {
        var hash = OFFSET_BASIS
        for (byte in text.toByteArray(Charsets.UTF_8)) {
            hash = hash xor (byte.toLong() and 0xff)
            hash *= PRIME
        }
        return hash
    }

    fun fmix64(value: Long): Long {
        var k = value
        k = k xor (k ushr 33)
        k *= -0xae502812aa7333L // 0xff51afd7ed558ccd
        k = k xor (k ushr 33)
        k *= -0x3b314601e57a13adL // 0xc4ceb9fe1a85ec53
        k = k xor (k ushr 33)
        return k
    }

    fun score(hubId: String, key: String): Long = fmix64(fnv1a64(hubId + "\u0000" + key))

    /** The candidate whose id scores highest for [key]; null when there is none. */
    fun <T> choose(key: String, candidates: Collection<T>, idOf: (T) -> String): T? {
        var best: T? = null
        var bestId = ""
        var bestScore = 0L
        for (candidate in candidates) {
            val id = idOf(candidate)
            val s = score(id, key)
            val better = best == null || java.lang.Long.compareUnsigned(s, bestScore).let { it > 0 || (it == 0 && id < bestId) }
            if (better) {
                best = candidate
                bestId = id
                bestScore = s
            }
        }
        return best
    }
}
