package io.damoreira.kraken.messaging.store

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

private val payloadMapper: JsonMapper = JsonMapper.builder().build()

/**
 * The deepest payload every message store holds: MySQL's JSON limit (MySQL 8.4 refuses a document whose `JSON_DEPTH` exceeds
 * 100). Depth as `JSON_DEPTH` counts it: a scalar or an empty array or object is 1, a non-empty one is 1 + its deepest member.
 */
private const val MAX_DEPTH = 100

/**
 * The payload contract (both libraries', `store::check_outbox_payload` in kraken-messaging-rs): an event's payload in the outbox
 * ([OutboxEntry.payload]) and in the saga dispatch (an entry of kind `EVENT`) is a JSON object (UTF-8) whose integers all fit in
 * a signed 64-bit integer, which every message store holds and a Rust consumer (serde_json) can decode, so it also refuses:
 *
 * - a depth over 100, counted as MySQL's `JSON_DEPTH` (a scalar or an empty array or object is 1, a non-empty one is 1 + its
 *   deepest member: `{}` is 1, `{"a":1}` is 2, `{"a":[1]}` is 3): the payload contract is store-independent, and MySQL refuses
 *   deeper documents (serde_json's own limit, 128 nested arrays and objects, is looser);
 * - a number no double holds (`1e400`; one that merely rounds, `1e-400`, is fine);
 * - a lone surrogate escape (`"\ud800"`), in a value or a name (a pair, `"\ud83d\ude00"`, is fine);
 * - a leading byte order mark (and any bytes that are not UTF-8).
 *
 * Anything else (another JSON value, bytes that are not JSON, a larger integer anywhere in it) is refused too, all with an
 * [IllegalArgumentException] naming [what] (`outbox entry <id> (<type>)`, `saga event <id> (<type>)`).
 *
 * Every message store applies it before it appends, so the refusal is the same on all of them; a store may refuse more (what its
 * database cannot hold), never less.
 */
fun checkOutboxPayload(payload: ByteArray, what: String) {
    require(!(payload.size >= 3 && payload[0] == 0xEF.toByte() && payload[1] == 0xBB.toByte() && payload[2] == 0xBF.toByte())) {
        "The payload of $what starts with a byte order mark"
    }
    try {
        Charsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT).onUnmappableCharacter(CodingErrorAction.REPORT)
            .decode(ByteBuffer.wrap(payload))
    } catch (e: CharacterCodingException) {
        throw IllegalArgumentException("The payload of $what is not UTF-8", e)
    }
    val node = try {
        payloadMapper.readTree(payload)
    } catch (e: RuntimeException) {
        throw IllegalArgumentException("The payload of $what is not a JSON object: ${e.message}", e)
    }
    require(node != null && node.isObject) { "The payload of $what is not a JSON object" }
    check(node, 1, what)
}

/** Walks [node], at [depth] (the payload's object is 1), refusing what the contract does not allow. */
private fun check(node: JsonNode, depth: Int, what: String) {
    // Every node at depth d makes the document at least d deep (JSON_DEPTH is the deepest node's).
    require(depth <= MAX_DEPTH) { "The payload of $what has a depth over $MAX_DEPTH (as MySQL's JSON_DEPTH counts it)" }
    when {
        node.isContainer -> {
            if (node.isObject) {
                for ((name, value) in node.properties()) {
                    require(!hasLoneSurrogate(name)) { "The payload of $what has a name with a lone surrogate" }
                    check(value, depth + 1, what)
                }
            } else {
                for (value in node.values()) check(value, depth + 1, what)
            }
        }
        node.isIntegralNumber -> require(node.canConvertToLong()) { "The payload of $what has an integer outside the signed 64-bit range" }
        node.isFloatingPointNumber -> require(node.doubleValue().isFinite()) { "The payload of $what has a number no double holds" }
        node.isString -> require(!hasLoneSurrogate(node.asString())) { "The payload of $what has a string with a lone surrogate" }
    }
}

private fun hasLoneSurrogate(text: String): Boolean {
    var i = 0
    while (i < text.length) {
        val c = text[i]
        if (Character.isHighSurrogate(c)) {
            if (i + 1 >= text.length || !Character.isLowSurrogate(text[i + 1])) return true
            i += 2
            continue
        }
        if (Character.isLowSurrogate(c)) return true
        i++
    }
    return false
}
