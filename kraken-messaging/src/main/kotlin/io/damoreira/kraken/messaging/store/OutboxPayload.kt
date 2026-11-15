package io.damoreira.kraken.messaging.store

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.nio.ByteBuffer
import java.nio.charset.CharacterCodingException
import java.nio.charset.CodingErrorAction

private val payloadMapper: JsonMapper = JsonMapper.builder().build()

/** The most arrays and objects nested in one another a payload may hold (127: serde_json's limit of 128 refuses the 128th). */
private const val MAX_NESTING = 127

/**
 * The payload contract (both libraries', `store::check_outbox_payload` in kraken-messaging-rs): an event's payload in the outbox
 * ([OutboxEntry.payload]) and in the saga dispatch (an entry of kind `EVENT`) is a JSON object (UTF-8) whose integers all fit in
 * a signed 64-bit integer, and which a Rust consumer (serde_json) can decode, so it also refuses:
 *
 * - a number no double holds (`1e400`; one that merely rounds, `1e-400`, is fine);
 * - a lone surrogate escape (`"\ud800"`), in a value or a name (a pair, `"😀"`, is fine);
 * - 128 or more arrays and objects nested in one another, the payload's own object included (127 are fine);
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

/** Walks [node], [depth] containers deep, refusing what the contract does not allow. */
private fun check(node: JsonNode, depth: Int, what: String) {
    when {
        node.isContainer -> {
            require(depth <= MAX_NESTING) { "The payload of $what nests 128 or more arrays and objects" }
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
