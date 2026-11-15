package io.damoreira.kraken.messaging.store

import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper

private val payloadMapper: JsonMapper = JsonMapper.builder().build()

/**
 * The payload contract (both libraries', `store::check_outbox_payload` in kraken-messaging-rs): an event's payload in the outbox
 * ([OutboxEntry.payload]) and in the saga dispatch (an entry of kind `EVENT`) is a JSON object (UTF-8) whose integers
 * all fit in a signed 64-bit integer. Anything else (another JSON value, bytes that are not JSON, a larger integer anywhere in it)
 * is refused with an [IllegalArgumentException] naming [what] (`outbox entry <id> (<type>)`, `saga event <id> (<type>)`).
 *
 * Every message store applies it before it appends, so the refusal is the same on all of them; a store may refuse more (what its
 * database cannot hold), never less.
 */
fun checkOutboxPayload(payload: ByteArray, what: String) {
    val node = try {
        payloadMapper.readTree(payload)
    } catch (e: RuntimeException) {
        throw IllegalArgumentException("The payload of $what is not a JSON object: ${e.message}", e)
    }
    require(node != null && node.isObject) { "The payload of $what is not a JSON object" }
    require(fitsLong(node)) { "The payload of $what has an integer outside the signed 64-bit range" }
}

private fun fitsLong(node: JsonNode): Boolean = if (node.isIntegralNumber) node.canConvertToLong() else node.values().all(::fitsLong)
