package io.damoreira.kraken.messaging.store

import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/** The payload contract every message store applies to the outbox and the saga dispatch, through one check. */
class OutboxPayloadTest {
    @Test fun `a JSON object whose integers fit in a signed 64-bit integer is accepted`() {
        checkOutboxPayload("""{"a":1,"b":[9223372036854775807,-9223372036854775808],"c":{"d":"x"},"e":1.5}""".toByteArray(), "event e-1")
        checkOutboxPayload("{}".toByteArray(), "event e-1")
    }

    @Test fun `anything else is refused naming what it was`() {
        for (bad in listOf("[]", "1", "\"x\"", "null", "", "{", "not json", """{"a":9223372036854775808}""", """{"a":[{"b":-9223372036854775809}]}""")) {
            val e = assertFailsWith<IllegalArgumentException>(bad) { checkOutboxPayload(bad.toByteArray(), "outbox entry e-7 (orders.Placed)") }
            assertTrue("outbox entry e-7 (orders.Placed)" in e.message!!, "$bad: ${e.message}")
        }
    }

    @Test fun `what a Rust consumer cannot decode is refused too, each saying why`() {
        val cases = mapOf(
            """{"a":1e400}""".toByteArray() to "double",
            """{"a":"\ud800"}""".toByteArray() to "surrogate",
            """{"\udfff":1}""".toByteArray() to "surrogate",
            ("""{"a":""" + "[".repeat(127) + "1" + "]".repeat(127) + "}").toByteArray() to "128",
            byteArrayOf(0xEF.toByte(), 0xBB.toByte(), 0xBF.toByte()) + "{}".toByteArray() to "byte order mark",
            byteArrayOf('{'.code.toByte(), '"'.code.toByte(), 0xED.toByte(), 0xA0.toByte(), 0x80.toByte(), '"'.code.toByte(), ':'.code.toByte(), '1'.code.toByte(), '}'.code.toByte()) to "UTF-8",
        )
        for ((payload, why) in cases) {
            val e = assertFailsWith<IllegalArgumentException>(String(payload)) { checkOutboxPayload(payload, "event e-9") }
            assertTrue("event e-9" in e.message!! && why in e.message!!, "${String(payload).take(60)}: ${e.message}")
        }
        // At the bounds: 127 nested containers, a pair, a tiny number that rounds to 0.
        checkOutboxPayload(("""{"a":""" + "[".repeat(126) + "1" + "]".repeat(126) + ""","p":"\ud83d\ude00","t":1e-400}""").toByteArray(), "event e-9")
    }
}
