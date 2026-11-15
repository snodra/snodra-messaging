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
}
