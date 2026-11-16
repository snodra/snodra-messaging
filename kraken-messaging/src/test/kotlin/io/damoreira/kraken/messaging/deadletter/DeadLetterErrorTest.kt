package io.damoreira.kraken.messaging.deadletter

import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DeadLetterErrorTest {
    @Test fun `an error message is cut at 2000 code points without splitting a pair`() {
        val long = "x".repeat(1999) + "😀" + "tail"
        val cut = DeadLetterError("E", long).message
        assertEquals(2000, cut.codePointCount(0, cut.length))
        assertTrue(cut.endsWith("😀"))
    }
}
