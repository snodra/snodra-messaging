package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.control.ReplayRange
import io.damoreira.kraken.messaging.control.ResetContext
import io.damoreira.kraken.messaging.control.ResetTarget
import io.damoreira.kraken.messaging.handler.DisallowReplay
import io.damoreira.kraken.messaging.handler.ProcessingGroup
import io.damoreira.kraken.messaging.handler.ResetHandler
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class AuditHandlerTest {
    private fun note(key: String, text: String) = AuditNote(key, text, Instant.EPOCH)

    @Test fun `the handler is in the named processor audit`() {
        assertEquals("audit", AuditHandler::class.java.getAnnotation(ProcessingGroup::class.java).value)
    }

    @Test fun `a note is recorded in the order it was handled`() {
        val audit = AuditHandler()

        audit.on(note("k1", "first"))
        audit.on(note("k2", "other"))
        audit.on(note("k1", "second"))

        assertEquals(listOf("first", "other", "second"), audit.handled.map { it.text })
    }

    @Test fun `while failing a poison note fails and is not recorded, then it works again`() {
        val audit = AuditHandler()
        audit.failing = true

        val failure = assertFailsWith<IllegalStateException> { audit.on(note("k1", "poison-1")) }
        assertEquals("the audit processor is failing on demand (key=k1 text=poison-1)", failure.message)
        assertEquals(emptyList(), audit.handled)

        audit.failing = false
        audit.on(note("k1", "poison-1"))
        assertEquals(listOf("poison-1"), audit.handled.map { it.text })
    }

    @Test fun `while failing the other notes are handled, whatever their key`() {
        val audit = AuditHandler()
        audit.failing = true

        audit.on(note("k2", "fine"))
        audit.on(note("k1", "not poisoned"))

        assertEquals(listOf("fine", "not poisoned"), audit.handled.map { it.text })
    }

    @Test fun `a reset of the audit processor is recorded with its target and ranges (P6b)`() {
        val audit = AuditHandler()
        assertTrue(AuditHandler::class.java.getMethod("onReset", ResetContext::class.java).isAnnotationPresent(ResetHandler::class.java))
        audit.onReset(ResetContext("audit", "op-1", ResetTarget.Earliest, listOf(ReplayRange("kraken.events", 0, 0, 3))))
        audit.onReset(ResetContext("audit", "op-2", ResetTarget.Timestamp(42), listOf(ReplayRange("kraken.events", 1, 2, 5))))
        assertEquals(
            listOf(
                AuditReset("op-1", "earliest", listOf(AuditRange(0, 0, 3))),
                AuditReset("op-2", "timestamp 42", listOf(AuditRange(1, 2, 5))),
            ),
            audit.resets,
        )
    }

    @Test fun `the notifier is in audit, never replays and records each note it notified (P6b)`() {
        assertEquals("audit", AuditNotifier::class.java.getAnnotation(ProcessingGroup::class.java).value)
        assertTrue(AuditNotifier::class.java.getMethod("on", AuditNote::class.java).isAnnotationPresent(DisallowReplay::class.java))
        val notifier = AuditNotifier()
        notifier.on(note("k1", "first"))
        assertEquals(listOf("first"), notifier.notified.map { it.text })
    }
}
