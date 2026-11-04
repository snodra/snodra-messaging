package io.damoreira.kraken.examples.b

import io.damoreira.kraken.messaging.handler.ProcessingGroup
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

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
}
