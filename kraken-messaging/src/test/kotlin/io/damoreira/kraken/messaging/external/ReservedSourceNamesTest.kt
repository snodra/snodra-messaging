package io.damoreira.kraken.messaging.external

import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.mockk.mockk
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import io.damoreira.kraken.messaging.store.ProcessedEventStore
import kotlin.test.assertTrue

@MessageType("rs.Thing")
data class RsThing(val id: String) : Event

/**
 * External source names that would collide with a marker family (ADR 0021 `saga:`, `ingress:`, `failed:`; ADR 0022
 * `processor:`) are refused: a partner source at startup, a webhook source at the call.
 */
class ReservedSourceNamesTest {
    private val translator = ExternalRecordTranslator { null }

    @Test fun `a partner source named like a marker family fails the startup naming it`() {
        for (name in listOf("processor", "saga", "ingress", "failed")) {
            val e = assertThrows<IllegalStateException> {
                ExternalSources(
                    sources = mapOf(name to mockk<ExternalSourceProvider>(relaxed = true)),
                    translators = mapOf(name to translator),
                    dispatcher = mockk<LocalEventDispatcher>(relaxed = true),
                    codec = JsonCodec(), typeResolver = MessageTypeResolver("rs"),
                )
            }
            assertTrue(e.message!!.contains("external source '$name'"), e.message)
            assertTrue(e.message!!.contains("reserved"), e.message)
        }
    }

    @Test fun `a webhook source named like a marker family is refused`() {
        val ingress = ExternalEventIngress(mockk<LocalEventDispatcher>(relaxed = true), mockk<ProcessedEventStore>(relaxed = true), JsonCodec(), MessageTypeResolver("rs"))
        for (name in listOf("processor", "saga", "ingress", "failed")) {
            val e = assertThrows<IllegalArgumentException> { ingress.accept(name, "k-1", RsThing("1")) }
            assertTrue(e.message!!.contains(name) && e.message!!.contains("reserved"), e.message)
        }
    }
}
