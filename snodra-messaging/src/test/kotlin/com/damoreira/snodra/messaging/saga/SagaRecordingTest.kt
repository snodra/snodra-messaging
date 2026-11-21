package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import java.time.Instant
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class SagaRecordingTest {
    private fun commands() = RecordingSagaCommands("s1", 2, "c1", Instant.EPOCH, JsonCodec(), MessageTypeResolver("rt"))

    @Test
    fun `a handling records at most 999 messages`() {
        val recording = commands()
        repeat(999) { recording.publish(RtNote("n$it")) }
        assertEquals(2000L, recording.entries.first().seq)
        assertEquals(2998L, recording.entries.last().seq)

        val refused = assertFailsWith<IllegalStateException> { recording.publish(RtNote("one too many")) }
        assertTrue("999" in refused.message.orEmpty(), "the message names the limit: ${refused.message}")
        assertEquals(999, recording.entries.size, "the refused message is not recorded")
    }
}
