package io.damoreira.kraken.messaging.codec

import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.fixtures.Count
import io.damoreira.kraken.messaging.fixtures.NoAnnotation
import io.damoreira.kraken.messaging.fixtures.Ping
import io.damoreira.kraken.messaging.fixtures.Pong
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant
import kotlin.test.assertEquals

class CodecTest {
    private val codec = JsonCodec()

    @Test fun `annotation wins over prefix`() =
        assertEquals("test.Ping", MessageTypeResolver("svc").typeNameOf(Ping::class))

    @Test fun `prefix plus simple name when unannotated`() =
        assertEquals("svc.NoAnnotation", MessageTypeResolver("svc").typeNameOf(NoAnnotation::class))

    @Test fun `instants are iso utc and unknown fields ignored`() {
        val bytes = codec.encode(Pong("hi", Instant.parse("2026-10-28T14:03:22Z")))
        assertEquals("""{"echoed":"hi","at":"2026-10-28T14:03:22Z"}""", String(bytes))
        assertEquals(
            Pong("hi", Instant.parse("2026-10-28T14:03:22Z")),
            codec.decode("""{"echoed":"hi","at":"2026-10-28T14:03:22Z","extra":1}""".toByteArray(), Pong::class),
        )
    }

    @Test fun `unit encodes as empty object and decodes back`() {
        assertEquals("{}", String(codec.encode(Unit)))
        assertEquals(Unit, codec.decode("{}".toByteArray(), Unit::class))
    }

    @Test fun `decode failure is InvalidMessageException`() {
        val e = assertThrows<InvalidMessageException> { codec.decode("{}".toByteArray(), Count::class) }
        assertEquals("Cannot decode payload as ${Count::class.qualifiedName}", e.message)
    }

    @Test fun `json null literal is InvalidMessageException`() {
        assertThrows<InvalidMessageException> { codec.decode("null".toByteArray(), Pong::class) }
        assertThrows<InvalidMessageException> { codec.decode("null".toByteArray(), Unit::class) }
    }

    @Test fun `response type read from Command supertype`() =
        assertEquals(Pong::class, ResponseTypes.responseTypeOf(Ping::class))

    @Test fun `response type of unit command is Unit`() =
        assertEquals(Unit::class, ResponseTypes.responseTypeOf(NoAnnotation::class))

    @Test fun `plain class has no response type`() {
        assertThrows<IllegalArgumentException> { ResponseTypes.responseTypeOf(Pong::class) }
    }
}
