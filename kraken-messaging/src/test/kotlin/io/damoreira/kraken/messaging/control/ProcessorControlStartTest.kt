package io.damoreira.kraken.messaging.control

import io.damoreira.kraken.messaging.deadletter.DeadLetters
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.kafka.EventConsumer
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

/** Starting without the `processor_control` documents (P6b): a consumer starts only once its document could be read. */
class ProcessorControlStartTest {
    @Test fun `a document that cannot be read at start keeps the consumer stopped until a poll reads it`() {
        val readable = AtomicBoolean(false)
        val store = mockk<ProcessorControlStore>()
        every { store.read(any()) } answers { if (readable.get()) emptyMap() else throw IllegalStateException("MongoDB is down") }
        val running = AtomicBoolean(false)
        val consumer = mockk<EventConsumer>(relaxed = true)
        every { consumer.processor } returns "audit"
        every { consumer.paused } returns false
        every { consumer.isRunning } answers { running.get() }
        every { consumer.start() } answers { running.set(true) }
        val control = ProcessorControl(store, mockk<HandlerRegistry>(), mockk<DeadLetters>(), emptyMap(), pollInterval = Duration.ofMillis(100))
        try {
            control.attach(listOf(consumer)) // does not throw: the service starts
            Thread.sleep(400)
            verify(exactly = 0) { consumer.start() }
            readable.set(true)
            Thread.sleep(400)
            verify(exactly = 1) { consumer.start() }
        } finally {
            control.detach()
        }
    }
}
