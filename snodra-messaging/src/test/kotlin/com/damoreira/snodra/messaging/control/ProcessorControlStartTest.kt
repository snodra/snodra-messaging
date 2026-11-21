package com.damoreira.snodra.messaging.control

import com.damoreira.snodra.messaging.deadletter.DeadLetters
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.events.EventConsumer
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.store.ProcessorControlStore
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean

/** Starting without the `processor_control` documents: a consumer starts only once its document could be read. */
class ProcessorControlStartTest {
    @Test fun `a document that cannot be read at start keeps the consumer stopped until a poll reads it`() {
        val readable = AtomicBoolean(false)
        val store = mockk<ProcessorControlStore>()
        coEvery { store.read(any()) } answers { if (readable.get()) emptyMap() else throw IllegalStateException("MongoDB is down") }
        val running = AtomicBoolean(false)
        val consumer = mockk<EventConsumer>(relaxed = true)
        every { consumer.processor } returns "audit"
        every { consumer.paused } returns false
        every { consumer.isRunning } answers { running.get() }
        every { consumer.start(any()) } answers { running.set(true) }
        val control = ProcessorControl(store, mockk<HandlerRegistry>(), mockk<DeadLetters>(), mockk<EventStore>(relaxed = true), pollInterval = Duration.ofMillis(100))
        try {
            control.attach(listOf(consumer)) // does not throw: the service starts
            Thread.sleep(400)
            verify(exactly = 0) { consumer.start(any()) }
            readable.set(true)
            Thread.sleep(400)
            // The control plane never waits for a previous reader: it starts without waiting, and retries at its next poll.
            verify(exactly = 1) { consumer.start(waitForPrevious = false) }
            verify(exactly = 0) { consumer.start() }
        } finally {
            control.detach()
        }
    }
}
