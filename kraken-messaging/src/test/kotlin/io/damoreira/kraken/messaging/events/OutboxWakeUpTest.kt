package io.damoreira.kraken.messaging.events

import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.memory.MemoryMessageStore
import kotlinx.coroutines.runBlocking
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.Test
import java.time.Duration
import java.time.Instant
import kotlin.test.assertEquals

/** A store that tells of appends ([io.damoreira.kraken.messaging.store.OutboxStore.onAppend]) has them relayed at once, not at the next poll. */
class OutboxWakeUpTest {
    @Test
    fun anAppendTheStoreToldOfIsRelayedWithoutWaitingForThePoll() {
        val store = MemoryMessageStore()
        val events = MemoryEventStore()
        val relay = OutboxPublisher(store.outbox, events, "kraken.events", "orders", pollInterval = Duration.ofMinutes(10), batchSize = 100)
        relay.start()
        try {
            Thread.sleep(200) // the relay's first poll found nothing: it waits its ten minutes now
            runBlocking {
                store.outbox.append(OutboxEntry("e-1", "t.Happened", "k", emptyMap(), """{"id":"e-1"}""".toByteArray(), Instant.now(), 1))
            }
            await().atMost(Duration.ofSeconds(5)).until { events.events("kraken.events").map { it.id } == listOf("e-1") }
        } finally {
            relay.stop()
        }
    }

    @Test
    fun aRelayListensForAppendsOnlyWhileItRuns() {
        val store = MemoryMessageStore()
        val relay = OutboxPublisher(store.outbox, MemoryEventStore(), "kraken.events", "orders", pollInterval = Duration.ofMinutes(10), batchSize = 100)
        assertEquals(0, store.appendListenerCount(), "a relay that never started listens to nothing")
        relay.start()
        assertEquals(1, store.appendListenerCount())
        relay.stop()
        assertEquals(0, store.appendListenerCount(), "no listener outlives its relay")
        relay.start()
        relay.stop()
        assertEquals(0, store.appendListenerCount(), "nor a restarted one")
    }
}
