package com.damoreira.snodra.messaging.test

import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.deadletter.DeadLetter
import com.damoreira.snodra.messaging.eventstore.EventHeaders
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
import com.damoreira.snodra.messaging.eventstore.memory.MemoryEventStore
import com.damoreira.snodra.messaging.eventstore.memory.StoredEvent
import com.damoreira.snodra.messaging.handler.Processors
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import kotlinx.coroutines.runBlocking
import java.time.Clock
import java.time.Duration
import java.util.UUID
import kotlin.reflect.KClass

/**
 * What a zero-infrastructure test ([SnodraInMemory]) does with the service's events: [publish] one to the service as another
 * service would (its processors and sagas get it), read what the service [published], and see its processors' [deadLetters] and
 * which events they [processed]. The `await` variants poll until the condition holds, at most a timeout (10 s by default), and
 * fail with what they saw.
 *
 * Built over the application's [MemoryEventStore] and [MemoryMessageStore]; [service] is the service's name (the `source`
 * header of what it publishes), [source] its events source.
 */
class InMemoryEvents(
    val events: MemoryEventStore,
    val store: MemoryMessageStore,
    private val service: String,
    private val source: String,
    private val codec: JsonCodec = JsonCodec(),
    private val typeResolver: MessageTypeResolver,
    /** The application clock: it stamps the `timestamp` header of what [publish] sends. */
    private val clock: Clock = Clock.systemUTC(),
) {
    /**
     * Publishes [event] to the service's events source as service [from] would (the seven headers, a fresh `id`, [headers] on
     * top), with [key] as its order key; returns its id.
     */
    fun publish(event: Event, key: String? = null, headers: Map<String, String> = emptyMap(), from: String = "test"): String {
        val id = UUID.randomUUID().toString()
        val type = typeResolver.typeNameOf(event::class)
        val trace = UUID.randomUUID().toString().replace("-", "")
        val all = linkedMapOf(
            EventHeaders.TYPE to type,
            EventHeaders.ID to id,
            EventHeaders.CORRELATION_ID to id,
            EventHeaders.TIMESTAMP to EventHeaders.timestamp(clock.instant()),
            EventHeaders.SOURCE to from,
            EventHeaders.USER_ID to "",
            EventHeaders.TRACEPARENT to "00-$trace-${trace.take(16)}-01",
        )
        all += headers
        runBlocking { events.publish(OutgoingEvent(source, id, type, key, all, codec.encode(event))) }
        return id
    }

    /** What the service published (header `source` = the service), oldest first. */
    fun published(): List<StoredEvent> = events.events(source).filter { it.headers[EventHeaders.SOURCE] == service }

    /** The events of [type] the service published, decoded, oldest first. */
    fun <T : Any> published(type: KClass<T>): List<T> {
        val name = typeResolver.typeNameOf(type)
        return published().filter { it.type == name }.map { codec.decode(it.payload, type) }
    }

    /** Waits until the service published at least [count] events of [type]; returns them all. */
    fun <T : Any> awaitPublished(type: KClass<T>, count: Int = 1, timeout: Duration = TIMEOUT): List<T> =
        await("${type.simpleName} published $count time(s)", timeout, { published(type) }) { it.size >= count }

    /** The dead letters of [processor] (every processor's when null), by `parkedAt`. */
    fun deadLetters(processor: String? = null): List<DeadLetter> = runBlocking { store.deadLetters.list(processor, Int.MAX_VALUE) }

    /** Waits until [processor] has at least [count] dead letters (every processor's when null); returns them all. */
    fun awaitDeadLetters(processor: String? = null, count: Int = 1, timeout: Duration = TIMEOUT): List<DeadLetter> =
        await("${count} dead letter(s) of ${processor ?: "any processor"}", timeout, { deadLetters(processor) }) { it.size >= count }

    /** Whether [processor] (`default` unless named by `@ProcessingGroup`) handled the event [eventId] (its marker is committed). */
    fun processed(eventId: String, processor: String = Processors.DEFAULT): Boolean =
        runBlocking { store.processedEvents.exists(Processors.markerId(processor, eventId)) }

    /** Waits until [processor] handled the event [eventId]. */
    fun awaitProcessed(eventId: String, processor: String = Processors.DEFAULT, timeout: Duration = TIMEOUT) {
        await("event $eventId processed by $processor", timeout, { processed(eventId, processor) }) { it }
    }

    private fun <T> await(what: String, timeout: Duration, read: () -> T, done: (T) -> Boolean): T {
        val deadline = System.nanoTime() + timeout.toNanos()
        while (true) {
            val seen = read()
            if (done(seen)) return seen
            if (System.nanoTime() > deadline) throw AssertionError("$what within $timeout; saw $seen")
            Thread.sleep(POLL_MS)
        }
    }

    private companion object {
        val TIMEOUT: Duration = Duration.ofSeconds(10)
        const val POLL_MS = 20L
    }
}
