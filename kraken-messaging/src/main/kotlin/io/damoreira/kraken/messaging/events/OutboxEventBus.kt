package io.damoreira.kraken.messaging.events

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.eventstore.EventHeaders
import io.damoreira.kraken.messaging.saga.RawEventPublisher
import io.damoreira.kraken.messaging.security.CallerContext
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.OutboxStore
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * Ordering per key is guaranteed for events published by one service instance (the per-process [seq] counter breaks
 * `createdAt` millisecond ties); across instances the outbox never promised it.
 *
 * Appends events to the message store's [outbox]; [OutboxPublisher] relays them to the event store. The append joins the Spring
 * transaction active on the calling thread (so the event commits or rolls back with the business write). With the in-memory
 * message store and a transaction of another manager (the application's own database), the event is appended after that
 * transaction committed and never when it rolled back: after the commit, not atomically with it (a failure of that late append
 * is logged and cannot undo the commit). The blocking
 * entry points ([publishBlocking], [publishRaw]) run the append on the calling thread too.
 */
class OutboxEventBus(
    private val outbox: OutboxStore,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val traceHeaders: TraceHeaders,
) : EventBus, RawEventPublisher {
    private val seq = AtomicLong(System.currentTimeMillis() shl 20)

    override suspend fun publish(event: Event, key: String?) = outbox.append(entry(event, key, currentCoroutineContext()))

    override fun publishBlocking(event: Event, key: String?) = runBlocking { outbox.append(entry(event, key, null)) }

    /** A saga's recorded event: no caller (so an empty `userId` header) and the correlation id of the message that caused it. */
    override fun publishRaw(type: String, key: String?, payload: ByteArray, correlationId: String) =
        runBlocking { outbox.append(entry(type, key, payload, correlationId, userId = "")) }

    /** The correlation id comes from [context]'s [CorrelationContext], else the MDC, else a fresh UUID. */
    private fun entry(event: Event, key: String?, context: CoroutineContext?) = entry(
        typeResolver.typeNameOf(event::class), key, codec.encode(event), CorrelationContext.resolve(context), CallerContext.currentUserId() ?: "", context,
    )

    private fun entry(type: String, key: String?, payload: ByteArray, correlationId: String, userId: String, context: CoroutineContext? = null): OutboxEntry {
        val headers = linkedMapOf(
            EventHeaders.CORRELATION_ID to correlationId,
            EventHeaders.USER_ID to userId,
        )
        traceHeaders.inject(headers, context)
        return OutboxEntry(
            id = UUID.randomUUID().toString(),
            type = type,
            key = key,
            headers = headers,
            payload = payload,
            createdAt = Instant.now(),
            seq = seq.incrementAndGet(),
        )
    }
}
