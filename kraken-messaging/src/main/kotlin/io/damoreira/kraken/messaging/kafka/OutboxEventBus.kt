package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.saga.RawEventPublisher
import io.damoreira.kraken.messaging.security.CallerContext
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import kotlinx.coroutines.currentCoroutineContext
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import kotlin.coroutines.CoroutineContext

/**
 * Ordering per key is guaranteed for events published by one service instance (the per-process [seq] counter breaks
 * `createdAt` millisecond ties); across instances the outbox never promised it.
 *
 * Writes events to the `outbox` collection; [OutboxPublisher] relays them to Kafka. The insert joins a thread-bound
 * Mongo transaction when one is active (so the event commits or rolls back with the business write).
 */
class OutboxEventBus(
    private val mongo: MongoTemplate,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val traceHeaders: TraceHeaders,
) : EventBus, RawEventPublisher {
    private val seq = AtomicLong(System.currentTimeMillis() shl 20)

    override suspend fun publish(event: Event, key: String?) = insert(event, key, currentCoroutineContext())

    override fun publishBlocking(event: Event, key: String?) = insert(event, key, null)

    /** A saga's recorded event: no caller (so an empty `userId` header) and the correlation id of the message that caused it. */
    override fun publishRaw(type: String, key: String?, payload: ByteArray, correlationId: String) =
        insertDocument(type, key, org.bson.Document.parse(String(payload, Charsets.UTF_8)), correlationId, userId = "")

    /** The correlation id comes from [context]'s [CorrelationContext], else the MDC, else a fresh UUID. */
    private fun insert(event: Event, key: String?, context: CoroutineContext?) = insertDocument(
        typeResolver.typeNameOf(event::class), key, org.bson.Document.parse(String(codec.encode(event), Charsets.UTF_8)),
        CorrelationContext.resolve(context), CallerContext.currentUserId() ?: "",
    )

    private fun insertDocument(type: String, key: String?, payload: org.bson.Document, correlationId: String, userId: String) {
        val headers = linkedMapOf(
            EventHeaders.CORRELATION_ID to correlationId,
            EventHeaders.USER_ID to userId,
        )
        traceHeaders.inject(headers)
        mongo.insert(
            OutboxDocument(
                id = UUID.randomUUID().toString(),
                type = type,
                key = key,
                headers = headers,
                payload = payload,
                createdAt = Instant.now(),
                seq = seq.incrementAndGet(),
            ),
        )
    }
}
