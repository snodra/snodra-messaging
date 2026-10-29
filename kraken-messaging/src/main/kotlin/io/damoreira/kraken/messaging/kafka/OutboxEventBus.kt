package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.security.CallerContext
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import org.slf4j.MDC
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong

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
) : EventBus {
    private val seq = AtomicLong(System.currentTimeMillis() shl 20)

    override suspend fun publish(event: Event, key: String?) = insert(event, key)

    override fun publishBlocking(event: Event, key: String?) = insert(event, key)

    private fun insert(event: Event, key: String?) {
        val headers = linkedMapOf(
            EventHeaders.CORRELATION_ID to (MDC.get("correlationId") ?: UUID.randomUUID().toString()),
            EventHeaders.USER_ID to (CallerContext.currentUserId() ?: ""),
        )
        traceHeaders.inject(headers)
        mongo.insert(
            OutboxDocument(
                id = UUID.randomUUID().toString(),
                type = typeResolver.typeNameOf(event::class),
                key = key,
                headers = headers,
                payload = org.bson.Document.parse(String(codec.encode(event), Charsets.UTF_8)),
                createdAt = Instant.now(),
                seq = seq.incrementAndGet(),
            ),
        )
    }
}
