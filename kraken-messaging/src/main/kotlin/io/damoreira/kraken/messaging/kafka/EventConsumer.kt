package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.listener.AcknowledgingMessageListener
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.kafka.support.Acknowledgment
import org.springframework.util.backoff.FixedBackOff
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Consumes one event topic with at-least-once delivery and exactly-once handler effects.
 *
 * Per record: the idempotency marker ([ProcessedEventDocument], keyed by the `id` header) is inserted and the handlers
 * run inside one Mongo transaction; the offset is acknowledged only after the transaction committed. A duplicate id
 * (`DuplicateKeyException`) is acknowledged and skipped. A handler failure rolls the marker back and the record is
 * redelivered by a [DefaultErrorHandler] up to [maxAttempts] invocations in total, then logged (never the payload)
 * and skipped with its offset committed.
 *
 * Handlers run via `runBlocking` on the listener thread so the thread-bound Mongo transaction wraps them; handlers
 * that switch threads (other dispatchers, `withContext`) lose the transaction and their writes are not rolled back.
 *
 * Records without a `type` header, without a registered handler, or with a payload that is not a JSON object are
 * acknowledged and skipped (they cannot become valid by retrying).
 *
 * [backOffMs] is the delay between redeliveries (default 1000 ms; lowered in tests).
 */
class EventConsumer(
    consumerFactory: ConsumerFactory<String, ByteArray>,
    private val topic: String,
    private val groupId: String,
    private val registry: HandlerRegistry,
    private val invoker: HandlerInvoker,
    private val tx: TransactionTemplate,
    private val mongo: MongoTemplate,
    private val maxAttempts: Int,
    private val backOffMs: Long = 1000L,
    private val codec: JsonCodec = JsonCodec(),
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(EventConsumer::class.java)
    private val factory = consumerFactory
    private val running = AtomicBoolean(false)
    @Volatile private var container: ConcurrentMessageListenerContainer<String, ByteArray>? = null

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
    }

    @Synchronized
    override fun start() {
        if (!running.compareAndSet(false, true)) return
        val props = ContainerProperties(topic).apply {
            setGroupId(this@EventConsumer.groupId)
            setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE)
            setMessageListener(AcknowledgingMessageListener<String, ByteArray> { record, ack -> handle(record, ack!!) })
        }
        val errorHandler = DefaultErrorHandler(
            { record, ex -> logSkipped(record, ex) },
            FixedBackOff(backOffMs, (maxAttempts - 1).toLong()),
        ).apply { setCommitRecovered(true) }
        container = ConcurrentMessageListenerContainer(factory, props).apply {
            concurrency = 1
            setCommonErrorHandler(errorHandler)
            start()
        }
        log.info("EventConsumer started on topic {} group {} handling {} event types", topic, groupId, registry.eventTypes().size)
    }

    @Synchronized
    override fun stop() {
        if (!running.compareAndSet(true, false)) return
        container?.stop()
        container = null
    }

    internal fun groupIdForTest(): String = groupId

    override fun isRunning(): Boolean = running.get()
    override fun isAutoStartup(): Boolean = true
    override fun getPhase(): Int = Int.MAX_VALUE - 50

    private fun handle(record: ConsumerRecord<String, ByteArray>, ack: Acknowledgment) {
        val headers = record.headers().associate { it.key() to String(it.value() ?: ByteArray(0), Charsets.UTF_8) }
        val type = headers[EventHeaders.TYPE]
        if (type.isNullOrBlank()) {
            log.warn("Skipping record without type header at {}-{}@{}", record.topic(), record.partition(), record.offset())
            ack.acknowledge(); return
        }
        if (registry.eventHandlers(type).isEmpty()) {
            ack.acknowledge(); return
        }
        val id = headers[EventHeaders.ID]?.takeIf { it.isNotBlank() } ?: "${record.topic()}-${record.partition()}-${record.offset()}".also {
            log.warn("Record of type {} at {}-{}@{} has no id header, using {} for idempotency", type, record.topic(), record.partition(), record.offset(), it)
        }
        val payload = record.value() ?: ByteArray(0)
        val isObject = try { codec.decodeTree(payload).isObject } catch (_: Exception) { false }
        if (!isObject) {
            log.warn("Skipping record {} of type {} at {}-{}@{}: payload is not a JSON object", id, type, record.topic(), record.partition(), record.offset())
            ack.acknowledge(); return
        }
        val ctx = MessageContext(
            messageId = id,
            type = type,
            correlationId = headers[EventHeaders.CORRELATION_ID] ?: id,
            userId = headers[EventHeaders.USER_ID]?.takeIf { it.isNotBlank() },
            roles = emptySet(),
            authorization = null,
            headers = headers,
        )
        try {
            tx.executeWithoutResult {
                mongo.insert(ProcessedEventDocument(id, type, Instant.now()))
                runBlocking { invoker.invokeEvent(type, payload, ctx) }
            }
        } catch (_: DuplicateKeyException) {
            log.debug("Event {} of type {} already processed, skipping", id, type)
        }
        ack.acknowledge()
    }

    private fun logSkipped(record: ConsumerRecord<*, *>, ex: Exception) {
        val cause = ex.cause ?: ex
        val h = { name: String -> record.headers().lastHeader(name)?.value()?.let { String(it, Charsets.UTF_8) } }
        log.error(
            "Giving up on event type={} id={} at {}-{}@{} after {} attempts: {}: {}",
            h(EventHeaders.TYPE), h(EventHeaders.ID), record.topic(), record.partition(), record.offset(), maxAttempts,
            cause::class.java.name, cause.message,
        )
    }
}
