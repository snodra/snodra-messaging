package io.damoreira.kraken.messaging.kafka

import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Relays unsent `outbox` documents to Kafka in creation order, on a single daemon thread.
 *
 * A retriable send failure increments the document's `attempts` and ends the batch (so per-key order holds); the next
 * poll retries it. A non-retriable failure ([NON_RETRIABLE] in the cause chain) or the [maxAttempts]-th failed attempt
 * marks the document failed (`failedAt`), logs it once at error (never the payload) and moves on to the next
 * document: one poison document does not block the outbox. Failed documents stay in the collection for inspection;
 * clearing `failedAt` (and `attempts`) re-queues one.
 */
class OutboxPublisher(
    private val mongo: MongoTemplate,
    private val kafka: KafkaTemplate<String, ByteArray>,
    private val topic: String,
    private val source: String,
    private val pollInterval: Duration,
    private val batchSize: Int,
    private val maxAttempts: Int = 10,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(OutboxPublisher::class.java)
    private val lock = ReentrantLock()
    private val wake = lock.newCondition()

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
    }

    @Volatile private var running = false
    @Volatile private var stopRequested = false
    private var thread: Thread? = null

    /**
     * Sends one batch of pending documents sequentially; returns how many were sent. Stops at the first retriable
     * failure and between documents once [stop] was requested.
     */
    fun publishPendingNow(): Int {
        val pending = mongo.find(
            Query(Criteria.where("sentAt").isNull.and("failedAt").isNull)
                .with(Sort.by(Sort.Direction.ASC, "createdAt", "seq")).limit(batchSize),
            OutboxDocument::class.java,
        )
        var sent = 0
        for (doc in pending) {
            if (stopRequested) return sent
            try {
                // Bounded by the producer's max.block.ms + delivery.timeout.ms (10 s + 30 s in the auto-configuration).
                kafka.send(toRecord(doc)).get(SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS)
            } catch (e: Exception) {
                if (e is InterruptedException) Thread.currentThread().interrupt()
                val attempt = doc.attempts + 1
                val nonRetriable = nonRetriableCause(e)
                if (e !is InterruptedException && (nonRetriable != null || attempt >= maxAttempts)) {
                    val cause = nonRetriable ?: rootCause(e)
                    log.error(
                        "Outbox document {} of type {} marked failed after {} attempt(s){}: {}: {}",
                        doc.id, doc.type, attempt, if (nonRetriable != null) " (not retriable)" else "",
                        cause::class.java.name, cause.message,
                    )
                    mongo.updateFirst(
                        byId(doc.id), Update().inc("attempts", 1).set("failedAt", Instant.now()), OutboxDocument::class.java,
                    )
                    continue
                }
                log.warn("Outbox send failed for {} ({}), attempt {}", doc.id, doc.type, attempt, e)
                mongo.updateFirst(byId(doc.id), Update().inc("attempts", 1), OutboxDocument::class.java)
                return sent
            }
            mongo.updateFirst(byId(doc.id), Update().set("sentAt", Instant.now()), OutboxDocument::class.java)
            sent++
        }
        return sent
    }

    private fun causes(e: Throwable) = generateSequence(e) { it.cause.takeIf { c -> c !== it } }.take(16)

    private fun nonRetriableCause(e: Throwable): Throwable? = causes(e).firstOrNull { c -> NON_RETRIABLE.any { it.isInstance(c) } }

    private fun rootCause(e: Throwable): Throwable = causes(e).last()

    private fun byId(id: String) = Query(Criteria.where("_id").`is`(id))

    private fun toRecord(doc: OutboxDocument): ProducerRecord<String, ByteArray> {
        val record = ProducerRecord<String, ByteArray>(
            topic, null, doc.key, doc.payload.toJson().toByteArray(Charsets.UTF_8),
        )
        val all = LinkedHashMap(doc.headers)
        all[EventHeaders.TYPE] = doc.type
        all[EventHeaders.ID] = doc.id
        all[EventHeaders.TIMESTAMP] = doc.createdAt.toString()
        all[EventHeaders.SOURCE] = source
        all.forEach { (k, v) -> record.headers().add(RecordHeader(k, v.toByteArray(Charsets.UTF_8))) }
        return record
    }

    override fun start() {
        lock.withLock {
            if (running) return
            running = true
            stopRequested = false
            thread = Thread(::loop, "kraken-outbox-publisher").apply { isDaemon = true; start() }
        }
    }

    private fun loop() {
        while (running) {
            try {
                publishPendingNow()
            } catch (e: Exception) {
                log.warn("Outbox poll failed", e)
            }
            lock.withLock {
                if (running) wake.awaitNanos(pollInterval.toNanos())
            }
        }
    }

    override fun stop() {
        val t = lock.withLock {
            running = false
            stopRequested = true
            wake.signalAll()
            thread.also { thread = null }
        } ?: return
        // The loop stops between documents, so at most one send is in flight; interrupt it if it takes longer.
        t.join(STOP_JOIN_MS)
        if (t.isAlive) t.interrupt()
    }

    override fun isRunning(): Boolean = running

    /** Stops after the web server's graceful drain (`DEFAULT_PHASE - 1024`) and after [EventConsumer]. */
    override fun getPhase(): Int = PHASE

    companion object {
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 1950
        private const val SEND_TIMEOUT_SECONDS = 45L
        private const val STOP_JOIN_MS = 15_000L

        /** Send failures that retrying the same record cannot fix. */
        val NON_RETRIABLE: List<Class<out Throwable>> = listOf(
            RecordTooLargeException::class.java,
            SerializationException::class.java,
            InvalidTopicException::class.java,
            AuthorizationException::class.java,
        )
    }
}
