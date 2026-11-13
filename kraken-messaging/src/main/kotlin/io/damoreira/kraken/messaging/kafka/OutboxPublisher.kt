package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.eventstore.EventHeaders
import io.damoreira.kraken.messaging.metrics.MessagingMetrics
import io.damoreira.kraken.messaging.metrics.NoopMessagingMetrics
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.OutboxStore
import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.errors.AuthorizationException
import org.apache.kafka.common.errors.InvalidTopicException
import org.apache.kafka.common.errors.RecordTooLargeException
import org.apache.kafka.common.errors.SerializationException
import org.apache.kafka.common.header.internals.RecordHeader
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration
import java.util.UUID
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Relays the [outbox] entries to Kafka in creation order, on a single daemon thread.
 *
 * Every poll [claims][OutboxStore.claim] a batch of up to [batchSize] entries for this relay under a [lease] and sends them one
 * after the other: several instances relay one outbox at once, never the same entry while its lease holds, and the entries a
 * crashed instance held are relayed by the others once its lease passed. A batch stops before its lease could run out under
 * a send ([SEND_TIMEOUT] before its end); the next poll claims again, which renews the lease of what this relay holds.
 *
 * A retriable send failure counts an attempt ([OutboxStore.markAttemptFailed]) and ends the batch (so per-key order holds): the
 * entry stays this relay's and the next poll retries it first. A non-retriable failure ([NON_RETRIABLE] in the cause chain) or
 * the [maxAttempts]-th failed attempt gives the entry up ([OutboxStore.markFailed]), logs it once at error (never the payload)
 * and moves on to the next entry: one poison entry does not block the outbox. Failed entries stay in the store for inspection.
 *
 * Reports to [metrics] (P9) every send attempt. When the metrics want it ([MessagingMetrics.countsOutbox]) the relay thread
 * also samples the outbox ([samplePending]: the pending entries and the oldest of them, whichever instance holds it) at most
 * every [pendingSampleInterval], so the gauges never make the export thread touch the database.
 */
class OutboxPublisher(
    private val outbox: OutboxStore,
    private val kafka: KafkaTemplate<String, ByteArray>,
    private val topic: String,
    private val source: String,
    private val pollInterval: Duration,
    private val batchSize: Int,
    private val maxAttempts: Int = 10,
    private val metrics: MessagingMetrics = NoopMessagingMetrics,
    private val pendingSampleInterval: Duration = Duration.ofSeconds(5),
    /** How long a claimed batch stays this relay's (`kraken.messaging.outbox.lease`); longer than one send can take ([SEND_TIMEOUT]). */
    private val lease: Duration = Duration.ofMinutes(2),
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(OutboxPublisher::class.java)
    private val owner = UUID.randomUUID().toString()
    private val lock = ReentrantLock()
    private val wake = lock.newCondition()

    init {
        require(maxAttempts >= 1) { "maxAttempts must be >= 1" }
        require(lease > SEND_TIMEOUT) { "lease must be longer than a send ($SEND_TIMEOUT), was $lease" }
    }

    @Volatile private var running = false
    @Volatile private var stopRequested = false
    private var thread: Thread? = null

    /**
     * Claims one batch and sends it sequentially; returns how many were sent. Stops at the first retriable failure, between
     * entries once [stop] was requested, and before the batch's lease could run out under the next send.
     */
    fun publishPendingNow(): Int {
        val claimedAt = System.nanoTime()
        val batch = runBlocking { outbox.claim(owner, lease, batchSize) }
        val sendUntil = claimedAt + lease.minus(SEND_TIMEOUT).toNanos()
        var sent = 0
        for (entry in batch) {
            if (stopRequested || System.nanoTime() - sendUntil >= 0) return sent
            try {
                // Bounded by the producer's max.block.ms + delivery.timeout.ms (10 s + 30 s in the auto-configuration).
                kafka.send(toRecord(entry)).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)
            } catch (e: Exception) {
                metrics.outboxPublished(ok = false)
                if (e is InterruptedException) Thread.currentThread().interrupt()
                val attempt = entry.attempts + 1
                val nonRetriable = nonRetriableCause(e)
                if (e !is InterruptedException && (nonRetriable != null || attempt >= maxAttempts)) {
                    val cause = nonRetriable ?: rootCause(e)
                    log.error(
                        "Outbox entry {} of type {} marked failed after {} attempt(s){}: {}: {}",
                        entry.id, entry.type, attempt, if (nonRetriable != null) " (not retriable)" else "",
                        cause::class.java.name, cause.message,
                    )
                    runBlocking { outbox.markFailed(entry.id, "${cause::class.java.name}: ${cause.message}") }
                    continue
                }
                log.warn("Outbox send failed for {} ({}), attempt {}", entry.id, entry.type, attempt, e)
                runBlocking { outbox.markAttemptFailed(entry.id, e.toString()) }
                return sent
            }
            metrics.outboxPublished(ok = true)
            runBlocking { outbox.markSent(listOf(entry.id)) }
            sent++
        }
        return sent
    }

    /** Samples the entries not sent and not given up on: how many ([MessagingMetrics.outboxPending]) and the oldest ([MessagingMetrics.outboxOldest]). */
    fun samplePending() {
        metrics.outboxPending(runBlocking { outbox.pending() })
        metrics.outboxOldest(runBlocking { outbox.oldestPending() })
    }

    private fun causes(e: Throwable) = generateSequence(e) { it.cause.takeIf { c -> c !== it } }.take(16)

    private fun nonRetriableCause(e: Throwable): Throwable? = causes(e).firstOrNull { c -> NON_RETRIABLE.any { it.isInstance(c) } }

    private fun rootCause(e: Throwable): Throwable = causes(e).last()

    private fun toRecord(entry: OutboxEntry): ProducerRecord<String, ByteArray> {
        val record = ProducerRecord<String, ByteArray>(topic, null, entry.key, entry.payload)
        val all = LinkedHashMap(entry.headers)
        all[EventHeaders.TYPE] = entry.type
        all[EventHeaders.ID] = entry.id
        all[EventHeaders.TIMESTAMP] = entry.createdAt.toString()
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
        var nextSample = System.nanoTime()
        while (running) {
            try {
                publishPendingNow()
            } catch (e: Exception) {
                log.warn("Outbox poll failed", e)
            }
            if (metrics.countsOutbox && System.nanoTime() - nextSample >= 0) {
                nextSample = System.nanoTime() + pendingSampleInterval.toNanos()
                try {
                    samplePending()
                } catch (e: Exception) {
                    log.debug("Counting the pending outbox entries failed, keeping the last value: {}", e.toString())
                }
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
        /** The longest one send to Kafka may take (the producer's max.block.ms + delivery.timeout.ms, with a margin). */
        val SEND_TIMEOUT: Duration = Duration.ofSeconds(45)
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
