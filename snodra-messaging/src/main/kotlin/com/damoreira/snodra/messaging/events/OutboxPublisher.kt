package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.eventstore.EventHeaders
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.EventStoreException
import com.damoreira.snodra.messaging.eventstore.OutgoingEvent
import com.damoreira.snodra.messaging.metrics.MessagingMetrics
import com.damoreira.snodra.messaging.metrics.NoopMessagingMetrics
import com.damoreira.snodra.messaging.store.OutboxEntry
import com.damoreira.snodra.messaging.store.OutboxStore
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Duration
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Relays the [outbox] entries to the event store ([events]) in creation order, on a single daemon thread: each entry
 * becomes an [OutgoingEvent] of the service's events [source], with the entry's headers and then `type`, `id`, `timestamp`
 * (its creation) and `source` ([service]).
 *
 * Every poll [claims][OutboxStore.claim] a batch of up to [batchSize] entries for this relay under a [lease] and sends them one
 * after the other: several instances relay one outbox at once, never the same entry while its lease holds, and the entries a
 * crashed instance held are relayed by the others once its lease passed. A batch stops before its lease could run out under
 * a send ([SEND_TIMEOUT] before its end); the next poll claims again, which renews the lease of what this relay holds.
 *
 * A retriable send failure ([EventStoreException.Failed], or no answer within [SEND_TIMEOUT]) counts an attempt
 * ([OutboxStore.markAttemptFailed]) and ends the batch (so per-key order holds): the entry stays this relay's and the next poll
 * retries it first. A failure retrying cannot fix ([EventStoreException.Rejected], [EventStoreException.Invalid]) or the
 * [maxAttempts]-th failed attempt gives the entry up ([OutboxStore.markFailed]), logs it once at error (never the payload)
 * and moves on to the next entry: one poison entry does not block the outbox. Failed entries stay in the store for inspection.
 *
 * Between polls it waits [pollInterval], or less when the outbox tells it entries were appended ([OutboxStore.onAppend]).
 *
 * Reports to [metrics] every send attempt. When the metrics want it ([MessagingMetrics.countsOutbox]) the relay thread
 * also samples the outbox ([samplePending]: the pending entries and the oldest of them, whichever instance holds it) at most
 * every [pendingSampleInterval], so the gauges never make the export thread touch the database.
 */
class OutboxPublisher(
    private val outbox: OutboxStore,
    private val events: EventStore,
    /** The service's events source (`snodra.messaging.events.source`). */
    private val source: String,
    /** The service's name: every event's `source` header. */
    private val service: String,
    private val pollInterval: Duration,
    private val batchSize: Int,
    private val maxAttempts: Int = 10,
    private val metrics: MessagingMetrics = NoopMessagingMetrics,
    private val pendingSampleInterval: Duration = Duration.ofSeconds(5),
    /** How long a claimed batch stays this relay's (`snodra.messaging.outbox.lease`); longer than one send can take ([SEND_TIMEOUT]). */
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

    /** Set by an append the outbox told of ([OutboxStore.onAppend]) while a poll ran: the next wait is skipped. */
    private var woken = false

    /** The registration with the outbox's appends while running ([start] to [stop]). */
    private var listening: AutoCloseable? = null

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
                runBlocking { withTimeout(SEND_TIMEOUT.toMillis()) { events.publish(toEvent(entry)) } }
            } catch (e: Exception) {
                metrics.outboxPublished(ok = false)
                if (e is InterruptedException) Thread.currentThread().interrupt()
                val attempt = entry.attempts + 1
                val hopeless = e is EventStoreException.Rejected || e is EventStoreException.Invalid
                if (e !is InterruptedException && (hopeless || attempt >= maxAttempts)) {
                    log.error(
                        "Outbox entry {} of type {} marked failed after {} attempt(s){}: {}",
                        entry.id, entry.type, attempt, if (hopeless) " (not retriable)" else "", describe(e),
                    )
                    runBlocking { outbox.markFailed(entry.id, describe(e)) }
                    continue
                }
                log.warn("Outbox send failed for {} ({}), attempt {}: {}", entry.id, entry.type, attempt, describe(e))
                runBlocking { outbox.markAttemptFailed(entry.id, describe(e)) }
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

    /** What a failed send says: the event store's message, or the timeout. */
    private fun describe(e: Exception): String = when (e) {
        is TimeoutCancellationException -> "no answer from the event store within $SEND_TIMEOUT"
        is EventStoreException -> e.message ?: e::class.java.simpleName
        else -> "${e::class.java.name}: ${e.message}"
    }

    private fun toEvent(entry: OutboxEntry): OutgoingEvent {
        val headers = LinkedHashMap(entry.headers)
        headers[EventHeaders.TYPE] = entry.type
        headers[EventHeaders.ID] = entry.id
        headers[EventHeaders.TIMESTAMP] = EventHeaders.timestamp(entry.createdAt)
        headers[EventHeaders.SOURCE] = service
        return OutgoingEvent(source, entry.id, entry.type, entry.key, headers, entry.payload)
    }

    override fun start() {
        lock.withLock {
            if (running) return
            running = true
            stopRequested = false
            listening = outbox.onAppend {
                lock.withLock {
                    woken = true
                    wake.signalAll()
                }
            }
            thread = Thread(::loop, "snodra-outbox-publisher").apply { isDaemon = true; start() }
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
                if (running && !woken) wake.awaitNanos(pollInterval.toNanos())
                woken = false
            }
        }
    }

    override fun stop() {
        val t = lock.withLock {
            running = false
            stopRequested = true
            listening?.let { runCatching { it.close() } }
            listening = null
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
        /** The longest one publish may take: a publish without an answer by then counts as a retriable failure. */
        val SEND_TIMEOUT: Duration = Duration.ofSeconds(45)
        private const val STOP_JOIN_MS = 15_000L
    }
}
