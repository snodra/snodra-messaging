package io.damoreira.kraken.messaging.deadline

import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.NoopSagaMetrics
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.store.DeadlineStore
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Settings of the [DeadlineScheduler] (`kraken.messaging.deadlines.*`, S1 plan R8): how often it polls and how long a
 * claimed deadline stays reserved for the instance that claimed it, at most [batchSize] deadlines per poll.
 */
data class DeadlineSettings(
    val pollInterval: Duration = Duration.ofSeconds(1),
    val lease: Duration = Duration.ofSeconds(30),
    val batchSize: Int = 100,
) {
    init {
        require(!pollInterval.isZero && !pollInterval.isNegative) { "pollInterval must be positive, was $pollInterval" }
        require(!lease.isZero && !lease.isNegative) { "lease must be positive, was $lease" }
        require(batchSize >= 1) { "batchSize must be >= 1, was $batchSize" }
    }
}

/**
 * Fires the due deadlines of the sagas of this service, at least once.
 *
 * A poll claims due deadlines one at a time ([DeadlineStore.claimDue]: status SCHEDULED, `dueAt <= now`, not claimed
 * or the claim expired; it sets `claimedBy` and `claimedUntil = now + lease`) and hands each to [SagaRuntime.applyDeadline],
 * which fires it in a transaction (loads the saga, runs the `@DeadlineHandler`, writes state, marks it FIRED; a deadline
 * of an ended or missing saga is CANCELLED instead). Several service instances can run a scheduler: the lease makes each
 * deadline fire on one of them. When firing fails (or the instance dies) the claim is left alone, so the deadline fires
 * again once the lease expired: at least once. Time comes from [clock].
 *
 * [pollOnce] does one pass; [start] runs it every [DeadlineSettings.pollInterval] on a daemon thread. Runs after the outbox
 * publisher starts and stops before the event consumer, like the dispatch relay.
 */
class DeadlineScheduler(
    private val deadlines: DeadlineStore,
    private val runtime: SagaRuntime,
    private val clock: Clock = Clock.systemUTC(),
    private val settings: DeadlineSettings = DeadlineSettings(),
    private val metrics: SagaMetrics = NoopSagaMetrics,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(DeadlineScheduler::class.java)
    private val owner = UUID.randomUUID().toString()
    private val lock = ReentrantLock()
    private val wake = lock.newCondition()

    @Volatile private var running = false
    @Volatile private var stopRequested = false
    private var thread: Thread? = null

    /** One pass: claims and fires the due deadlines; returns how many it claimed. Reports what is still overdue to the metrics. */
    fun pollOnce(): Int {
        var claimed = 0
        while (claimed < settings.batchSize && !stopRequested) {
            val deadline = claimNext() ?: break
            claimed++
            try {
                runtime.applyDeadline(deadline)
            } catch (e: Exception) {
                // The claim stays: the lease expires and the deadline fires again (at least once).
                log.warn("Deadline {} '{}' of saga {} failed, it fires again when its lease expires: {}", deadline.id, deadline.name, deadline.sagaId, e.toString())
                log.debug("Deadline failure", e)
            }
        }
        metrics.deadlinesOverdue(deadlines.countOverdueByName(clock.instant()))
        return claimed
    }

    private fun claimNext(): DeadlineDocument? {
        val now = clock.instant()
        return deadlines.claimDue(owner, now, now.plus(settings.lease))
    }

    override fun start() {
        lock.withLock {
            if (running) return
            running = true
            stopRequested = false
            thread = Thread(::loop, "kraken-deadline-scheduler").apply { isDaemon = true; start() }
        }
    }

    private fun loop() {
        while (running) {
            try {
                pollOnce()
            } catch (e: Exception) {
                log.warn("Deadline poll failed", e)
            }
            lock.withLock {
                if (running) wake.awaitNanos(settings.pollInterval.toNanos())
            }
        }
    }

    override fun stop() {
        val t = lock.withLock {
            running = false
            stopRequested = true
            wake.signalAll()
            thread.also { thread = null }
        }
        t?.join(STOP_JOIN_MS)
    }

    override fun isRunning(): Boolean = running

    /** Starts after the outbox publisher and stops before the event consumer (which feeds the sagas whose deadlines it fires). */
    override fun getPhase(): Int = PHASE

    companion object {
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 1850
        private const val STOP_JOIN_MS = 15_000L
    }
}
