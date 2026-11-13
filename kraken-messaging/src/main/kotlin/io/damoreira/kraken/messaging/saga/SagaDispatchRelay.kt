package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.store.SagaDispatchStore
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.transaction.support.TransactionOperations
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.Callable
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ExecutionException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Future
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Settings of the [SagaDispatchRelay] (`kraken.messaging.sagas.relay.*`, S1 plan R8): how often it polls, how long a saga's
 * dispatch lease holds, and the transient retry of a command (up to [maxAttempts] attempts in all, then `EXHAUSTED`, with
 * [initialBackoff] growing by [multiplier] up to [maxBackoff]). [parallelism] sagas are dispatched at a time, at most
 * [batchSize] of them per poll. A command send gets [sendTimeout] for its answer (H1): one that takes longer is abandoned
 * and counts as a transient failure. Through the hub the bound is never shorter than the hub's own deadline for the call plus
 * a guard ([RawCommandSender.sendTimeoutFor]).
 */
data class RelaySettings(
    val pollInterval: Duration = Duration.ofMillis(500),
    val lease: Duration = Duration.ofSeconds(30),
    val maxAttempts: Int = 10,
    val initialBackoff: Duration = Duration.ofSeconds(1),
    val multiplier: Double = 2.0,
    val maxBackoff: Duration = Duration.ofMinutes(5),
    val parallelism: Int = 4,
    val batchSize: Int = 100,
    val sendTimeout: Duration = Duration.ofMinutes(1),
) {
    init {
        require(!pollInterval.isZero && !pollInterval.isNegative) { "pollInterval must be positive, was $pollInterval" }
        require(!lease.isZero && !lease.isNegative) { "lease must be positive, was $lease" }
        require(parallelism >= 1 && batchSize >= 1) { "parallelism and batchSize must be >= 1" }
        require(!sendTimeout.isZero && !sendTimeout.isNegative) { "sendTimeout must be positive, was $sendTimeout" }
    }

    /** The backoff between the attempts of one command (validates [maxAttempts], [multiplier] and the backoffs). */
    val retryPolicy: RetryPolicy = RetryPolicy(maxAttempts, initialBackoff, multiplier, maxBackoff)
}

/**
 * Sends what sagas recorded in `saga_dispatch` ([store]) after their transactions committed, in order per saga.
 *
 * A poll finds the sagas whose first pending entry (lowest `seq` not dispatched and not failed) is due, claims each one
 * with a lease ([SagaDispatchStore.claimLease], so two service instances never dispatch one saga's entries at once) and works
 * through its entries in `seq` order; different sagas go in parallel. An ended saga's entries are dispatched like any.
 *
 * - An EVENT entry is inserted into the outbox and marked dispatched **in one transaction**: exactly once. One the outbox
 *   refuses (its payload is not a JSON object the store can hold; the store's `record` refuses it first) is given up as
 *   INVALID, logged at error and counted, and never retried.
 * - A COMMAND entry is sent as raw bytes by wire type name ([RawCommandSender]) with `retry = RetryPolicy.NONE` (the bus's
 *   own retry, when configured, must never stack under the relay's) and the headers `idempotency-key` =
 *   `<sagaId>:<seq>` (the entry's id) and `saga-id`, the entry's routing key and the correlation id of the message that
 *   caused it, outside any caller identity. It is marked dispatched after the reply, so a crash in between re-sends it
 *   with the same key (an `@IdempotentHandler` adapter then runs its body once).
 * - A send that has no answer within [RelaySettings.sendTimeout] (a local adapter that hangs: a local call has no timeout of
 *   its own) is abandoned: its thread is interrupted (a blocking call that honours interrupts ends; one that does not keeps
 *   that thread until it returns) and it is a transient failure, retried later with the same key.
 * - A transient failure counts an attempt and delays the entry (`nextAttemptAt`), holding back that saga's later entries
 *   only; after [RelaySettings.maxAttempts] it becomes `EXHAUSTED`. A permanent failure is a known outcome: it is
 *   **persisted first** (`failedAt`, `failureReason`, `failureMessage` and `noticePending`), so the command is never sent
 *   again, and then the [SagaCommandFailed] is dispatched locally (dedupe `failed:<entry id>`). A notice that cannot be
 *   delivered (an ordinary handler throws, the store is down) is retried by the poll with its own bounded backoff
 *   (`noticeAttempts`, `noticeNextAt`; [RelaySettings.maxAttempts] attempts, then logged at error and counted). Later
 *   entries of the saga continue after a failed one: the saga decides what the failure means. A [SagaCommandFailed] that
 *   no live saga with a handler and no ordinary handler took is logged at error and counted.
 * - An exception outside the send (the store is down, a mark fails) leaves the entry untouched: it is due again at the
 *   next poll.
 *
 * [pollOnce] does one pass (what the tests and the polling thread call); [start] runs it every
 * [RelaySettings.pollInterval] on a daemon thread. Runs after the outbox publisher starts and stops before the event consumer.
 */
class SagaDispatchRelay(
    private val store: SagaDispatchStore,
    private val tx: TransactionOperations,
    private val commands: RawCommandSender,
    private val events: RawEventPublisher,
    private val local: LocalEventDispatcher,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val clock: Clock = Clock.systemUTC(),
    private val settings: RelaySettings = RelaySettings(),
    private val metrics: SagaMetrics = NoopSagaMetrics,
) : SmartLifecycle {
    private val log = LoggerFactory.getLogger(SagaDispatchRelay::class.java)
    private val owner = UUID.randomUUID().toString()
    private val lock = ReentrantLock()
    private val wake = lock.newCondition()

    @Volatile private var running = false
    @Volatile private var stopRequested = false
    private var thread: Thread? = null
    private var pool: ExecutorService? = null
    private var renewer: ScheduledExecutorService? = null
    private var sends: ExecutorService? = null
    private val inFlight: MutableSet<String> = ConcurrentHashMap.newKeySet()

    /**
     * One synchronous pass: hands the due failure notices and sagas to the pool ([submitDue]) and waits for exactly those
     * to finish; returns how many entries and notices they dealt with. What the tests and a caller that wants to know
     * call; the polling thread uses [submitDue] and never waits.
     */
    fun pollOnce(): Int = submitDue().sumOf { it.get() }

    /**
     * Hands every claimable due saga and every due failure notice to the worker pool and returns at once, **never waiting
     * for the sends** and skipping what is already in flight on this instance: one saga whose adapter call hangs (a local
     * adapter has no timeout of its own) holds back only itself. While a saga's send is alive its lease is renewed (see
     * [sendCommand]), so no other instance takes the saga over and sends its entry again.
     */
    internal fun submitDue(): List<Future<Int>> {
        val futures = ArrayList<Future<Int>?>()
        for (notice in claimDueNotices()) futures += submit("notice:${notice.id}") { deliverNoticeQuietly(notice) }
        for (sagaId in dueSagas()) futures += submit("saga:$sagaId") { dispatchSaga(sagaId) }
        return futures.filterNotNull()
    }

    /** Runs [work] on the pool unless [key] is already in flight (then null); the key is released when it finishes. */
    private fun submit(key: String, work: () -> Int): Future<Int>? {
        if (!inFlight.add(key)) return null
        return try {
            executor().submit(Callable { try { work() } finally { inFlight.remove(key) } })
        } catch (e: RuntimeException) {
            inFlight.remove(key) // the pool is shut down
            null
        }
    }

    /** The sagas whose first pending entry is due, oldest work first, without those in flight here. */
    private fun dueSagas(): List<String> =
        store.dueSagas(clock.instant(), inFlight.filter { it.startsWith("saga:") }.map { it.removePrefix("saga:") }, settings.batchSize)

    /** Works through one saga's entries while this relay holds its lease; the number of entries dealt with. */
    private fun dispatchSaga(sagaId: String): Int {
        if (!claim(sagaId)) return 0
        var dealt = 0
        try {
            while (!stopRequested) {
                val entry = store.nextPending(sagaId) ?: break
                if (entry.nextAttemptAt != null && entry.nextAttemptAt.isAfter(clock.instant())) break
                if (!renew(sagaId)) {
                    log.warn("Lost the dispatch lease of saga {} while dispatching", sagaId)
                    break
                }
                dealt++
                if (!dispatch(entry)) break
            }
        } catch (e: Exception) {
            log.warn("Dispatching saga {} failed, trying again at the next poll: {}", sagaId, e.toString())
            log.debug("Dispatch failure of saga {}", sagaId, e)
        } finally {
            release(sagaId)
        }
        return dealt
    }

    /** Dispatches [entry]; false when this saga's turn must end (the entry waits). */
    private fun dispatch(entry: SagaDispatchDocument): Boolean = when (entry.kind) {
        DispatchKind.EVENT -> {
            try {
                tx.executeWithoutResult {
                    events.publishRaw(entry.type, entry.routingKey, entry.payload.toByteArray(Charsets.UTF_8), entry.headers[DISPATCH_CORRELATION_HEADER] ?: entry.id)
                    markDispatched(entry)
                }
            } catch (e: IllegalArgumentException) {
                invalidEvent(entry, e) // the outbox refuses it: retrying cannot change the payload
            }
            true
        }
        DispatchKind.COMMAND -> sendCommand(entry)
    }

    /**
     * The backstop for an EVENT entry the outbox refuses (the store's [SagaDispatchStore.record] refuses such an entry first):
     * given up as INVALID, logged at error and counted, so it never holds back the saga's later entries. No failure notice:
     * `SagaCommandFailed` is about commands.
     */
    private fun invalidEvent(entry: SagaDispatchDocument, error: IllegalArgumentException) {
        val now = clock.instant()
        store.markFailed(entry.id, entry.attempts, SagaCommandFailed.Reason.INVALID, error.message.orEmpty(), now, now)
        store.clearNotice(entry.id)
        metrics.commandFailureUnhandled(entry.type, SagaCommandFailed.Reason.INVALID)
        log.error("Saga {} event {} ({}) cannot be published, given up as INVALID: {}", entry.sagaId, entry.id, entry.type, error.message)
    }

    private fun sendCommand(entry: SagaDispatchDocument): Boolean {
        val correlationId = entry.headers[DISPATCH_CORRELATION_HEADER] ?: entry.id
        val headers = entry.headers.filterKeys { it != DISPATCH_CORRELATION_HEADER } + mapOf("idempotency-key" to entry.id, "saga-id" to entry.sagaId)
        val period = maxOf(settings.lease.toMillis() / 3, MIN_RENEW_PERIOD_MS)
        val beat = leaseRenewer().scheduleAtFixedRate({ keepLease(entry.sagaId) }, period, period, TimeUnit.MILLISECONDS)
        // On a thread of its own, so a call that never returns can be abandoned after the send timeout.
        val send = sendExecutor().submit(Callable {
            runBlocking(CorrelationContext(correlationId)) {
                commands.sendRaw(entry.type, entry.payload.toByteArray(Charsets.UTF_8), SendOptions(routingKey = entry.routingKey, retry = RetryPolicy.NONE, headers = headers))
            }
        })
        // A local adapter has no other bound; through the hub the bound waits for the hub to give up first (ADR 0021 ruling 74).
        val timeout = commands.sendTimeoutFor(entry.type, settings.sendTimeout)
        val outcome = try {
            send.get(timeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            send.cancel(true)
            RawOutcome.Transient("no answer within ${timeout.toMillis()} ms: the call was abandoned and its thread interrupted")
        } catch (e: ExecutionException) {
            RawFailures.classify(e.cause ?: e)
        } catch (e: Exception) {
            send.cancel(true)
            if (e is InterruptedException) Thread.currentThread().interrupt()
            RawFailures.classify(e)
        } finally {
            beat.cancel(false)
        }
        return when (outcome) {
            RawOutcome.Success -> {
                markDispatched(entry)
                true
            }
            is RawOutcome.Transient -> retryLater(entry, outcome.message)
            is RawOutcome.Permanent -> {
                fail(entry, outcome.reason, outcome.message)
                true
            }
        }
    }

    /** A transient failure: another attempt later, or `EXHAUSTED` when the limit is reached. False: the entry waits. */
    private fun retryLater(entry: SagaDispatchDocument, error: String): Boolean {
        val attempts = entry.attempts + 1
        if (attempts >= settings.maxAttempts) {
            fail(entry, SagaCommandFailed.Reason.EXHAUSTED, "gave up after $attempts attempts, the last failure was: $error", attempts)
            return true
        }
        val next = clock.instant().plus(settings.retryPolicy.jitteredBackoffBefore(attempts + 1))
        store.scheduleRetry(entry.id, attempts, next)
        log.warn("Command {} ({}) failed attempt {} of {}, next at {}: {}", entry.id, entry.type, attempts, settings.maxAttempts, next, error)
        return false
    }

    /**
     * Gives the entry up with a known outcome. The outcome is persisted first (`failedAt`, the reason and message, and a
     * pending failure notice reserved for this instance), so nothing that goes wrong afterwards can make the command be sent
     * again; the notice is then delivered, and retried on its own when that fails (see [deliverNotice]).
     */
    private fun fail(entry: SagaDispatchDocument, reason: SagaCommandFailed.Reason, message: String, attempts: Int = entry.attempts) {
        val now = clock.instant()
        store.markFailed(entry.id, attempts, reason, message, now, noticeNextAt = now.plus(settings.lease))
        log.warn("Saga {} command {} ({}) failed with {}: {}", entry.sagaId, entry.id, entry.type, reason, message)
        deliverNoticeQuietly(entry.copy(attempts = attempts, failedAt = now, failureReason = reason, failureMessage = message, noticePending = true, noticeAttempts = 0))
    }

    /** Claims the pending failure notices that are due (up to a batch, not those in flight here), each reserved for this instance by moving `noticeNextAt` a lease ahead. */
    private fun claimDueNotices(): List<SagaDispatchDocument> {
        if (stopRequested) return emptyList()
        val now = clock.instant()
        return store.claimDueNotices(now, reserveUntil = now.plus(settings.lease), limit = settings.batchSize)
    }

    /**
     * [deliverNotice], whose own failures (the delivery's are handled inside it) are the store's: the notice's pending mark
     * could not be cleared or its next attempt not recorded. It stays pending and is delivered again when its claim
     * expires (the dedupe id makes that harmless); the entry itself is settled, so the saga's later entries go on.
     */
    private fun deliverNoticeQuietly(entry: SagaDispatchDocument): Int {
        try {
            deliverNotice(entry)
        } catch (e: Exception) {
            log.warn("The failure notice of {} could not be settled in the store; it is delivered again when its claim expires: {}", entry.id, e.toString())
        }
        return 1
    }

    /**
     * Tells the saga (and any ordinary handler) that the command of [entry] failed: a [SagaCommandFailed] dispatched
     * locally under the dedupe id `failed:<entry id>`, never published. Done: the pending mark is removed. A failure of the
     * delivery (an ordinary handler that throws, the store down) counts a notice attempt and sets `noticeNextAt` with the
     * relay's backoff; after [RelaySettings.maxAttempts] the notice is given up (logged at error and counted). The
     * dispatcher's markers make every repeat harmless. The pending notice of an EVENT entry (left by a crash between the
     * backstop's `markFailed` and its `clearNotice`, see [invalidEvent]) is only cleared: `SagaCommandFailed` is about commands.
     */
    private fun deliverNotice(entry: SagaDispatchDocument) {
        if (entry.kind == DispatchKind.EVENT) return clearNotice(entry)
        val reason = entry.failureReason ?: return clearNotice(entry)
        val failed = SagaCommandFailed(entry.sagaId, entry.type, entry.id, reason, entry.failureMessage.orEmpty())
        val type = typeResolver.typeNameOf(SagaCommandFailed::class)
        val correlationId = entry.headers[DISPATCH_CORRELATION_HEADER] ?: entry.id
        val dedupeId = "failed:${entry.id}"
        val ctx = MessageContext(
            messageId = dedupeId, type = type, correlationId = correlationId, userId = null, roles = emptySet(), authorization = null,
            headers = mapOf("id" to dedupeId, "type" to type, "correlationId" to correlationId),
        )
        val outcome = try {
            local.dispatch(dedupeId, type, codec.encode(failed), ctx)
        } catch (e: Exception) {
            return noticeFailed(entry, e)
        }
        when (outcome) {
            LocalEventDispatcher.Outcome.NO_INTEREST -> {
                metrics.commandFailureUnhandled(entry.type, reason)
                log.error(
                    "Saga {} command {} ({}) failed with {} and nothing in this service handles SagaCommandFailed (no saga type with a handler for it, no ordinary handler): {}",
                    entry.sagaId, entry.id, entry.type, reason, entry.failureMessage,
                )
            }
            LocalEventDispatcher.Outcome.UNCLAIMED -> {
                metrics.commandFailureUnhandled(entry.type, reason)
                log.error(
                    "Saga {} command {} ({}) failed with {} and no live instance of a saga took it (the saga ended or no longer associates with {}), and no ordinary handler handles it: {}",
                    entry.sagaId, entry.id, entry.type, reason, entry.sagaId, entry.failureMessage,
                )
            }
            else -> Unit
        }
        clearNotice(entry)
    }

    private fun clearNotice(entry: SagaDispatchDocument) = store.clearNotice(entry.id)

    private fun noticeFailed(entry: SagaDispatchDocument, error: Exception) {
        val attempts = (entry.noticeAttempts ?: 0) + 1
        if (attempts >= settings.maxAttempts) {
            metrics.commandFailureUnhandled(entry.type, entry.failureReason!!)
            log.error(
                "Gave up delivering SagaCommandFailed for saga {} command {} ({}, {}) after {} attempts; the saga may have applied it (its stage runs even when an ordinary handler fails), but the delivery kept failing: {}",
                entry.sagaId, entry.id, entry.type, entry.failureReason, attempts, error.toString(),
            )
            clearNotice(entry)
            return
        }
        val next = clock.instant().plus(settings.retryPolicy.jitteredBackoffBefore(attempts + 1))
        store.scheduleNoticeRetry(entry.id, attempts, next)
        log.warn("Delivering SagaCommandFailed for command {} failed attempt {} of {}, next at {}: {}", entry.id, attempts, settings.maxAttempts, next, error.toString())
    }

    private fun markDispatched(entry: SagaDispatchDocument) = store.markDispatched(entry.id, clock.instant())

    // ---- the saga's dispatch lease -----------------------------------------------------------------------------

    /** Takes the lease of [sagaId] when it is free or expired. */
    private fun claim(sagaId: String): Boolean {
        val now = clock.instant()
        return store.claimLease(sagaId, owner, now.plus(settings.lease), now)
    }

    private fun renew(sagaId: String): Boolean = store.renewLease(sagaId, owner, clock.instant().plus(settings.lease))

    /** The heartbeat of a live send: extends the lease so that it never expires under an adapter call that is still running. */
    private fun keepLease(sagaId: String) {
        try {
            if (!renew(sagaId)) log.warn("Lost the dispatch lease of saga {} while a send was running", sagaId)
        } catch (e: Exception) {
            log.debug("Could not renew the dispatch lease of saga {}: {}", sagaId, e.toString())
        }
    }

    private fun release(sagaId: String) {
        try {
            store.releaseLease(sagaId, owner)
        } catch (e: Exception) {
            log.debug("Could not release the dispatch lease of saga {} (it expires): {}", sagaId, e.toString())
        }
    }

    // ---- lifecycle ---------------------------------------------------------------------------------------------

    private fun executor(): ExecutorService = synchronized(this) {
        pool?.takeUnless { it.isShutdown } ?: Executors.newFixedThreadPool(settings.parallelism) { r ->
            Thread(r, "kraken-saga-dispatch").apply { isDaemon = true }
        }.also { pool = it }
    }

    /** The threads the command sends run on: as many as sends are alive, an abandoned hung call keeping its own. */
    private fun sendExecutor(): ExecutorService = synchronized(this) {
        sends?.takeUnless { it.isShutdown } ?: Executors.newCachedThreadPool { r ->
            Thread(r, "kraken-saga-send").apply { isDaemon = true }
        }.also { sends = it }
    }

    private fun leaseRenewer(): ScheduledExecutorService = synchronized(this) {
        renewer?.takeUnless { it.isShutdown } ?: Executors.newSingleThreadScheduledExecutor { r ->
            Thread(r, "kraken-saga-lease-renewer").apply { isDaemon = true }
        }.also { renewer = it }
    }

    override fun start() {
        lock.withLock {
            if (running) return
            running = true
            stopRequested = false
            thread = Thread(::loop, "kraken-saga-dispatch-relay").apply { isDaemon = true; start() }
        }
    }

    private fun loop() {
        while (running) {
            try {
                submitDue()
            } catch (e: Exception) {
                log.warn("Saga dispatch poll failed", e)
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
        synchronized(this) {
            pool?.shutdown()
            if (pool?.awaitTermination(STOP_JOIN_MS, TimeUnit.MILLISECONDS) == false) {
                log.warn("Saga dispatch sends are still running {} s after stop; abandoning them (their leases expire)", STOP_JOIN_MS / 1000)
                pool?.shutdownNow()
            }
            pool = null
            sends?.shutdownNow()
            sends = null
            renewer?.shutdownNow()
            renewer = null
        }
    }

    override fun isRunning(): Boolean = running

    /** Starts after the outbox publisher and stops before the event consumer (which feeds the sagas whose failures it delivers). */
    override fun getPhase(): Int = PHASE

    companion object {
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 1850
        private const val STOP_JOIN_MS = 15_000L
        private const val MIN_RENEW_PERIOD_MS = 10L
    }
}
