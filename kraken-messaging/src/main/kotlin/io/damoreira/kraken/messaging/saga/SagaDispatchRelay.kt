package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import org.springframework.context.SmartLifecycle
import org.springframework.dao.DuplicateKeyException
import org.springframework.data.domain.Sort
import org.springframework.data.mongodb.core.FindAndModifyOptions
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.aggregation.Aggregation
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import org.springframework.data.mongodb.core.query.Update
import org.springframework.transaction.support.TransactionOperations
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.util.Date
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
 * and counts as a transient failure.
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
 * Sends what sagas recorded in `saga_dispatch` after their transactions committed, in order per saga.
 *
 * A poll finds the sagas whose first pending entry (lowest `seq` not dispatched and not failed) is due, claims each one
 * with a lease (`saga_dispatch_leases`, so two service instances never dispatch one saga's entries at once) and works
 * through its entries in `seq` order; different sagas go in parallel. An ended saga's entries are dispatched like any.
 *
 * - An EVENT entry is inserted into the outbox and marked dispatched **in one transaction**: exactly once.
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
    private val mongo: MongoTemplate,
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

    /** The sagas whose first pending entry is due, oldest work first. */
    private fun dueSagas(): List<String> {
        val now = Date.from(clock.instant())
        val aggregation = Aggregation.newAggregation(
            Aggregation.match(Criteria.where("dispatchedAt").isNull.and("failedAt").isNull.and("sagaId").nin(inFlight.filter { it.startsWith("saga:") }.map { it.removePrefix("saga:") })),
            Aggregation.sort(Sort.by("sagaId", "seq")),
            Aggregation.group("sagaId").first("nextAttemptAt").`as`("next").first("createdAt").`as`("since"),
            Aggregation.match(Criteria().orOperator(Criteria.where("next").isNull, Criteria.where("next").lte(now))),
            Aggregation.sort(Sort.by("since", "_id")), // oldest work first: the first pending entry of each saga, by when it was recorded
            Aggregation.limit(settings.batchSize.toLong()),
        ).withOptions(Aggregation.newAggregationOptions().allowDiskUse(true).build())
        return mongo.aggregate(aggregation, "saga_dispatch", org.bson.Document::class.java).mappedResults.map { it.getString("_id") }
    }

    /** Works through one saga's entries while this relay holds its lease; the number of entries dealt with. */
    private fun dispatchSaga(sagaId: String): Int {
        if (!claim(sagaId)) return 0
        var dealt = 0
        try {
            while (!stopRequested) {
                val entry = firstPending(sagaId) ?: break
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
            tx.executeWithoutResult {
                events.publishRaw(entry.type, entry.routingKey, org.bson.Document.parse(entry.payload), entry.headers[DISPATCH_CORRELATION_HEADER] ?: entry.id)
                markDispatched(entry)
            }
            true
        }
        DispatchKind.COMMAND -> sendCommand(entry)
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
        val outcome = try {
            send.get(settings.sendTimeout.toMillis(), TimeUnit.MILLISECONDS)
        } catch (_: TimeoutException) {
            send.cancel(true)
            RawOutcome.Transient("no answer within ${settings.sendTimeout.toMillis()} ms: the call was abandoned and its thread interrupted")
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
        mongo.updateFirst(byId(entry.id), Update().set("attempts", attempts).set("nextAttemptAt", next), SagaDispatchDocument::class.java)
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
        mongo.updateFirst(
            byId(entry.id),
            Update().set("attempts", attempts).set("failedAt", now).set("failureReason", reason).set("failureMessage", message)
                .set("noticePending", true).set("noticeAttempts", 0).set("noticeNextAt", now.plus(settings.lease)),
            SagaDispatchDocument::class.java,
        )
        log.warn("Saga {} command {} ({}) failed with {}: {}", entry.sagaId, entry.id, entry.type, reason, message)
        deliverNoticeQuietly(entry.copy(attempts = attempts, failedAt = now, failureReason = reason, failureMessage = message, noticePending = true, noticeAttempts = 0))
    }

    /** Claims the pending failure notices that are due (up to a batch, not those in flight here), each reserved for this instance by moving `noticeNextAt` a lease ahead. */
    private fun claimDueNotices(): List<SagaDispatchDocument> {
        val claimed = ArrayList<SagaDispatchDocument>()
        while (claimed.size < settings.batchSize && !stopRequested) {
            val now = clock.instant()
            val entry = mongo.findAndModify(
                Query(
                    Criteria.where("noticePending").`is`(true)
                        .orOperator(Criteria.where("noticeNextAt").isNull, Criteria.where("noticeNextAt").lte(now)),
                ).with(Sort.by("noticeNextAt")),
                Update().set("noticeNextAt", now.plus(settings.lease)),
                FindAndModifyOptions.options().returnNew(true),
                SagaDispatchDocument::class.java,
            ) ?: break
            claimed += entry
        }
        return claimed
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
     * dispatcher's markers make every repeat harmless.
     */
    private fun deliverNotice(entry: SagaDispatchDocument) {
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

    private fun clearNotice(entry: SagaDispatchDocument) {
        mongo.updateFirst(byId(entry.id), Update().unset("noticePending").unset("noticeAttempts").unset("noticeNextAt"), SagaDispatchDocument::class.java)
    }

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
        mongo.updateFirst(byId(entry.id), Update().set("noticeAttempts", attempts).set("noticeNextAt", next), SagaDispatchDocument::class.java)
        log.warn("Delivering SagaCommandFailed for command {} failed attempt {} of {}, next at {}: {}", entry.id, attempts, settings.maxAttempts, next, error.toString())
    }

    private fun markDispatched(entry: SagaDispatchDocument) {
        mongo.updateFirst(byId(entry.id), Update().set("dispatchedAt", clock.instant()), SagaDispatchDocument::class.java)
    }

    private fun firstPending(sagaId: String): SagaDispatchDocument? = mongo.findOne(
        Query(Criteria.where("sagaId").`is`(sagaId).and("dispatchedAt").isNull.and("failedAt").isNull).with(Sort.by("seq")).limit(1),
        SagaDispatchDocument::class.java,
    )

    private fun byId(id: String) = Query(Criteria.where("_id").`is`(id))

    // ---- the saga's dispatch lease -----------------------------------------------------------------------------

    /** Takes the lease of [sagaId] when it is free or expired: an upsert on `{_id, until < now}` fails on the duplicate `_id` while it is held. */
    private fun claim(sagaId: String): Boolean {
        val now = clock.instant()
        return try {
            mongo.findAndModify(
                Query(Criteria.where("_id").`is`(sagaId).and("until").lt(now)),
                Update().set("owner", owner).set("until", now.plus(settings.lease)),
                FindAndModifyOptions.options().upsert(true).returnNew(true),
                SagaDispatchLeaseDocument::class.java,
            )
            true
        } catch (_: DuplicateKeyException) {
            false
        }
    }

    private fun renew(sagaId: String): Boolean = mongo.updateFirst(
        Query(Criteria.where("_id").`is`(sagaId).and("owner").`is`(owner)),
        Update().set("until", clock.instant().plus(settings.lease)),
        SagaDispatchLeaseDocument::class.java,
    ).matchedCount == 1L

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
            mongo.remove(Query(Criteria.where("_id").`is`(sagaId).and("owner").`is`(owner)), SagaDispatchLeaseDocument::class.java)
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
