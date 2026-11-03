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
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

/**
 * Settings of the [SagaDispatchRelay] (`kraken.messaging.sagas.relay.*`, S1 plan R8): how often it polls, how long a saga's
 * dispatch lease holds, and the transient retry of a command (up to [maxAttempts] attempts in all, then `EXHAUSTED`, with
 * [initialBackoff] growing by [multiplier] up to [maxBackoff]). [parallelism] sagas are dispatched at a time, at most
 * [batchSize] of them per poll.
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
) {
    init {
        require(!pollInterval.isZero && !pollInterval.isNegative) { "pollInterval must be positive, was $pollInterval" }
        require(!lease.isZero && !lease.isNegative) { "lease must be positive, was $lease" }
        require(parallelism >= 1 && batchSize >= 1) { "parallelism and batchSize must be >= 1" }
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
 * - A COMMAND entry is sent as raw bytes by wire type name ([RawCommandSender]) with the headers `idempotency-key` =
 *   `<sagaId>:<seq>` (the entry's id) and `saga-id`, the entry's routing key and the correlation id of the message that
 *   caused it, outside any caller identity. It is marked dispatched after the reply, so a crash in between re-sends it
 *   with the same key (an `@IdempotentHandler` adapter then runs its body once).
 * - A transient failure counts an attempt and delays the entry (`nextAttemptAt`), holding back that saga's later entries
 *   only; after [RelaySettings.maxAttempts] it becomes `EXHAUSTED`. A permanent failure becomes a [SagaCommandFailed]
 *   at once: dispatched locally (dedupe `failed:<entry id>`) first, then the entry is marked failed, so a crash in between
 *   re-sends the command and delivers the event again (skipped by its marker). Later entries of the saga continue after a
 *   failed one: the saga decides what the failure means. A [SagaCommandFailed] nobody handled is logged at error and counted.
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

    /**
     * One pass: dispatches the due entries of every claimable saga and returns how many entries it dealt with (sent,
     * published, failed or put back to wait). Never throws for one saga's trouble: it is logged and tried again next pass.
     */
    fun pollOnce(): Int {
        val sagas = dueSagas()
        if (sagas.isEmpty()) return 0
        if (sagas.size == 1 || settings.parallelism == 1) return sagas.sumOf { dispatchSaga(it) }
        val futures = sagas.map { executor().submit(Callable { dispatchSaga(it) }) }
        return futures.sumOf { it.get() }
    }

    /** The sagas whose first pending entry is due, oldest work first. */
    private fun dueSagas(): List<String> {
        val now = Date.from(clock.instant())
        val aggregation = Aggregation.newAggregation(
            Aggregation.match(Criteria.where("dispatchedAt").isNull.and("failedAt").isNull),
            Aggregation.sort(Sort.by("sagaId", "seq")),
            Aggregation.group("sagaId").first("nextAttemptAt").`as`("next"),
            Aggregation.match(Criteria().orOperator(Criteria.where("next").isNull, Criteria.where("next").lte(now))),
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
        val outcome = try {
            runBlocking(CorrelationContext(correlationId)) {
                commands.sendRaw(entry.type, entry.payload.toByteArray(Charsets.UTF_8), SendOptions(routingKey = entry.routingKey, headers = headers))
            }
        } catch (e: Exception) {
            if (e is InterruptedException) Thread.currentThread().interrupt()
            RawFailures.classify(e)
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

    /** Tells the saga its command failed, then gives the entry up. */
    private fun fail(entry: SagaDispatchDocument, reason: SagaCommandFailed.Reason, message: String, attempts: Int = entry.attempts) {
        val failed = SagaCommandFailed(entry.sagaId, entry.type, entry.id, reason, message)
        val type = typeResolver.typeNameOf(SagaCommandFailed::class)
        val correlationId = entry.headers[DISPATCH_CORRELATION_HEADER] ?: entry.id
        val dedupeId = "failed:${entry.id}"
        val ctx = MessageContext(
            messageId = dedupeId, type = type, correlationId = correlationId, userId = null, roles = emptySet(), authorization = null,
            headers = mapOf("id" to dedupeId, "type" to type, "correlationId" to correlationId),
        )
        val outcome = local.dispatch(dedupeId, type, codec.encode(failed), ctx)
        if (outcome == LocalEventDispatcher.Outcome.NO_INTEREST || outcome == LocalEventDispatcher.Outcome.UNCLAIMED) {
            metrics.commandFailureUnhandled(entry.type, reason)
            log.error("Saga {} command {} ({}) failed with {} and no live saga with a SagaCommandFailed handler and no ordinary handler took it: {}", entry.sagaId, entry.id, entry.type, reason, message)
        } else {
            log.warn("Saga {} command {} ({}) failed with {}: {}", entry.sagaId, entry.id, entry.type, reason, message)
        }
        mongo.updateFirst(byId(entry.id), Update().set("attempts", attempts).set("failedAt", clock.instant()), SagaDispatchDocument::class.java)
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
                pollOnce()
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
            pool?.awaitTermination(STOP_JOIN_MS, TimeUnit.MILLISECONDS)
            pool = null
        }
    }

    override fun isRunning(): Boolean = running

    /** Starts after the outbox publisher and stops before the event consumer (which feeds the sagas whose failures it delivers). */
    override fun getPhase(): Int = PHASE

    companion object {
        const val PHASE = SmartLifecycle.DEFAULT_PHASE - 1850
        private const val STOP_JOIN_MS = 15_000L
    }
}
