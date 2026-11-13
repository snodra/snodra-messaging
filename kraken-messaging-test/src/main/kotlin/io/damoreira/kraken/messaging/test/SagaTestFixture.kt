package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.saga.Association
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.DeadlineStatus
import io.damoreira.kraken.messaging.saga.DispatchKind
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.store.DeadlineStore
import io.damoreira.kraken.messaging.store.SagaDispatchRecorder
import io.damoreira.kraken.messaging.store.SagaFailureStore
import io.damoreira.kraken.messaging.store.SagaMarkerStore
import io.damoreira.kraken.messaging.store.SagaRepository
import io.damoreira.kraken.messaging.store.SagaStores
import org.springframework.dao.DuplicateKeyException
import org.springframework.transaction.TransactionStatus
import org.springframework.transaction.support.SimpleTransactionStatus
import org.springframework.transaction.support.TransactionCallback
import org.springframework.transaction.support.TransactionOperations
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneId
import java.util.UUID
import kotlin.reflect.KClass

/**
 * Drives one saga class with events and time, in memory: no Kafka, no MongoDB, no Spring context. Usage:
 *
 * ```kotlin
 * SagaTestFixture(PaymentSaga::class)
 *     .givenCurrentTime(Instant.parse("2026-11-03T10:00:00Z"))
 *     .whenEvent(OrderPlaced("o1", 42))
 *     .expectActiveSagas(1)
 *     .expectAssociation("paymentRef", "pay-o1")
 *     .expectDispatchedCommands(RequestCharge("pay-o1", 42))
 *     .expectScheduledDeadline("charge-timeout", Instant.parse("2026-11-03T10:15:00Z"))
 * ```
 *
 * It runs the library's own [SagaRuntime] (the same handler discovery, association lookup, exactly-once markers, version
 * checks and deadline firing) over in-memory implementations of its persistence ports, so a saga that behaves here
 * behaves the same on MongoDB. Every step encodes the saga state as JSON after the handlers ran and decodes it again
 * before the next one, so state the library's [JsonCodec] cannot carry fails the step instead of passing in memory. Each
 * step is atomic: a handler that throws leaves nothing behind.
 *
 * Steps fail with an [IllegalStateException] when the saga does (a handler threw, the state is not serialisable, a
 * handler parameter has no bean) and without retries; expectations fail with an [AssertionError] saying what was
 * expected and what was found. Every step returns the fixture, so a test reads as one chain.
 *
 * Time is the fixture's own clock, set by [givenCurrentTime] (default 2026-01-01T00:00:00Z). [whenTimeElapses] advances
 * it and fires every deadline that falls due, each at its own due time and in due order, so a deadline that
 * reschedules itself (the polling pattern) fires once per interval.
 *
 * What the dispatch expectations see is what the sagas sent since the last `given…` step: the commands and events of
 * all `when…` steps after it, in order. [beans] are what handler parameters of "any Spring bean" type resolve to (the
 * first one that is an instance of the parameter's type); [typePrefix] names messages that have no `@MessageType`.
 */
class SagaTestFixture<S : Any>(type: KClass<S>, vararg beans: Any, typePrefix: String = "test") {
    private val codec = JsonCodec()
    private val resolver = MessageTypeResolver(typePrefix)
    private val clock = FixtureClock(DEFAULT_TIME)
    private val store = InMemorySagaStore()
    private val sagaTypes = SagaModelScanner(resolver, codec).scan(listOf(type))
    private val sagaName = sagaTypes.single().name
    private val runtime = SagaRuntime(
        types = sagaTypes,
        ports = store.ports(),
        tx = store,
        codec = codec,
        typeResolver = resolver,
        beans = { klass ->
            beans.firstOrNull { klass.isInstance(it) }
                ?: throw IllegalStateException(
                    "The saga asks for a bean of type ${klass.qualifiedName}, but the fixture has none: " +
                        "pass one as SagaTestFixture(${type.simpleName}::class, bean)",
                )
        },
        clock = clock,
        retry = SagaRetry(RetryPolicy(maxAttempts = 1), sleeper = {}),
    )

    /** Where the observed dispatch entries start: the end of the given phase. */
    private var observedFrom = 0

    // ---- given -------------------------------------------------------------------------------------------------

    /** Delivers [events] to the saga without observing what they cause: the expectations start after them. */
    fun givenEvents(vararg events: Event): SagaTestFixture<S> {
        events.forEach { deliver(it, emptyMap()) }
        observedFrom = store.dispatch.size
        return this
    }

    /** Sets the fixture's clock to [now]; deadlines are scheduled against it. */
    fun givenCurrentTime(now: Instant): SagaTestFixture<S> {
        clock.now = now
        observedFrom = store.dispatch.size
        return this
    }

    // ---- when --------------------------------------------------------------------------------------------------

    /** Delivers [event] (with message [headers], which `associationHeader` and `MessageContext` see) to the saga. */
    fun whenEvent(event: Event, headers: Map<String, String> = emptyMap()): SagaTestFixture<S> {
        deliver(event, headers)
        return this
    }

    /**
     * Tells the saga that [command], which it sent, failed with [reason], as the dispatch relay does: delivers a
     * `SagaCommandFailed` for the saga instance that sent it, with its idempotency key. [command] is matched by type and
     * JSON payload against everything the sagas recorded (the last such entry when several are equal).
     */
    fun whenCommandFailed(command: Command<*>, reason: SagaCommandFailed.Reason, message: String = ""): SagaTestFixture<S> {
        val type = resolver.typeNameOf(command::class)
        val payload = codec.mapper.readTree(codec.encode(command))
        val sent = store.dispatch.lastOrNull { it.kind == DispatchKind.COMMAND && it.type == type && codec.mapper.readTree(it.payload) == payload }
            ?: throw IllegalStateException("$sagaName: no saga sent $type $payload, so it cannot have failed; the commands sent are ${show(store.dispatch.filter { it.kind == DispatchKind.COMMAND }.map { it.type to codec.mapper.readTree(it.payload) })}")
        deliver(SagaCommandFailed(sent.sagaId, sent.type, sent.id, reason, message), emptyMap(), messageId = "failed:${sent.id}")
        return this
    }

    /** Advances the clock by [duration], firing the deadlines that fall due on the way, each at its due time. */
    fun whenTimeElapses(duration: Duration): SagaTestFixture<S> {
        require(!duration.isNegative) { "Time cannot elapse backwards: $duration" }
        val target = clock.now.plus(duration)
        var fired = 0
        while (true) {
            val next = store.deadlines.values.filter { it.status == DeadlineStatus.SCHEDULED && it.dueAt <= target }.minByOrNull { it.dueAt } ?: break
            check(++fired <= MAX_DEADLINES_PER_STEP) { "More than $MAX_DEADLINES_PER_STEP deadlines fell due in one step: a deadline of $sagaName keeps rescheduling itself" }
            if (next.dueAt > clock.now) clock.now = next.dueAt
            try {
                runtime.applyDeadline(next)
            } catch (e: Exception) {
                throw IllegalStateException("Saga $sagaName failed firing its deadline '${next.name}': ${describe(e)}", e)
            }
        }
        clock.now = target
        return this
    }

    // ---- expect ------------------------------------------------------------------------------------------------

    /** The number of live (not ended) saga instances. */
    fun expectActiveSagas(count: Int): SagaTestFixture<S> {
        val live = store.sagas.values.count { !it.ended }
        if (live != count) fail("expected $count active sagas but found $live")
        return this
    }

    /** Some live instance is associated with `key = value`. */
    fun expectAssociation(key: String, value: String): SagaTestFixture<S> {
        val live = store.sagas.values.filter { !it.ended }
        if (live.none { Association(key, value) in it.associations }) {
            fail("expected a live saga associated with $key=$value but found ${live.map { s -> s.associations.joinToString(prefix = "[", postfix = "]") { "${it.k}=${it.v}" } }}")
        }
        return this
    }

    /** Exactly these commands were sent, in this order (compared by type and JSON payload); none when called without arguments. */
    fun expectDispatchedCommands(vararg commands: Command<*>): SagaTestFixture<S> = expectDispatched(DispatchKind.COMMAND, "commands", commands.toList())

    /** Exactly these events were published, in this order (compared by type and JSON payload); none when called without arguments. */
    fun expectPublishedEvents(vararg events: Event): SagaTestFixture<S> = expectDispatched(DispatchKind.EVENT, "events", events.toList())

    /** A deadline [name] is scheduled to fire at exactly [at]. */
    fun expectScheduledDeadline(name: String, at: Instant): SagaTestFixture<S> {
        val scheduled = scheduled()
        if (scheduled.none { it.name == name && it.dueAt == at }) {
            fail("expected a deadline '$name' at $at but found ${describeDeadlines(scheduled)}")
        }
        return this
    }

    /** No deadline is scheduled any more (every one fired or was cancelled). */
    fun expectNoScheduledDeadlines(): SagaTestFixture<S> {
        val scheduled = scheduled()
        if (scheduled.isNotEmpty()) fail("expected no scheduled deadlines but found ${describeDeadlines(scheduled)}")
        return this
    }

    /** The saga ended: it started at least one instance and none is live. */
    fun expectSagaEnded(): SagaTestFixture<S> {
        if (store.sagas.isEmpty()) fail("expected the saga to have ended but no instance was ever started")
        val live = store.sagas.values.count { !it.ended }
        if (live > 0) fail("expected the saga to have ended but $live of ${store.sagas.size} instances are not ended")
        return this
    }

    // ---- internals ---------------------------------------------------------------------------------------------

    private fun deliver(event: Event, headers: Map<String, String>, messageId: String = UUID.randomUUID().toString()) {
        val type = resolver.typeNameOf(event::class)
        if (!runtime.interested(type)) return
        val ctx = MessageContext(messageId, type, messageId, null, emptySet(), null, headers)
        runtime.process(type, codec.encode(event), ctx)
        // The runtime parks an event whose handler keeps failing and moves on; a test must see that as a failure.
        val failure = store.failures.values.firstOrNull() ?: return
        store.failures.clear()
        throw IllegalStateException(
            "Saga ${failure.sagaType} failed handling $type: ${failure.error}. " +
                "A step fails when a handler throws or when the saga state cannot be written to or read from JSON (the library stores a saga with its JsonCodec).",
        )
    }

    private fun expectDispatched(kind: DispatchKind, label: String, expected: List<Any>): SagaTestFixture<S> {
        val found = store.dispatch.drop(observedFrom).filter { it.kind == kind }.map { it.type to codec.mapper.readTree(it.payload) }
        val wanted = expected.map { resolver.typeNameOf(it::class) to codec.mapper.readTree(codec.encode(it)) }
        if (found != wanted) {
            fail(if (wanted.isEmpty()) "expected no $label but found ${show(found)}" else "expected $label ${show(wanted)} but found ${show(found)}")
        }
        return this
    }

    private fun show(messages: List<Pair<String, Any>>) = messages.joinToString(prefix = "[", postfix = "]") { "${it.first} ${it.second}" }

    private fun scheduled() = store.deadlines.values.filter { it.status == DeadlineStatus.SCHEDULED }

    private fun describeDeadlines(deadlines: List<DeadlineDocument>) =
        if (deadlines.isEmpty()) "none" else deadlines.joinToString(prefix = "[", postfix = "]") { "'${it.name}' at ${it.dueAt}" }

    private fun fail(message: String): Nothing = throw AssertionError("$sagaName: $message")

    private fun describe(e: Throwable): String {
        val root = generateSequence(e) { t -> t.cause?.takeIf { it !== t } }.last()
        return "${root.javaClass.name}: ${root.message}"
    }

    private companion object {
        val DEFAULT_TIME: Instant = Instant.parse("2026-01-01T00:00:00Z")
        const val MAX_DEADLINES_PER_STEP = 10_000
    }
}

/** A clock the fixture moves. */
private class FixtureClock(@Volatile var now: Instant) : Clock() {
    override fun getZone(): ZoneId = ZoneId.of("UTC")
    override fun withZone(zone: ZoneId?): Clock = this
    override fun instant(): Instant = now
}

/**
 * The saga persistence ports in memory, and the transaction: a step that throws restores the state it started from, as
 * a rolled-back Mongo transaction would (the exactly-once marker included, so a failing event is parked, not skipped).
 */
private class InMemorySagaStore : TransactionOperations {
    val sagas = LinkedHashMap<String, SagaDocument>()
    val dispatch = ArrayList<SagaDispatchDocument>()
    val deadlines = LinkedHashMap<String, DeadlineDocument>()
    val failures = LinkedHashMap<String, SagaFailureDocument>()
    val markers = HashSet<String>()

    override fun <T> execute(action: TransactionCallback<T>): T {
        val before = Snapshot(LinkedHashMap(sagas), ArrayList(dispatch), LinkedHashMap(deadlines), LinkedHashMap(failures), HashSet(markers))
        val status: TransactionStatus = SimpleTransactionStatus()
        try {
            return action.doInTransaction(status)
        } catch (e: Throwable) {
            restore(before)
            throw e
        }
    }

    private class Snapshot(
        val sagas: Map<String, SagaDocument>, val dispatch: List<SagaDispatchDocument>, val deadlines: Map<String, DeadlineDocument>,
        val failures: Map<String, SagaFailureDocument>, val markers: Set<String>,
    )

    private fun restore(s: Snapshot) {
        sagas.clear(); sagas.putAll(s.sagas)
        dispatch.clear(); dispatch.addAll(s.dispatch)
        deadlines.clear(); deadlines.putAll(s.deadlines)
        failures.clear(); failures.putAll(s.failures)
        markers.clear(); markers.addAll(s.markers)
    }

    fun ports() = SagaStores(
        sagas = object : SagaRepository {
            override fun findById(id: String) = sagas[id]

            override fun findLive(type: String, key: String, value: String) =
                sagas.values.filter { it.type == type && !it.ended && Association(key, value) in it.associations }

            override fun insert(saga: SagaDocument) {
                if (saga.id in sagas) throw DuplicateKeyException("saga ${saga.id} exists")
                if (saga.startKey != null && sagas.values.any { !it.ended && it.startKey == saga.startKey }) {
                    throw DuplicateKeyException("a live saga holds the start key ${saga.startKey}")
                }
                sagas[saga.id] = saga
            }

            override fun update(saga: SagaDocument, expectedVersion: Long): Boolean {
                if (sagas[saga.id]?.version != expectedVersion) return false
                sagas[saga.id] = saga
                return true
            }
        },
        dispatch = object : SagaDispatchRecorder {
            override fun record(entries: List<SagaDispatchDocument>) {
                dispatch += entries
            }
        },
        deadlines = object : DeadlineStore {
            override fun insert(deadline: DeadlineDocument) {
                deadlines[deadline.id] = deadline
            }

            override fun findScheduled(id: String) = deadlines[id]?.takeIf { it.status == DeadlineStatus.SCHEDULED }

            override fun cancel(sagaId: String, deadlineId: String, now: Instant) = cancelWhere(now) { it.sagaId == sagaId && it.id == deadlineId }

            override fun cancelByName(sagaId: String, name: String, now: Instant) = cancelWhere(now) { it.sagaId == sagaId && it.name == name }

            override fun cancelAll(sagaId: String, now: Instant) = cancelWhere(now) { it.sagaId == sagaId }

            private fun cancelWhere(now: Instant, matches: (DeadlineDocument) -> Boolean) {
                deadlines.replaceAll { _, d ->
                    if (d.status == DeadlineStatus.SCHEDULED && matches(d)) d.copy(status = DeadlineStatus.CANCELLED, finishedAt = now) else d
                }
            }

            override fun markFired(id: String, now: Instant): Boolean {
                val d = deadlines[id]?.takeIf { it.status == DeadlineStatus.SCHEDULED } ?: return false
                deadlines[id] = d.copy(status = DeadlineStatus.FIRED, finishedAt = now)
                return true
            }
        },
        failures = object : SagaFailureStore {
            override fun find(id: String) = failures[id]

            override fun save(failure: SagaFailureDocument) {
                failures[failure.id] = failure
            }

            override fun delete(id: String) = failures.remove(id) != null

            override fun failedAgain(id: String, error: String, attempts: Int, now: Instant): Boolean {
                val failure = failures[id] ?: return false
                failures[id] = failure.copy(attempts = failure.attempts + attempts, error = error, lastFailedAt = now)
                return true
            }

            override fun list(sagaType: String?) = failures.values.filter { sagaType == null || it.sagaType == sagaType }.sortedBy { it.lastFailedAt }
        },
        markers = object : SagaMarkerStore {
            override fun exists(id: String) = id in markers

            override fun insert(id: String, type: String, now: Instant) {
                if (!markers.add(id)) throw DuplicateKeyException("marker $id exists")
            }
        },
    )
}
