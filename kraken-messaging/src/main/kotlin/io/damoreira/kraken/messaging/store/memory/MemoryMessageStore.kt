package io.damoreira.kraken.messaging.store.memory

import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.store.DeadLetterStore
import io.damoreira.kraken.messaging.store.ExternalCallRecord
import io.damoreira.kraken.messaging.store.ExternalCallStore
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.OutboxStore
import io.damoreira.kraken.messaging.store.ProcessedEventStore
import io.damoreira.kraken.messaging.store.ProcessedMarker
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import io.damoreira.kraken.messaging.store.SagaStores
import org.springframework.dao.ConcurrencyFailureException
import org.springframework.dao.DuplicateKeyException
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.time.Clock
import java.time.Instant
import java.util.concurrent.CopyOnWriteArrayList

/**
 * The in-memory message store (ADR 0028): every store of the library (outbox, processed events, dead letters, processor
 * control, external calls, the saga ports) as maps in this JVM's memory, for tests, local runs and single-process demos.
 * **Not durable**: everything is gone when the process ends, and only this JVM (this instance of the store) sees it. Name
 * `memory`; no schema.
 *
 * **Transactions** are Spring's, through [transactions] (a [MemoryTransactionManager]): what a store joining the caller's
 * transaction writes (the outbox, the processed events, a dead letter's deletion in a retry, the external calls, the saga ports)
 * goes to the transaction's journal, which its own reads see and nobody else does until the commit applies it at once; a
 * rollback drops it. Two live transactions that write the same row conflict at the second write, and a transaction whose rows
 * changed under it since it wrote them fails at its commit; both with a [ConcurrencyFailureException] (transient: a retry
 * resolves it), as a database's write conflict. What commits on its own (the dead letters, the processor control, the relay's
 * claims) applies at once, atomically. Times are stamped with [clock]; leases are compared with it.
 *
 * The relay is woken up as soon as an append commits ([OutboxStore.onAppend]). Nothing expires but dead letters (at their
 * `expireAt`): a long run keeps the rest in memory.
 */
class MemoryMessageStore(internal val clock: Clock = Clock.systemUTC()) : MessageStore {
    override val name: String = NAME

    private val lock = Any()
    private var version = 0L
    private val appendListeners = CopyOnWriteArrayList<AppendListener>()

    /** The key the [MemoryTransactionManager] binds this store's transaction under. */
    internal val transactionKey = Any()

    internal val outboxTable = Table<OutboxRow>("outbox")
    internal val markerTable = Table<ProcessedMarker>("processed_events")
    internal val deadLetterTable = Table<DeadLetter>("dead_letters")
    internal val controlTable = Table<ProcessorControlStore.Control>("processor_control")
    internal val externalCallTable = Table<ExternalCallRecord>("external_calls")
    internal val sagaTable = Table<SagaDocument>("sagas")
    internal val dispatchTable = Table<SagaDispatchDocument>("saga_dispatch")
    internal val leaseTable = Table<DispatchLease>("saga_dispatch_leases")
    internal val deadlineTable = Table<DeadlineDocument>("deadlines")
    internal val failureTable = Table<SagaFailureDocument>("saga_failures")

    override val transactions: PlatformTransactionManager = MemoryTransactionManager(this)

    override val outbox: OutboxStore = MemoryOutboxStore(this)

    override val processedEvents: ProcessedEventStore = MemoryProcessedEventStore(this)

    override val deadLetters: DeadLetterStore = MemoryDeadLetterStore(this)

    override val processorControl: ProcessorControlStore = MemoryProcessorControlStore(this)

    override val externalCalls: ExternalCallStore = MemoryExternalCallStore(this)

    override val sagas: SagaStores = memorySagaStores(this)

    internal fun <T> locked(block: () -> T): T = synchronized(lock, block)

    internal fun nextVersion(): Long = ++version

    internal fun now(): Instant = clock.instant()

    /**
     * The live transaction bound to the calling thread, if any; one that ended (a call from its after-completion callbacks) is
     * none: such a write commits on its own.
     */
    internal fun bound(): MemoryTransaction? =
        (TransactionSynchronizationManager.getResource(transactionKey) as? MemoryTransaction)?.takeIf { !it.ended }

    /** [table] as the calling thread sees it: through its transaction when [joined] and one is bound, else as committed. */
    internal fun <V : Any> view(table: Table<V>, joined: Boolean): View<V> = View(table, if (joined) bound() else null, this)

    /** Adds [listener] until the handle returned is closed. */
    internal fun addAppendListener(listener: () -> Unit): AutoCloseable {
        val registered = AppendListener(listener) // its own identity: one registration, one removal
        appendListeners += registered
        return AutoCloseable { appendListeners.remove(registered) }
    }

    /** How many append listeners are registered (for tests). */
    internal fun appendListenerCount(): Int = appendListeners.size

    private class AppendListener(val wake: () -> Unit)

    /** An entry was appended and committed: wakes the relays up. */
    internal fun appended() {
        for (listener in appendListeners) runCatching { listener.wake() }
    }

    /**
     * Applies [tx]: fails with a [ConcurrencyFailureException] when a row it wrote changed since (nothing applied), with a
     * [DuplicateKeyException] when it would leave two live sagas with one start key, else applies every write at once.
     */
    internal fun commit(tx: MemoryTransaction) {
        if (tx.ended) return
        try {
            locked {
                for ((table, writes) in tx.writes) {
                    for ((id, write) in writes) {
                        if ((table.rows[id]?.version ?: 0L) != write.base) {
                            throw ConcurrencyFailureException("write conflict: ${table.name} $id changed since this transaction wrote it")
                        }
                    }
                }
                checkLiveStartKeys(tx)
                for ((table, writes) in tx.writes) apply(table, writes)
            }
        } finally {
            end(tx)
        }
        if (tx.appended) appended()
    }

    /** Drops [tx]. Idempotent. */
    internal fun rollback(tx: MemoryTransaction) {
        if (!tx.ended) end(tx)
    }

    private fun end(tx: MemoryTransaction) = locked {
        tx.ended = true
        for ((table, writes) in tx.writes) for (id in writes.keys) if (table.reserved[id] === tx) table.reserved.remove(id)
    }

    @Suppress("UNCHECKED_CAST")
    private fun <V : Any> apply(table: Table<V>, writes: Map<String, Pending>) {
        for ((id, write) in writes) {
            val value = write.value as V?
            if (value == null) table.rows.remove(id) else table.rows[id] = Row(value, nextVersion())
        }
    }

    /** A live saga's start key is unique among the live sagas (the MongoDB add-on's partial unique index). */
    private fun checkLiveStartKeys(tx: MemoryTransaction) {
        val writes = tx.writes[sagaTable] ?: return
        val after = View(sagaTable, tx, this).values()
        for (pending in writes.values) {
            val saga = pending.value as SagaDocument? ?: continue
            val key = saga.startKey ?: continue
            if (saga.ended) continue
            if (after.any { it.id != saga.id && !it.ended && it.startKey == key }) {
                throw DuplicateKeyException("sagas: the start key $key is held by a live saga")
            }
        }
    }

    companion object {
        /** The message store's name (`kraken.messaging.store.type=memory`). */
        const val NAME = "memory"
    }
}
