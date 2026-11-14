package io.damoreira.kraken.messaging.store.memory

import io.damoreira.kraken.messaging.deadletter.DeadLetter
import io.damoreira.kraken.messaging.deadletter.DeadLetterError
import io.damoreira.kraken.messaging.deadletter.DeadLetterReason
import io.damoreira.kraken.messaging.eventstore.ReplayRange
import io.damoreira.kraken.messaging.external.ExternalCallStatus
import io.damoreira.kraken.messaging.store.DeadLetterStore
import io.damoreira.kraken.messaging.store.ExternalCallRecord
import io.damoreira.kraken.messaging.store.ExternalCallStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import io.damoreira.kraken.messaging.store.OutboxStore
import io.damoreira.kraken.messaging.store.ProcessedEventStore
import io.damoreira.kraken.messaging.store.ProcessedMarker
import io.damoreira.kraken.messaging.store.ProcessorControlStore
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Companion.KIND_RESET
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Control
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Lock
import io.damoreira.kraken.messaging.store.ProcessorControlStore.Operation
import org.springframework.dao.DuplicateKeyException
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import java.time.Duration
import java.time.Instant

/*
 * The stores of the in-memory message store, with the MongoDB add-on's semantics. What joins the caller's transaction reads and
 * writes through it ([MemoryMessageStore.view] with `joined`); what commits on its own reads and writes the committed rows.
 * Every method runs under the store's lock, so each is atomic.
 */

/**
 * How positions and segments are ordered and matched as the MongoDB add-on stores them: a canonical decimal number is a number
 * (ordered by value, before any text), any other position is text (ordered by its UTF-8 bytes); a number and a text never match
 * one range bound.
 */
internal object StoredPositions {
    private fun number(position: String): Long? = position.toLongOrNull()?.takeIf { it.toString() == position }

    val order: Comparator<String> = Comparator { a, b ->
        val x = number(a)
        val y = number(b)
        when {
            x != null && y != null -> x.compareTo(y)
            x != null -> -1
            y != null -> 1
            else -> compareBytes(a.toByteArray(Charsets.UTF_8), b.toByteArray(Charsets.UTF_8))
        }
    }

    /** Whether [a] and [b] are both numbers or both text (a range or a bound compares only those). */
    fun sameKind(a: String, b: String): Boolean = (number(a) != null) == (number(b) != null)

    private fun compareBytes(a: ByteArray, b: ByteArray): Int {
        for (i in 0 until minOf(a.size, b.size)) {
            val d = (a[i].toInt() and 0xFF) - (b[i].toInt() and 0xFF)
            if (d != 0) return d
        }
        return a.size - b.size
    }
}

/** An outbox entry and the relay's bookkeeping: sent, given up on, and who holds it until when. */
internal data class OutboxRow(
    val entry: OutboxEntry,
    val sentAt: Instant? = null,
    val failedAt: Instant? = null,
    val owner: String? = null,
    val leaseUntil: Instant? = null,
) {
    val unfinished: Boolean get() = sentAt == null && failedAt == null
}

internal class MemoryOutboxStore(private val store: MemoryMessageStore) : OutboxStore {
    private val order = compareBy<OutboxRow>({ it.entry.createdAt }, { it.entry.seq }, { it.entry.id })

    /**
     * Appends [entry] in the store's transaction bound to the calling thread, or at once without one. Inside a transaction of
     * another manager (the application's own database), the entry is appended after that transaction committed and never when
     * it rolled back: after the commit, not atomically with it (the in-memory store cannot join another database).
     */
    override suspend fun append(entry: OutboxEntry) {
        JsonPayloads.requireObject(entry.payload, "outbox entry ${entry.id} (${entry.type})")
        val copy = OutboxEntry(entry.id, entry.type, entry.key, LinkedHashMap(entry.headers), entry.payload.copyOf(), entry.createdAt, entry.seq, entry.attempts)
        if (store.bound() == null && TransactionSynchronizationManager.isSynchronizationActive() &&
            TransactionSynchronizationManager.isActualTransactionActive()
        ) {
            TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                override fun afterCommit() = appendNow(copy)
            })
            return
        }
        appendNow(copy)
    }

    private fun appendNow(copy: OutboxEntry) {
        val entry = copy
        val committed = store.locked {
            val view = store.view(store.outboxTable, joined = true)
            if (view.contains(entry.id)) throw DuplicateKeyException("outbox: an entry ${entry.id} exists")
            view.put(entry.id, OutboxRow(entry))
            val tx = store.bound()
            if (tx != null) tx.appended = true
            tx == null
        }
        if (committed) store.appended()
    }

    override suspend fun claim(owner: String, lease: Duration, limit: Int): List<OutboxEntry> = store.locked {
        val now = store.now()
        val view = store.view(store.outboxTable, joined = false)
        val claimable = view.values().filter { row ->
            row.unfinished && (row.owner == null || row.owner == owner || !row.leaseUntil!!.isAfter(now))
        }.sortedWith(order).take(limit)
        claimable.map { row ->
            view.put(row.entry.id, row.copy(owner = owner, leaseUntil = now.plus(lease)))
            row.entry
        }
    }

    override suspend fun markSent(ids: Collection<String>) = store.locked {
        val view = store.view(store.outboxTable, joined = false)
        val now = store.now()
        for (id in ids) view.update(id) { it.copy(sentAt = now) }
    }

    override suspend fun markAttemptFailed(id: String, error: String) = store.locked {
        store.view(store.outboxTable, joined = false).update(id) { it.copy(entry = it.entry.withAttempts(it.entry.attempts + 1)) }
        Unit
    }

    override suspend fun markFailed(id: String, error: String) = store.locked {
        val now = store.now()
        store.view(store.outboxTable, joined = false).update(id) { it.copy(entry = it.entry.withAttempts(it.entry.attempts + 1), failedAt = now) }
        Unit
    }

    override suspend fun pending(): Long = store.locked { store.view(store.outboxTable, joined = false).values().count { it.unfinished }.toLong() }

    override suspend fun oldestPending(): Instant? = store.locked {
        store.view(store.outboxTable, joined = false).values().filter { it.unfinished }.minWithOrNull(order)?.entry?.createdAt
    }

    override suspend fun purge(sentBefore: Instant): Long = store.locked {
        val view = store.view(store.outboxTable, joined = false)
        val gone = view.values().filter { it.sentAt?.isBefore(sentBefore) == true }
        gone.forEach { view.delete(it.entry.id) }
        gone.size.toLong()
    }

    override fun onAppend(wake: () -> Unit): AutoCloseable = store.addAppendListener(wake)

    private fun OutboxEntry.withAttempts(n: Int) = OutboxEntry(id, type, key, headers, payload, createdAt, seq, n)
}

internal class MemoryProcessedEventStore(private val store: MemoryMessageStore) : ProcessedEventStore {
    override suspend fun record(marker: ProcessedMarker): Boolean = store.locked {
        val view = store.view(store.markerTable, joined = true)
        if (view.contains(marker.id)) return@locked false
        view.put(marker.id, marker)
        true
    }

    override suspend fun exists(id: String): Boolean = store.locked { store.view(store.markerTable, joined = true).contains(id) }

    override suspend fun purge(before: Instant): Long = store.locked {
        val view = store.view(store.markerTable, joined = false)
        val gone = view.values().filter { it.processedAt.isBefore(before) }
        gone.forEach { view.delete(it.id) }
        gone.size.toLong()
    }
}

/** Dead letters commit on their own (but [deleteInTransaction]); a letter is gone once its `expireAt` passed. */
internal class MemoryDeadLetterStore(private val store: MemoryMessageStore) : DeadLetterStore {
    private val byPosition = compareBy<DeadLetter, String>(StoredPositions.order) { it.position }.thenBy { it.id }

    /** The committed letters, the expired ones deleted first. Under the lock. */
    private fun live(): View<DeadLetter> {
        val view = store.view(store.deadLetterTable, joined = false)
        val now = store.now()
        view.values().filter { !it.expireAt.isAfter(now) }.forEach { view.delete(it.id) }
        return view
    }

    private fun blocking(processor: String, key: String) =
        live().values().filter { it.processor == processor && it.key == key && it.reason != DeadLetterReason.INVALID }

    override suspend fun park(letter: DeadLetter): Boolean = store.locked {
        val view = live()
        if (view.contains(letter.id)) return@locked false
        view.put(letter.id, letter.copy(headers = LinkedHashMap(letter.headers), payloadBinary = letter.payloadBinary?.copyOf()))
        true
    }

    override suspend fun head(processor: String, key: String): String? = store.locked { blocking(processor, key).minWithOrNull(byPosition)?.id }

    override suspend fun earlier(processor: String, key: String, position: String): String? = store.locked {
        blocking(processor, key).filter { StoredPositions.sameKind(it.position, position) && StoredPositions.order.compare(it.position, position) < 0 }
            .minWithOrNull(byPosition)?.id
    }

    override suspend fun exists(id: String): Boolean = store.locked { live().contains(id) }

    override suspend fun keys(processor: String, source: String, segments: Collection<String>): Map<String, Set<String>> = store.locked {
        if (segments.isEmpty()) return@locked emptyMap()
        val wanted = segments.toSet()
        live().values()
            .filter { it.processor == processor && it.source == source && it.segment in wanted && it.key != null && it.reason != DeadLetterReason.INVALID }
            .groupBy({ it.segment }, { it.key!! }).mapValues { it.value.toSet() }
    }

    override suspend fun count(processor: String?): Long = store.locked { live().values().count { processor == null || it.processor == processor }.toLong() }

    override suspend fun list(processor: String?, limit: Int): List<DeadLetter> = store.locked {
        live().values().filter { processor == null || it.processor == processor }.sortedWith(compareBy({ it.parkedAt }, { it.id })).take(limit)
    }

    override suspend fun summaries(processor: String): List<DeadLetterStore.Summary> = store.locked {
        live().values().filter { it.processor == processor }.sortedWith(byPosition)
            .map { DeadLetterStore.Summary(it.id, it.key, it.position, it.reason, it.parkedAt) }
    }

    override suspend fun get(id: String): DeadLetter? = store.locked { live().get(id) }

    override suspend fun delete(id: String): Boolean = store.locked { live().delete(id) }

    override suspend fun deleteInTransaction(id: String): Boolean = store.locked {
        live()
        store.view(store.deadLetterTable, joined = true).delete(id)
    }

    override suspend fun recordFailure(id: String, error: DeadLetterError): Boolean = store.locked {
        val now = store.now()
        live().update(id) {
            it.copy(attempts = it.attempts + 1, error = error, lastAttemptAt = now, reason = DeadLetterReason.HANDLER_FAILED, blockedBy = null)
        } != null
    }
}

/** The processors' control documents; every write commits on its own. */
internal class MemoryProcessorControlStore(private val store: MemoryMessageStore) : ProcessorControlStore {
    private fun view() = store.view(store.controlTable, joined = false)

    override suspend fun read(processors: Collection<String>): Map<String, Control> = store.locked {
        val view = view()
        processors.mapNotNull { view.get(it) }.associateBy { it.processor }
    }

    override suspend fun get(processor: String): Control? = store.locked { view().get(processor) }

    override suspend fun setPaused(processor: String, paused: Boolean, by: String) = store.locked {
        val now = store.now()
        val current = view().get(processor) ?: Control(processor)
        view().put(processor, current.copy(paused = paused, pausedAt = if (paused) now else null, pausedBy = if (paused) by else null, updatedAt = now))
    }

    override suspend fun lock(processor: String, op: String, lease: Duration): Lock = store.locked {
        val now = store.now()
        val current = view().get(processor)
        val held = current?.operation
        when {
            held != null && held.id == op -> {
                view().put(processor, current.copy(operation = held.copy(leaseUntil = now.plus(lease)), updatedAt = now))
                Lock.Taken
            }
            held == null || held.expired(now) -> {
                view().put(processor, (current ?: Control(processor)).copy(operation = Operation(op, KIND_RESET, now.plus(lease), now), updatedAt = now))
                Lock.Taken
            }
            else -> Lock.Held(held)
        }
    }

    override suspend fun renew(processor: String, op: String, lease: Duration): Boolean = store.locked {
        val now = store.now()
        val c = view().get(processor)
        val held = c?.operation?.takeIf { it.id == op } ?: return@locked false
        view().put(processor, c.copy(operation = held.copy(leaseUntil = now.plus(lease)), updatedAt = now))
        true
    }

    override suspend fun release(processor: String, op: String, expiredOnly: Boolean): Boolean = store.locked {
        val now = store.now()
        val c = view().get(processor)
        val held = c?.operation?.takeIf { it.id == op } ?: return@locked false
        if (expiredOnly && !held.expired(now)) return@locked false
        view().put(processor, c.copy(operation = null, updatedAt = now))
        true
    }

    override suspend fun setReplay(processor: String, ranges: List<ReplayRange>) = store.locked {
        val now = store.now()
        view().update(processor) { it.copy(replay = ranges.toList(), updatedAt = now) }
        Unit
    }

    override suspend fun removeReplay(processor: String, range: ReplayRange): Boolean = store.locked {
        val now = store.now()
        val c = view().get(processor)?.takeIf { range in it.replay } ?: return@locked false
        view().put(processor, c.copy(replay = c.replay.filter { it != range }, updatedAt = now))
        true
    }

    override suspend fun deleteMarkers(processor: String, ranges: List<ReplayRange>): Long = store.locked {
        val markers = store.view(store.markerTable, joined = false)
        fun inRange(m: ProcessedMarker, r: ReplayRange): Boolean {
            val position = m.position ?: return false
            return m.source == r.source && m.segment == r.segment &&
                StoredPositions.sameKind(position, r.from) && StoredPositions.order.compare(position, r.from) >= 0 &&
                StoredPositions.sameKind(position, r.until) && StoredPositions.order.compare(position, r.until) < 0
        }
        val gone = markers.values().filter { m -> m.processor == processor && ranges.any { inRange(m, it) } }
        gone.forEach { markers.delete(it.id) }
        gone.size.toLong()
    }
}

/** The external calls join the caller's transaction (the core runs each write in a short one of its own). */
internal class MemoryExternalCallStore(private val store: MemoryMessageStore) : ExternalCallStore {
    private fun view() = store.view(store.externalCallTable, joined = true)

    override suspend fun insert(call: ExternalCallRecord): Boolean = store.locked {
        if (view().contains(call.id)) return@locked false
        view().put(call.id, call.copy(reply = call.reply?.copyOf(), error = call.error?.copyOf()))
        true
    }

    override suspend fun find(id: String): ExternalCallRecord? = store.locked { view().get(id) }

    override suspend fun takeOver(
        id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, owner: String, leaseUntil: Instant, now: Instant,
    ): ExternalCallRecord? = store.locked {
        val current = view().get(id)?.takeIf { asRead(it, expectedOwner, expectedLeaseUntil) } ?: return@locked null
        current.copy(owner = owner, leaseUntil = leaseUntil, updatedAt = now, attempts = current.attempts + 1).also { view().put(id, it) }
    }

    override suspend fun giveUp(id: String, expectedOwner: String?, expectedLeaseUntil: Instant?, now: Instant): Boolean =
        settle(id, now, ExternalCallStatus.UNKNOWN) { asRead(it, expectedOwner, expectedLeaseUntil) }

    override suspend fun succeed(id: String, owner: String, reply: ByteArray, now: Instant): Boolean =
        settle(id, now, ExternalCallStatus.SUCCEEDED, reply = reply.copyOf()) { owned(it, owner) }

    override suspend fun fail(id: String, owner: String, error: ByteArray, now: Instant): Boolean =
        settle(id, now, ExternalCallStatus.FAILED, error = error.copyOf()) { owned(it, owner) }

    override suspend fun markUnknown(id: String, owner: String, now: Instant): Boolean = settle(id, now, ExternalCallStatus.UNKNOWN) { owned(it, owner) }

    override suspend fun reset(id: String, owner: String): Boolean = store.locked {
        val current = view().get(id)?.takeIf { owned(it, owner) } ?: return@locked false
        view().delete(current.id)
    }

    /** Settles [id] when [matches]: [status], no owner, no lease, `updatedAt` [now]; a reply or an error when given (SUCCEEDED: no error). */
    private fun settle(
        id: String, now: Instant, status: ExternalCallStatus, reply: ByteArray? = null, error: ByteArray? = null,
        matches: (ExternalCallRecord) -> Boolean,
    ): Boolean = store.locked {
        val current = view().get(id)?.takeIf(matches) ?: return@locked false
        view().put(
            id,
            current.copy(
                status = status, updatedAt = now, owner = null, leaseUntil = null,
                reply = reply ?: current.reply,
                error = if (status == ExternalCallStatus.SUCCEEDED) null else error ?: current.error,
            ),
        )
        true
    }

    private fun owned(r: ExternalCallRecord, owner: String) = r.status == ExternalCallStatus.IN_PROGRESS && r.owner == owner

    private fun asRead(r: ExternalCallRecord, owner: String?, leaseUntil: Instant?) =
        r.status == ExternalCallStatus.IN_PROGRESS && r.owner == owner && r.leaseUntil == leaseUntil
}
