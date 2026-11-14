package io.damoreira.kraken.messaging.store.memory

import org.springframework.dao.ConcurrencyFailureException
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper

/*
 * The in-memory message store's state: one table per collection, read and written under the store's lock, and the journal of
 * a transaction (its writes, applied at its commit). A transaction reads its own writes over what is committed; nobody else
 * sees them before the commit.
 */

/** One committed row: its value and the store-wide version of its last write. */
internal class Row<V : Any>(val value: V, val version: Long)

/** A collection: committed rows by id, and the ids a live transaction wrote (it holds them until it ends). */
internal class Table<V : Any>(val name: String) {
    val rows = HashMap<String, Row<V>>()
    val reserved = HashMap<String, MemoryTransaction>()
}

/** One write of a transaction: the new value (null: deleted) and the version of the committed row it replaces (0: none). */
internal class Pending(var value: Any?, val base: Long)

/** A transaction of the in-memory store: its writes by table and id, in order. */
internal class MemoryTransaction {
    val writes = LinkedHashMap<Table<*>, LinkedHashMap<String, Pending>>()
    var rollbackOnly = false
    var ended = false

    /** It appended to the outbox: the relay is woken up at its commit. */
    var appended = false
}

/**
 * A table as one caller sees it: through the transaction [tx] (its own writes over what is committed; its writes go to its
 * journal and reserve their ids) or, without one, the committed rows (writes commit at once). Used under the store's lock only.
 */
internal class View<V : Any>(private val table: Table<V>, private val tx: MemoryTransaction?, private val store: MemoryMessageStore) {
    @Suppress("UNCHECKED_CAST")
    private val pending: Map<String, Pending> get() = tx?.writes?.get(table).orEmpty()

    fun get(id: String): V? {
        val own = pending[id]
        @Suppress("UNCHECKED_CAST")
        return if (own != null) own.value as V? else table.rows[id]?.value
    }

    fun contains(id: String): Boolean = get(id) != null

    /** Every row, the transaction's writes included. */
    fun values(): List<V> {
        val own = pending
        if (own.isEmpty()) return table.rows.values.map { it.value }
        @Suppress("UNCHECKED_CAST")
        return table.rows.filterKeys { it !in own }.values.map { it.value } + own.values.mapNotNull { it.value as V? }
    }

    fun put(id: String, value: V) = write(id, value)

    /** Deletes [id]; true when it was there. */
    fun delete(id: String): Boolean {
        if (!contains(id)) return false
        write(id, null)
        return true
    }

    /** Replaces [id] by [change] of it; the new value, or null when there is no such row. */
    fun update(id: String, change: (V) -> V): V? {
        val current = get(id) ?: return null
        return change(current).also { put(id, it) }
    }

    private fun write(id: String, value: V?) {
        if (tx == null) {
            if (value == null) table.rows.remove(id) else table.rows[id] = Row(value, store.nextVersion())
            return
        }
        val holder = table.reserved[id]
        if (holder != null && holder !== tx) {
            throw ConcurrencyFailureException("write conflict: another transaction wrote ${table.name} $id and has not ended")
        }
        table.reserved[id] = tx
        val writes = tx.writes.getOrPut(table) { LinkedHashMap() }
        val existing = writes[id]
        if (existing != null) existing.value = value else writes[id] = Pending(value, table.rows[id]?.version ?: 0L)
    }
}

/** What the outbox and the saga dispatch accept as an event's payload (see `OutboxEntry`): a JSON object whose integers fit in a signed 64-bit integer. */
internal object JsonPayloads {
    private val mapper: JsonMapper = JsonMapper.builder().build()

    /** Refuses [payload] with an [IllegalArgumentException] naming [what] unless it is such a JSON object. */
    fun requireObject(payload: ByteArray, what: String) {
        val node = try {
            mapper.readTree(payload)
        } catch (e: RuntimeException) {
            throw IllegalArgumentException("The payload of $what is not a JSON object: ${e.message}", e)
        }
        require(node != null && node.isObject) { "The payload of $what is not a JSON object" }
        require(fits(node)) { "The payload of $what has an integer outside the signed 64-bit range" }
    }

    private fun fits(node: JsonNode): Boolean = if (node.isIntegralNumber) node.canConvertToLong() else node.values().all(::fits)
}
