package io.damoreira.kraken.messaging.jdbc

import org.springframework.dao.DataAccessException
import org.springframework.dao.DuplicateKeyException
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.core.PreparedStatementCreator
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate
import org.springframework.jdbc.core.namedparam.SqlParameterSource
import org.springframework.jdbc.core.simple.JdbcClient
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.jdbc.datasource.DelegatingDataSource
import org.springframework.jdbc.support.SQLErrorCodeSQLExceptionTranslator
import org.springframework.jdbc.support.SQLExceptionTranslator
import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.TransactionSynchronizationManager
import org.springframework.transaction.support.TransactionTemplate
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import java.sql.BatchUpdateException
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Clock
import java.time.Instant
import javax.sql.DataSource

/**
 * How the stores reach the database. Two ways, by what a write must commit with:
 *
 * - [tx]: with the service's writes. In the transaction active on the calling thread (the service's: a
 *   `DataSourceTransactionManager` or a `JpaTransactionManager` over the same DataSource bind its connection), else in a short
 *   transaction of its own: never on a bare connection, whose auto-commit the pool decides (a pool configured
 *   `auto-commit=false` would roll the write back when the connection returns). A transaction of another manager active on the
 *   thread (one that does not hold this DataSource's connection) is refused.
 * - [own]: in a transaction of its own, on a connection of its own, whatever the caller has: what commits on its own (dead
 *   letters, processor control, the relays' and schedulers' claims, the purge). The connection comes from the same DataSource
 *   through a distinct key, so Spring never hands it the caller's. These transactions run at READ COMMITTED: on MySQL, a
 *   locking read at REPEATABLE READ keeps every row it scanned locked, so one relay's claim would hide the rows another relay
 *   holds from that relay's own `SKIP LOCKED` claim.
 */
internal class Db(
    val dataSource: DataSource,
    val dialect: SqlDialect,
    private val clock: Clock,
) {
    private val translator = DialectTranslator(dialect, SQLErrorCodeSQLExceptionTranslator(dataSource))

    private val joinedTemplate = JdbcTemplate(dataSource).also { it.exceptionTranslator = translator }
    private val joinedClient = JdbcClient.create(NulRefusingTemplate(joinedTemplate))
    private val localTransactions = TransactionTemplate(DataSourceTransactionManager(dataSource))

    /** The same pool behind a key of its own: a transaction bound to [dataSource] is never this one's. */
    private val ownDataSource: DataSource = object : DelegatingDataSource(dataSource) {}
    private val ownTemplate = JdbcTemplate(ownDataSource).also { it.exceptionTranslator = translator }
    private val ownClient = JdbcClient.create(NulRefusingTemplate(ownTemplate))
    private val ownTransactions = TransactionTemplate(DataSourceTransactionManager(ownDataSource)).also {
        it.propagationBehavior = TransactionDefinition.PROPAGATION_REQUIRES_NEW
        it.isolationLevel = TransactionDefinition.ISOLATION_READ_COMMITTED
    }

    /** A batch of [sql] (positional parameters) over [rows]: only inside a [tx] block. */
    fun batch(sql: String, rows: List<Array<Any?>>) {
        rows.forEach { Limits.refuseNul("a parameter", it) }
        joinedTemplate.batchUpdate(sql, rows)
    }

    /**
     * Runs [block] with the service's writes: in the caller's transaction, else in a short one of its own. A transaction still
     * marked active while its synchronizations are gone has ended: its after-completion phase runs (where a
     * `@TransactionalEventListener(AFTER_COMMIT)` runs); [block] then runs in a transaction of its own, on its own connection, so it
     * commits whatever the pool's auto-commit (on the ended transaction's connection it would be rolled back when that returns).
     */
    fun <T> tx(block: (JdbcClient) -> T): T {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            if (!TransactionSynchronizationManager.isSynchronizationActive()) return own(block)
            checkJoinable()
            return block(joinedClient)
        }
        return localTransactions.execute { block(joinedClient) }
    }

    /** Runs [block] in a transaction of its own (READ COMMITTED), committed whatever the caller's does. */
    fun <T> own(block: (JdbcClient) -> T): T = ownTransactions.execute { block(ownClient) }

    /** The application clock's now, at millisecond precision. */
    fun now(): Instant = SqlDialect.millis(clock.instant())

    /** [instant] as a timestamp parameter (null stays null). */
    fun ts(instant: Instant?): Any? = instant?.let(dialect::timestamp)

    fun instant(rs: ResultSet, column: String): Instant? = dialect.instant(rs, column)

    /**
     * Inserts a row unless one with its key exists, through [client] (a [tx] or [own] one); false when it existed. On PostgreSQL without an error (a duplicate key would abort the caller's transaction), on MySQL by the error.
     */
    fun insertOnce(client: JdbcClient, table: String, columns: String, values: String, key: String, params: Map<String, Any?>): Boolean =
        if (dialect.duplicateAbortsTransaction) {
            client.sql(dialect.insertUnlessExists(table, columns, values, key)).params(params).update() == 1
        } else {
            try {
                client.sql("INSERT INTO $table ($columns) VALUES ($values)").params(params).update()
                true
            } catch (_: DuplicateKeyException) {
                false
            }
        }

    /**
     * A batched delete of the rows of [table] (primary key [key]) matching [condition], [BATCH] at a time, each batch in a
     * transaction of its own; how many were deleted.
     */
    fun purge(table: String, key: String, condition: String, params: Map<String, Any?>): Long {
        val sql = when (dialect) {
            PostgresDialect -> "DELETE FROM $table WHERE $key IN (SELECT $key FROM $table WHERE $condition LIMIT $BATCH)"
            MySqlDialect -> "DELETE FROM $table WHERE $condition LIMIT $BATCH"
        }
        var total = 0L
        while (true) {
            val n = own { it.sql(sql).params(params).update() }
            total += n
            if (n < BATCH) return total
        }
    }

    private fun checkJoinable() {
        check(!TransactionSynchronizationManager.isActualTransactionActive() || TransactionSynchronizationManager.hasResource(dataSource)) {
            "A transaction is active, but not on the message store's DataSource: the outbox and the markers would not commit or " +
                "roll back with it. Run the store under a DataSourceTransactionManager or a JpaTransactionManager over the same DataSource."
        }
    }

    companion object {
        const val BATCH = 1000
    }
}

/** The dialect's mapping first ([SqlDialect.translate]), then Spring's by error code. */
private class DialectTranslator(private val dialect: SqlDialect, private val fallback: SQLExceptionTranslator) : SQLExceptionTranslator {
    override fun translate(task: String, sql: String?, ex: SQLException): DataAccessException? {
        val cause = (ex as? BatchUpdateException)?.nextException ?: ex
        return dialect.translate(cause) ?: fallback.translate(task, sql, ex)
    }
}

/** Refuses U+0000 in every named parameter of every statement the store runs ([Limits.refuseNul]), naming the parameter. */
private class NulRefusingTemplate(template: JdbcTemplate) : NamedParameterJdbcTemplate(template) {
    override fun getPreparedStatementCreator(sql: String, paramSource: SqlParameterSource): PreparedStatementCreator {
        paramSource.parameterNames?.forEach { name -> Limits.refuseNul("the parameter $name", paramSource.getValue(name)) }
        return super.getPreparedStatementCreator(sql, paramSource)
    }
}

/** JSON to and from the text the columns hold. */
internal object Json {
    val mapper: JsonMapper = JsonMapper.builder().build()

    /** [value] as JSON text; refuses U+0000 in its strings (written as an escape, which PostgreSQL's `jsonb` refuses), naming [field]. */
    fun write(field: String, value: Any): String {
        Limits.refuseNul(field, value)
        return mapper.writeValueAsString(value)
    }

    fun strings(text: String?): Map<String, String> =
        text?.let { mapper.readTree(it) }?.properties()?.associate { (k, v) -> k to (if (v.isString) v.asString() else v.toString()) } ?: emptyMap()

    /**
     * Refuses [payload] with an [IllegalArgumentException] naming [what] unless it is a JSON object whose integers fit in a
     * signed 64-bit integer (the outbox's contract).
     */
    fun requireObject(payload: ByteArray, what: String) {
        val node = try {
            mapper.readTree(payload)
        } catch (e: RuntimeException) {
            throw IllegalArgumentException("The payload of $what is not a JSON object: ${e.message}", e)
        }
        require(node != null && node.isObject) { "The payload of $what is not a JSON object" }
        require(fits(node)) { "The payload of $what has an integer outside the signed 64-bit range" }
        require(noNul(node)) { "The message store refuses the character U+0000 in the payload of $what" }
    }

    private fun fits(node: JsonNode): Boolean = if (node.isIntegralNumber) node.canConvertToLong() else node.values().all(::fits)

    private fun noNul(node: JsonNode): Boolean = when {
        node.isString -> node.asString().indexOf('\u0000') < 0
        node.isObject -> node.properties().all { (k, v) -> k.indexOf('\u0000') < 0 && noNul(v) }
        else -> node.values().all(::noNul)
    }
}

/** A position (or segment) as the number it stands for when it is a canonical decimal one, else null: ordered by value. */
internal fun positionNumber(position: String): Long? = position.toLongOrNull()?.takeIf { it.toString() == position }

/** `ORDER BY` of positions as the stores order them: decimal ones by value first, then the others by their bytes. */
internal const val POSITION_ORDER = "CASE WHEN position_num IS NULL THEN 1 ELSE 0 END, position_num, position"

/** The tables of schema version 1, parents before children. */
internal object JdbcTables {
    val ALL = listOf(
        "kraken_outbox", "kraken_processed_events", "kraken_dead_letters", "kraken_processor_control", "kraken_external_calls",
        "kraken_sagas", "kraken_saga_associations", "kraken_saga_dispatch", "kraken_saga_dispatch_leases", "kraken_deadlines",
        "kraken_saga_failures",
    )
}
