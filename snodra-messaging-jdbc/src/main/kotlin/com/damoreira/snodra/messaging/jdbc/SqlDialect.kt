package com.damoreira.snodra.messaging.jdbc

import org.springframework.dao.CannotAcquireLockException
import org.springframework.dao.DataAccessException
import org.springframework.dao.DuplicateKeyException
import org.springframework.dao.PessimisticLockingFailureException
import java.sql.ResultSet
import java.sql.SQLException
import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit

/**
 * What differs between the SQL databases the JDBC message store runs on: the schema file, how an instant and a JSON value
 * are bound and read, the statement that inserts a row unless its key exists, how a database error maps to Spring's
 * exceptions, how the start of a saga is serialised per start key, and whether appends notify the relays. Every statement
 * the store runs is a fixed string with bind parameters: nothing in it comes from input.
 */
sealed interface SqlDialect {
    /** `postgresql` or `mysql` (`snodra.messaging.store.jdbc.dialect`). */
    val name: String

    /** The oldest server version this dialect supports: major, minor, patch. */
    val minimumVersion: Triple<Int, Int, Int>

    /** [minimumVersion] as people write it. */
    val minimumText: String

    /** The classpath location of schema version 1 (`db/snodra/<name>/V1__snodra.sql`). */
    val schemaV1: String get() = "db/snodra/$name/V1__snodra.sql"

    /** [instant] as this database's timestamp parameter: UTC, truncated to the millisecond (what both databases keep). */
    fun timestamp(instant: Instant): Any

    /** The timestamp in [column] of [rs], or null. */
    fun instant(rs: ResultSet, column: String): Instant?

    /** A JSON parameter in a statement (`:name` is the named parameter, bound as the JSON text). */
    fun json(parameter: String): String

    /**
     * `INSERT INTO table (columns) VALUES (values)` that inserts nothing, without an error, when a row with that primary key
     * exists. Its update count says whether it inserted (PostgreSQL); MySQL counts a row found as 1 (`CLIENT_FOUND_ROWS`, the
     * driver's default), so the store uses it only where that count is not read.
     */
    fun insertUnlessExists(table: String, columns: String, values: String, key: String): String

    /** Whether a duplicate key aborts the transaction it happens in (PostgreSQL), so the store must not let one happen there. */
    val duplicateAbortsTransaction: Boolean

    /** [e] as the Spring exception the store reports, or null for the default translation. */
    fun translate(e: SQLException): DataAccessException?

    /** Whether appends can notify listening relays (PostgreSQL `NOTIFY snodra_outbox`). */
    val notifies: Boolean get() = false

    companion object {
        /** Every dialect (a getter: the objects refer to this companion while they initialise). */
        val all: List<SqlDialect> get() = listOf(PostgresDialect, MySqlDialect)

        /** The dialect named [name] (`postgresql`, `mysql`), or null. */
        fun named(name: String): SqlDialect? = all.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }

        /** The dialect of a database whose JDBC product name is [product] (`PostgreSQL`, `MySQL`), or null. */
        fun ofProduct(product: String): SqlDialect? = when {
            product.equals("PostgreSQL", ignoreCase = true) -> PostgresDialect
            product.equals("MySQL", ignoreCase = true) -> MySqlDialect
            else -> null
        }

        internal fun millis(instant: Instant): Instant = instant.truncatedTo(ChronoUnit.MILLIS)
    }
}

/**
 * PostgreSQL 13 or later: `timestamptz(3)`, `jsonb`, `INSERT ... ON CONFLICT DO NOTHING`, errors by SQLSTATE, and a
 * `NOTIFY snodra_outbox` in every append's transaction (delivered when it commits) that wakes the relays listening.
 */
data object PostgresDialect : SqlDialect {
    override val name = "postgresql"
    override val minimumVersion = Triple(13, 0, 0)
    override val minimumText = "13.0"
    override val duplicateAbortsTransaction = true
    override val notifies = true

    /** The channel appends notify and relays listen on; the payload is empty. */
    const val CHANNEL = "snodra_outbox"

    /** The first key of the transaction-scoped advisory lock that serialises the start of sagas per start key (0x534E4452, 1397638226). */
    const val SAGA_START_LOCK = 0x534E4452

    override fun timestamp(instant: Instant): Any = OffsetDateTime.ofInstant(SqlDialect.millis(instant), ZoneOffset.UTC)

    override fun instant(rs: ResultSet, column: String): Instant? = rs.getObject(column, OffsetDateTime::class.java)?.toInstant()

    override fun json(parameter: String) = "CAST($parameter AS jsonb)"

    override fun insertUnlessExists(table: String, columns: String, values: String, key: String) =
        "INSERT INTO $table ($columns) VALUES ($values) ON CONFLICT ($key) DO NOTHING"

    override fun translate(e: SQLException): DataAccessException? = when (e.sqlState) {
        "23505" -> DuplicateKeyException(e.message ?: "duplicate key", e)
        "40001", "40P01" -> PessimisticLockingFailureException(e.message ?: "serialization failure or deadlock", e)
        "55P03" -> CannotAcquireLockException(e.message ?: "lock not available", e)
        else -> null
    }
}

/**
 * MySQL 8.0.17 or later (InnoDB, `utf8mb4_0900_bin`, a STRICT `sql_mode`): `DATETIME(3)` holding UTC, `JSON`, `INSERT ... ON DUPLICATE KEY UPDATE`, errors by vendor code.
 * No notification: the relays poll.
 */
data object MySqlDialect : SqlDialect {
    override val name = "mysql"
    override val minimumVersion = Triple(8, 0, 17)
    override val minimumText = "8.0.17"
    override val duplicateAbortsTransaction = false

    override fun timestamp(instant: Instant): Any = LocalDateTime.ofInstant(SqlDialect.millis(instant), ZoneOffset.UTC)

    override fun instant(rs: ResultSet, column: String): Instant? = rs.getObject(column, LocalDateTime::class.java)?.toInstant(ZoneOffset.UTC)

    override fun json(parameter: String) = "CAST($parameter AS JSON)"

    override fun insertUnlessExists(table: String, columns: String, values: String, key: String) =
        "INSERT INTO $table ($columns) VALUES ($values) ON DUPLICATE KEY UPDATE $key = $key"

    override fun translate(e: SQLException): DataAccessException? = when (e.errorCode) {
        1062, 1586 -> DuplicateKeyException(e.message ?: "duplicate key", e)
        1213 -> PessimisticLockingFailureException(e.message ?: "deadlock", e)
        1205, 3572 -> CannotAcquireLockException(e.message ?: "lock not available", e)
        else -> null
    }
}
