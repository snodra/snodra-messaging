package io.damoreira.kraken.messaging.jdbc

import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.datasource.init.ScriptUtils
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource

/**
 * The schema of the SQL message store: the server check and the versioned schema files.
 *
 * **The version rule** (a contract of both libraries): schema versions are additive. `V1__kraken.sql` is frozen once released;
 * a later change is `V2__kraken.sql`, `V3__...`, each applied over the one before. Code that knows version n works on a
 * database at version n **or later** (a newer library may have added tables or columns it does not read), and refuses one below
 * n. `kraken_schema` lists the versions applied.
 */
internal object JdbcSchema {
    private val log = LoggerFactory.getLogger(JdbcSchema::class.java)

    /** The schema version this library reads and writes. */
    const val VERSION = 1

    /** The session-level advisory lock (PostgreSQL, one bigint key) and the named lock (MySQL) that serialise [apply]. */
    private const val PG_LOCK = 0x4B524B4E_5343484DL // 'KRKN' 'SCHM'
    private const val MYSQL_LOCK = "kraken_schema"

    /** How long [apply] waits for another instance applying the schema. */
    private const val LOCK_WAIT_SECONDS = 60

    private const val SUPPORTED = "PostgreSQL 13 or later, MySQL 8.0.17 or later"

    /**
     * Applies the schema up to [VERSION] unless the database has it (then it is only read), under a database lock so instances
     * starting together apply it once. Missing rights to create tables fail with a message saying what to do.
     */
    fun apply(dataSource: DataSource, dialect: SqlDialect) {
        dataSource.connection.use { c ->
            verify(c, dialect)
            if (version(c) >= VERSION) return
            locked(c, dialect) {
                if (version(c) >= VERSION) return@locked // another instance applied it while this one waited
                log.info("Applying the kraken message store schema version {} ({})", VERSION, dialect.schemaV1)
                val script = ClassPathResource(dialect.schemaV1, JdbcSchema::class.java.classLoader)
                val autoCommit = c.autoCommit
                try {
                    // PostgreSQL applies it all or nothing; MySQL commits each DDL statement, and the script is idempotent.
                    c.autoCommit = false
                    ScriptUtils.executeSqlScript(c, script)
                    c.commit()
                } catch (e: Exception) {
                    c.rollback()
                    throw IllegalStateException(
                        "Could not apply the kraken message store's schema (${dialect.schemaV1}): apply that file yourself and set " +
                            "kraken.messaging.store.init-schema=false, or grant the service's database user the right to create tables. Cause: $e",
                        e,
                    )
                } finally {
                    c.autoCommit = autoCommit
                }
            }
        }
    }

    /**
     * For `kraken.messaging.store.init-schema=false`: fails with an [IllegalStateException] unless the database has the schema
     * at [VERSION] or later.
     */
    fun require(dataSource: DataSource, dialect: SqlDialect) {
        val found = dataSource.connection.use(::version)
        check(found >= VERSION) {
            "The kraken message store's schema is at version $found, it needs $VERSION or later: apply ${dialect.schemaV1} (and the " +
                "versions after it), or set kraken.messaging.store.init-schema=true"
        }
    }

    /**
     * Fails with an [IllegalStateException] naming what is supported unless [c] is a server [dialect] runs on. [overridden]
     * (the dialect was named by `kraken.messaging.store.jdbc.dialect`): a compatible database of another product name is
     * accepted, and its version is not checked. MySQL also needs a STRICT `sql_mode`, so the database refuses rather than
     * truncates a value.
     */
    fun verify(c: Connection, dialect: SqlDialect, overridden: Boolean = false) {
        val meta = c.metaData
        val product = meta.databaseProductName
        val version = meta.databaseProductVersion
        val sameProduct = SqlDialect.ofProduct(product) == dialect
        if (!overridden) {
            check(!version.contains("MariaDB", ignoreCase = true)) { "The kraken message store does not support MariaDB yet ($product $version): it supports $SUPPORTED" }
            check(sameProduct) { "The kraken message store's dialect is ${dialect.name}, but the database is $product $version: it supports $SUPPORTED" }
        }
        if (sameProduct && !version.contains("MariaDB", ignoreCase = true)) {
            val actual = Triple(meta.databaseMajorVersion, meta.databaseMinorVersion, patch(version))
            val (major, minor, patch) = dialect.minimumVersion
            val ok = compareValuesBy(actual, Triple(major, minor, patch), { it.first }, { it.second }, { it.third }) >= 0
            check(ok) { "The kraken message store needs $product ${dialect.minimumText} or later; this server is $product $version" }
        }
        if (dialect == MySqlDialect) {
            val mode = c.createStatement().use { s -> s.executeQuery("SELECT @@SESSION.sql_mode").use { rs -> if (rs.next()) rs.getString(1) ?: "" else "" } }
            check(mode.contains("STRICT_TRANS_TABLES") || mode.contains("STRICT_ALL_TABLES")) {
                "The kraken message store needs a strict sql_mode on MySQL (STRICT_TRANS_TABLES or STRICT_ALL_TABLES), so a value too long " +
                    "is refused rather than truncated; this session's sql_mode is '$mode'"
            }
        }
    }

    /** The patch number of a version text such as `8.0.36` or `17.7 (Debian 17.7-3)`; 0 when it has none. */
    private fun patch(version: String): Int =
        Regex("^\\d+\\.\\d+\\.(\\d+)").find(version.trim())?.groupValues?.get(1)?.toIntOrNull() ?: 0

    /** The schema version of the database [c] reaches: the highest in `kraken_schema`, 0 when there is none. */
    private fun version(c: Connection): Int = try {
        c.createStatement().use { s ->
            s.executeQuery("SELECT MAX(version) FROM kraken_schema").use { rs -> if (rs.next()) rs.getInt(1) else 0 }
        }
    } catch (_: SQLException) {
        if (!c.autoCommit) c.rollback()
        0 // no kraken_schema table yet
    }

    private fun locked(c: Connection, dialect: SqlDialect, block: () -> Unit) {
        val timedOut = "Timed out after $LOCK_WAIT_SECONDS s waiting for another instance to apply the kraken message store schema"
        when (dialect) {
            PostgresDialect -> {
                // A session lock: it needs a direct connection (not PgBouncer in transaction mode), and waits at most a minute.
                val deadline = System.nanoTime() + LOCK_WAIT_SECONDS * 1_000_000_000L
                while (!c.createStatement().use { s -> s.executeQuery("SELECT pg_try_advisory_lock($PG_LOCK)").use { it.next() && it.getBoolean(1) } }) {
                    check(System.nanoTime() < deadline) { timedOut }
                    Thread.sleep(200)
                }
            }
            MySqlDialect -> c.createStatement().use { s ->
                s.executeQuery("SELECT GET_LOCK('$MYSQL_LOCK', $LOCK_WAIT_SECONDS)").use { rs -> check(rs.next() && rs.getInt(1) == 1) { timedOut } }
            }
        }
        try {
            block()
        } finally {
            when (dialect) {
                PostgresDialect -> c.createStatement().use { it.execute("SELECT pg_advisory_unlock($PG_LOCK)") }
                MySqlDialect -> c.createStatement().use { it.execute("SELECT RELEASE_LOCK('$MYSQL_LOCK')") }
            }
        }
    }
}
