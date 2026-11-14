package io.damoreira.kraken.messaging.jdbc

import org.slf4j.LoggerFactory
import org.springframework.core.io.ClassPathResource
import org.springframework.jdbc.datasource.init.ScriptUtils
import java.sql.Connection
import java.sql.SQLException
import javax.sql.DataSource

/** The schema of the SQL message store: the server check and version 1, applied once. */
internal object JdbcSchema {
    private val log = LoggerFactory.getLogger(JdbcSchema::class.java)

    /** The schema version this library reads and writes. */
    const val VERSION = 1

    /** The session-level advisory lock (PostgreSQL, one bigint key) and the named lock (MySQL) that serialise [apply]. */
    private const val PG_LOCK = 0x4B524B4E_5343484DL // 'KRKN' 'SCHM'
    private const val MYSQL_LOCK = "kraken_schema"
    private const val MYSQL_LOCK_WAIT_SECONDS = 60

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
                    throw e
                } finally {
                    c.autoCommit = autoCommit
                }
            }
        }
    }

    /**
     * Fails with an [IllegalStateException] naming what is supported unless [c] is a server of [dialect]'s database at its
     * minimum version or later.
     */
    fun verify(c: Connection, dialect: SqlDialect) {
        val meta = c.metaData
        val product = meta.databaseProductName
        val version = meta.databaseProductVersion
        val supported = "PostgreSQL ${PostgresDialect.minimumVersion.first} or later, MySQL ${MySqlDialect.minimumVersion.first}.${MySqlDialect.minimumVersion.second} or later"
        check(!version.contains("MariaDB", ignoreCase = true)) { "The kraken message store does not support MariaDB yet ($product $version): it supports $supported" }
        val actual = SqlDialect.ofProduct(product)
        check(actual == dialect) {
            "The kraken message store's dialect is ${dialect.name}, but the database is $product $version: it supports $supported"
        }
        val (major, minor) = dialect.minimumVersion
        val ok = meta.databaseMajorVersion > major || (meta.databaseMajorVersion == major && meta.databaseMinorVersion >= minor)
        check(ok) { "The kraken message store needs $product $major.$minor or later; this server is $product $version" }
    }

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
        when (dialect) {
            PostgresDialect -> c.createStatement().use { it.execute("SELECT pg_advisory_lock($PG_LOCK)") }
            MySqlDialect -> c.createStatement().use { s ->
                s.executeQuery("SELECT GET_LOCK('$MYSQL_LOCK', $MYSQL_LOCK_WAIT_SECONDS)").use { rs ->
                    check(rs.next() && rs.getInt(1) == 1) { "Timed out waiting for another instance to apply the kraken message store schema" }
                }
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
