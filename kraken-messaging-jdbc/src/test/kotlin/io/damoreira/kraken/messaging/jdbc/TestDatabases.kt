package io.damoreira.kraken.messaging.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.testcontainers.containers.JdbcDatabaseContainer
import java.sql.DriverManager
import java.sql.SQLException
import java.util.UUID

/**
 * The PostgreSQL and MySQL containers of this module's tests, started once per JVM (and reused across runs when Testcontainers
 * reuse is on). Each [create] makes a database of its own on one of them, so tests never see each other's rows; closing a
 * [dataSource] (or [Connection.drop]) drops that database again. The credentials are the containers' fixed test values.
 */
enum class TestDatabase(private val start: () -> JdbcDatabaseContainer<*>) {
    POSTGRES({ KrakenContainers.postgres().also { it.start() } }),
    MYSQL({ KrakenContainers.mysql().also { it.start() } });

    private val container: JdbcDatabaseContainer<*> by lazy(start)

    val dialect: SqlDialect get() = if (this == POSTGRES) PostgresDialect else MySqlDialect

    /** A new, empty database: its JDBC URL, user and password. */
    fun create(): Connection {
        val name = "kraken_" + UUID.randomUUID().toString().replace("-", "").take(20)
        admin { it.createStatement().execute("CREATE DATABASE $name") }
        return Connection(this, name, urlOf(name), adminUser(), container.password)
    }

    /** A pooled DataSource over a new, empty database, which closing it drops. */
    fun dataSource(poolSize: Int = 10, autoCommit: Boolean = true): HikariDataSource = create().dataSource(poolSize, autoCommit, dropOnClose = true)

    internal fun drop(name: String) {
        admin { it.createStatement().execute(if (this == POSTGRES) "DROP DATABASE IF EXISTS $name WITH (FORCE)" else "DROP DATABASE IF EXISTS $name") }
    }

    /** Runs [sql] as the containers' administrator (on database [database] when given). */
    fun admin(sql: String, database: String? = null) {
        val url = database?.let(::urlOf) ?: container.jdbcUrl
        connect(url, adminUser(), container.password).use { it.createStatement().execute(sql) }
    }

    private fun adminUser() = if (this == POSTGRES) container.username else "root"

    private fun admin(block: (java.sql.Connection) -> Unit) = connect(container.jdbcUrl, adminUser(), container.password).use(block)

    /** Connects, retrying for a while: the host port of a container that just started can take a moment to forward. */
    private fun connect(url: String, user: String, password: String): java.sql.Connection {
        val deadline = System.nanoTime() + 60_000_000_000L
        while (true) {
            try {
                return DriverManager.getConnection(url, user, password)
            } catch (e: SQLException) {
                if (System.nanoTime() > deadline) throw e
                Thread.sleep(250)
            }
        }
    }

    private fun urlOf(database: String): String {
        val url = container.jdbcUrl
        val base = url.substringBefore('?')
        val query = url.substringAfter('?', "")
        val swapped = base.substringBeforeLast('/') + "/" + database
        return if (query.isEmpty()) swapped else "$swapped?$query"
    }

    class Connection(private val database: TestDatabase, val name: String, val url: String, val user: String, val password: String) {
        fun dataSource(poolSize: Int = 10, autoCommit: Boolean = true, dropOnClose: Boolean = false, initSql: String? = null): HikariDataSource {
            val config = HikariConfig().also {
                it.connectionInitSql = initSql
                it.jdbcUrl = url
                it.username = user
                it.password = password
                it.maximumPoolSize = poolSize
                it.isAutoCommit = autoCommit
            }
            return if (dropOnClose) DroppingDataSource(config) { drop() } else HikariDataSource(config)
        }

        /** Drops this database. */
        fun drop() = database.drop(name)
    }

    private class DroppingDataSource(config: HikariConfig, private val drop: () -> Unit) : HikariDataSource(config) {
        override fun close() {
            if (isClosed) return
            super.close()
            drop()
        }
    }
}
