package io.damoreira.kraken.messaging.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.testcontainers.containers.JdbcDatabaseContainer
import java.sql.DriverManager
import java.util.UUID

/**
 * The PostgreSQL and MySQL containers of this module's tests, started once per JVM (and reused across runs when Testcontainers
 * reuse is on). Each [create] makes a database of its own on one of them, so tests never see each other's rows. The
 * credentials are the containers' fixed test values.
 */
enum class TestDatabase(private val start: () -> JdbcDatabaseContainer<*>) {
    POSTGRES({ KrakenContainers.postgres().also { it.start() } }),
    MYSQL({ KrakenContainers.mysql().also { it.start() } });

    private val container: JdbcDatabaseContainer<*> by lazy(start)

    val dialect: SqlDialect get() = if (this == POSTGRES) PostgresDialect else MySqlDialect

    /** A new, empty database: its JDBC URL, user and password. */
    fun create(): Connection {
        val name = "kraken_" + UUID.randomUUID().toString().replace("-", "").take(20)
        val (url, user, password) = admin()
        connect(url, user, password).use { it.createStatement().execute("CREATE DATABASE $name") }
        return Connection(urlOf(name), user, password)
    }

    /** A pooled DataSource over a new, empty database. */
    fun dataSource(poolSize: Int = 10): HikariDataSource = create().dataSource(poolSize)

    /** Connects, retrying for a while: the host port of a container that just started can take a moment to forward. */
    private fun connect(url: String, user: String, password: String): java.sql.Connection {
        val deadline = System.nanoTime() + 60_000_000_000L
        while (true) {
            try {
                return DriverManager.getConnection(url, user, password)
            } catch (e: java.sql.SQLException) {
                if (System.nanoTime() > deadline) throw e
                Thread.sleep(250)
            }
        }
    }

    private fun admin(): Connection = when (this) {
        POSTGRES -> Connection(container.jdbcUrl, container.username, container.password)
        MYSQL -> Connection(container.jdbcUrl, "root", container.password)
    }

    private fun urlOf(database: String): String {
        val url = container.jdbcUrl
        val base = url.substringBefore('?')
        val query = url.substringAfter('?', "")
        val swapped = base.substringBeforeLast('/') + "/" + database
        return if (query.isEmpty()) swapped else "$swapped?$query"
    }

    data class Connection(val url: String, val user: String, val password: String) {
        fun dataSource(poolSize: Int = 10): HikariDataSource = HikariDataSource(
            HikariConfig().also {
                it.jdbcUrl = url
                it.username = user
                it.password = password
                it.maximumPoolSize = poolSize
            },
        )
    }
}
