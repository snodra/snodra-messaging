package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.store.OutboxEntry
import kotlinx.coroutines.runBlocking
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.simple.JdbcClient
import java.time.Instant
import javax.sql.DataSource

/**
 * The README's Flyway setup: kraken's schema files run by a Flyway of their own (history table `kraken_schema_history`,
 * baseline 0), next to the application's Flyway and its own V1, in the same database.
 */
abstract class JdbcFlywayTest(private val database: TestDatabase) {
    @Test
    fun aSeparateFlywayHistoryAppliesKrakensFilesBesideTheApplicationsV1() {
        database.dataSource().use { ds ->
            Flyway.configure().dataSource(ds).locations("classpath:db/app").load().migrate() // the application's own V1
            krakenFlyway(ds).migrate()
            krakenFlyway(ds).migrate() // a second start: nothing to do

            val store = JdbcMessageStore(ds, database.dialect)
            store.requireSchema()
            runBlocking { store.outbox.append(OutboxEntry("e-1", "t.Happened", "k", emptyMap(), "{}".toByteArray(), Instant.now(), 1)) }
            val jdbc = JdbcClient.create(ds)
            assertEquals(listOf("1"), jdbc.sql("SELECT version FROM flyway_schema_history WHERE version IS NOT NULL").query(String::class.java).list())
            assertEquals(listOf("0", "1"), jdbc.sql("SELECT version FROM kraken_schema_history WHERE version IS NOT NULL ORDER BY installed_rank").query(String::class.java).list())
        }
    }

    /** What the README tells a service to declare. */
    private fun krakenFlyway(ds: DataSource): Flyway = Flyway.configure()
        .dataSource(ds)
        .locations("classpath:db/kraken/${database.dialect.name}")
        .table("kraken_schema_history")
        .baselineOnMigrate(true)
        .baselineVersion("0")
        .load()
}

class PostgresFlywayTest : JdbcFlywayTest(TestDatabase.POSTGRES)

class MySqlFlywayTest : JdbcFlywayTest(TestDatabase.MYSQL)
