package io.damoreira.kraken.messaging.jdbc

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.jdbc.core.simple.JdbcClient
import java.util.concurrent.Callable
import java.util.concurrent.CyclicBarrier
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import javax.sql.DataSource

/** Schema version 1: applied once, safely when instances start together, never over a database that has it. */
abstract class JdbcSchemaTest(private val database: TestDatabase) {
    @Test
    fun instancesStartingTogetherApplyTheSchemaOnce() {
        database.dataSource(poolSize = 8).use { ds ->
            val start = CyclicBarrier(4)
            val pool = Executors.newFixedThreadPool(4)
            try {
                (1..4).map { pool.submit(Callable { start.await(10, TimeUnit.SECONDS); JdbcMessageStore(ds, database.dialect).initSchema() }) }
                    .forEach { it.get(60, TimeUnit.SECONDS) }
            } finally {
                pool.shutdownNow()
            }
            assertEquals(listOf(1), JdbcClient.create(ds).sql("SELECT version FROM kraken_schema").query(Int::class.javaObjectType).list())
            assertEquals(JdbcTables.ALL.toSet(), tables(ds).filter { it != "kraken_schema" }.toSet())
        }
    }

    @Test
    fun aDatabaseAtVersionOneIsOnlyRead() {
        database.dataSource().use { ds ->
            JdbcMessageStore(ds, database.dialect).initSchema()
            val jdbc = JdbcClient.create(ds)
            jdbc.sql("DROP TABLE kraken_saga_failures").update() // a change the store would undo if it applied the schema again
            JdbcMessageStore(ds, database.dialect).initSchema()
            assertFalse("kraken_saga_failures" in tables(ds), "the schema was not applied again")
        }
    }

    @Test
    fun theSchemaFileRunsAgainOverItself() {
        // What Flyway or a repeated manual run does: the file is idempotent.
        database.dataSource().use { ds ->
            repeat(2) {
                ds.connection.use { c ->
                    org.springframework.jdbc.datasource.init.ScriptUtils.executeSqlScript(c, org.springframework.core.io.ClassPathResource(database.dialect.schemaV1))
                }
            }
            assertEquals(listOf(1), JdbcClient.create(ds).sql("SELECT version FROM kraken_schema").query(Int::class.javaObjectType).list())
        }
    }

    @Test
    fun theWrongDialectFailsNamingTheSupportedDatabases() {
        database.dataSource(poolSize = 2).use { ds ->
            val other = if (database.dialect == PostgresDialect) MySqlDialect else PostgresDialect
            val e = assertThrows(IllegalStateException::class.java) { JdbcMessageStore(ds, other).initSchema() }
            assertTrue(e.message!!.contains("PostgreSQL 13 or later, MySQL 8.0.17 or later"), e.message)
            assertEquals(database.dialect, JdbcDialects.detect(ds, null), "detected from the product name")
            assertEquals(database.dialect, JdbcDialects.detect(ds, database.dialect.name.uppercase()))
            assertThrows(IllegalStateException::class.java) { JdbcDialects.detect(ds, "oracle") }
        }
    }

    /** Version rule: a database at a later version (a newer library applied V2) is accepted as it is. */
    @Test
    fun aDatabaseAtALaterVersionIsAccepted() {
        database.dataSource().use { ds ->
            val store = JdbcMessageStore(ds, database.dialect)
            val e = assertThrows(IllegalStateException::class.java) { store.requireSchema() }
            assertTrue(e.message!!.contains("schema is at version 0, it needs 1 or later"), e.message)
            store.initSchema()
            JdbcClient.create(ds).sql("INSERT INTO kraken_schema (version) VALUES (2)").update()
            store.initSchema()
            store.requireSchema()
        }
    }

    /** Without the right to create tables, init-schema says what to do. */
    @Test
    fun missingRightsToCreateTablesSayWhatToDo() {
        val db = database.create()
        val user = "kraken_ro_" + db.name.takeLast(8)
        try {
            if (database == TestDatabase.POSTGRES) {
                database.admin("CREATE ROLE $user LOGIN PASSWORD 'test'") // PostgreSQL 15+: no CREATE on schema public for it
            } else {
                database.admin("CREATE USER '$user'@'%' IDENTIFIED BY 'test'")
                database.admin("GRANT SELECT ON ${db.name}.* TO '$user'@'%'")
            }
            TestDatabase.Connection(database, db.name, db.url, user, "test").dataSource(poolSize = 2).use { ds ->
                val e = assertThrows(IllegalStateException::class.java) { JdbcMessageStore(ds, database.dialect).initSchema() }
                assertTrue(e.message!!.contains("apply that file yourself and set kraken.messaging.store.init-schema=false, or grant"), e.message)
            }
        } finally {
            db.drop()
            database.admin(if (database == TestDatabase.POSTGRES) "DROP ROLE IF EXISTS $user" else "DROP USER IF EXISTS '$user'@'%'")
        }
    }

    private fun tables(ds: DataSource): List<String> = ds.connection.use { c ->
        val names = mutableListOf<String>()
        c.metaData.getTables(c.catalog, if (database == TestDatabase.POSTGRES) "public" else null, "kraken_%", arrayOf("TABLE")).use { rs ->
            while (rs.next()) names += rs.getString("TABLE_NAME").lowercase()
        }
        names
    }
}

class PostgresSchemaTest : JdbcSchemaTest(TestDatabase.POSTGRES)

class MySqlSchemaTest : JdbcSchemaTest(TestDatabase.MYSQL)
