package com.damoreira.snodra.messaging.jdbc

import com.zaxxer.hikari.HikariDataSource
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.test.store.MessageStoreConformanceTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance

/**
 * The message store conformance kit against a real database: one database per class, its schema applied once, every table
 * emptied before each test.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class JdbcMessageStoreConformanceTest(private val database: TestDatabase) : MessageStoreConformanceTest() {
    private lateinit var dataSource: HikariDataSource

    @BeforeAll
    fun start() {
        dataSource = database.dataSource()
        JdbcMessageStore(dataSource, database.dialect).initSchema()
    }

    @AfterAll
    fun stop() {
        dataSource.close()
    }

    override fun store(): MessageStore {
        dataSource.connection.use { c -> c.createStatement().use { s -> JdbcTables.ALL.reversed().forEach { s.execute("DELETE FROM $it") } } }
        return JdbcMessageStore(dataSource, database.dialect)
    }
}

class PostgresMessageStoreConformanceTest : JdbcMessageStoreConformanceTest(TestDatabase.POSTGRES)

class MySqlMessageStoreConformanceTest : JdbcMessageStoreConformanceTest(TestDatabase.MYSQL)
