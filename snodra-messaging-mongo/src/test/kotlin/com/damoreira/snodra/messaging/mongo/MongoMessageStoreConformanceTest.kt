package com.damoreira.snodra.messaging.mongo

import com.mongodb.client.MongoClient
import com.mongodb.client.MongoClients
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.test.SnodraContainers
import com.damoreira.snodra.messaging.test.store.MessageStoreConformanceTest
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.TestInstance
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import java.util.UUID

/** The message store conformance kit against a real MongoDB replica set: every test gets a database of its own. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MongoMessageStoreConformanceTest : MessageStoreConformanceTest() {
    private val container = SnodraContainers.mongo()
    private lateinit var client: MongoClient

    @BeforeAll
    fun start() {
        container.start()
        client = MongoClients.create(container.connectionString + "/?directConnection=true")
    }

    @AfterAll
    fun stop() {
        client.close()
        container.stop()
    }

    override fun store(): MessageStore {
        val template = MongoTemplate(SimpleMongoClientDatabaseFactory(client, "store_" + UUID.randomUUID().toString().replace("-", "")))
        return MongoMessageStore(template).also { it.ensureIndexes() }
    }
}
