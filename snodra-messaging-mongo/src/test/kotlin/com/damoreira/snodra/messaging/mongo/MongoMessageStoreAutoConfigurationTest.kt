package com.damoreira.snodra.messaging.mongo

import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import com.damoreira.snodra.messaging.config.MemoryStoresAutoConfiguration
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.memory.MemoryEventStore
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.memory.MemoryMessageStore
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.transaction.PlatformTransactionManager
import java.time.Duration

/** When the MongoDB store is the application's, and how its settings come from `snodra.messaging.*` (no database needed). */
class MongoMessageStoreAutoConfigurationTest {
    private val factory: MongoDatabaseFactory = mockk(relaxed = true)
    private val mongo: MongoTemplate = mockk(relaxed = true)

    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                SnodraMessagingAutoConfiguration::class.java,
                MongoMessageStoreAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues("snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=test")

    private fun withMongo() = runner
        .withBean(MongoDatabaseFactory::class.java, { factory })
        .withBean(MongoTemplate::class.java, { mongo })

    @Test
    fun noMongoTemplateNoStore() {
        runner.run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBeansOfType(MessageStore::class.java).isEmpty())
            assertTrue(context.getBeansOfType(MongoTransactionManager::class.java).isEmpty())
        }
    }

    @Test
    fun theApplicationsMongoTemplateGetsTheStoreAndItsTransactionManager() {
        withMongo().run { context ->
            assertNull(context.startupFailure)
            val store = context.getBean(MessageStore::class.java) as MongoMessageStore
            assertEquals("mongo", store.name)
            // The store's transactions are the application's @Transactional manager: business writes and outbox commit together.
            assertSame(context.getBean(PlatformTransactionManager::class.java), store.transactions)
            assertTrue(store.transactions is MongoStoreTransactionManager, "transient commit failures are reported as such")
            assertEquals(MongoStoreSettings(), store.settings, "the defaults of snodra.messaging.*.retention")
            verify { mongo.indexOps("outbox") } // init-schema defaults to true
        }
    }

    @Test
    fun anApplicationTransactionManagerIsKept() {
        val mine = MongoTransactionManager(factory)
        withMongo().withBean(MongoTransactionManager::class.java, { mine }).run { context ->
            assertNull(context.startupFailure)
            assertSame(mine, context.getBean(MessageStore::class.java).transactions, "kept, without the transient commit mapping")
        }
    }

    @Test
    fun anApplicationStoreWins() {
        val mine: MessageStore = mockk(relaxed = true)
        withMongo().withBean(MessageStore::class.java, { mine }).run { context ->
            assertNull(context.startupFailure)
            assertSame(mine, context.getBean(MessageStore::class.java))
            assertTrue(context.getBeansOfType(MongoMessageStore::class.java).isEmpty())
        }
    }

    @Test
    fun theStoreSettingMemoryRunsTheInMemoryStoreInsteadOfMongo() {
        withMongo().withConfiguration(AutoConfigurations.of(MemoryStoresAutoConfiguration::class.java))
            .withPropertyValues("snodra.messaging.store.type=memory", "snodra.messaging.store.init-schema=false")
            .run { context ->
                assertNull(context.startupFailure)
                val store = context.getBean(MessageStore::class.java)
                assertTrue(store is MemoryMessageStore, "the in-memory store, never MongoDB behind the setting's back: $store")
                assertTrue(context.getBeansOfType(MongoTransactionManager::class.java).isEmpty(), "no MongoDB transaction manager either")
                assertSame(store.transactions, context.getBean(PlatformTransactionManager::class.java))
            }
        withMongo().withPropertyValues("snodra.messaging.store.type=mongo").run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBean(MessageStore::class.java) is MongoMessageStore, "named by the setting")
        }
    }

    @Test
    fun theMongoStoreRunsWithTheInMemoryEventStore() {
        withMongo().withConfiguration(AutoConfigurations.of(MemoryStoresAutoConfiguration::class.java))
            .withPropertyValues("snodra.messaging.event-store=memory")
            .run { context ->
                assertNull(context.startupFailure)
                assertTrue(context.getBean(MessageStore::class.java) is MongoMessageStore, "the message store stays MongoDB")
                assertTrue(context.getBean(EventStore::class.java) is MemoryEventStore, "the events are in memory")
                assertSame(context.getBean(MessageStore::class.java).transactions, context.getBean(PlatformTransactionManager::class.java))
            }
    }

    @Test
    fun initSchemaFalseSkipsTheIndexes() {
        withMongo().withPropertyValues("snodra.messaging.store.init-schema=false").run { context ->
            assertNull(context.startupFailure)
            context.getBean(MessageStore::class.java)
            verify(exactly = 0) { mongo.indexOps(any<String>()) }
        }
    }

    @Test
    fun retentionsComeFromTheProperties() {
        withMongo().withPropertyValues(
            "snodra.messaging.outbox.retention=1d",
            "snodra.messaging.idempotency.retention=2d",
            "snodra.messaging.external.calls.retention=3d",
            "snodra.messaging.sagas.ended-retention=4d",
            "snodra.messaging.sagas.dispatch-retention=5d",
            "snodra.messaging.deadlines.retention=6d",
            "snodra.messaging.sagas.failed-retention=8d",
        ).run { context ->
            assertNull(context.startupFailure)
            val days = { n: Long -> Duration.ofDays(n) }
            assertEquals(
                MongoStoreSettings(days(1), days(2), days(3), days(4), days(5), days(6), days(8)),
                (context.getBean(MessageStore::class.java) as MongoMessageStore).settings,
            )
        }
    }
}
