package io.damoreira.kraken.messaging.mongo

import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.config.MemoryStoresAutoConfiguration
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.memory.MemoryMessageStore
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

/** When the MongoDB store is the application's, and how its settings come from `kraken.messaging.*` (no database needed). */
class MongoMessageStoreAutoConfigurationTest {
    private val factory: MongoDatabaseFactory = mockk(relaxed = true)
    private val mongo: MongoTemplate = mockk(relaxed = true)

    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                MongoMessageStoreAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues("kraken.messaging.service-name=orders", "kraken.messaging.type-prefix=test")

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
            assertEquals(MongoStoreSettings(), store.settings, "the defaults of kraken.messaging.*.retention")
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
            .withPropertyValues("kraken.messaging.store=memory", "kraken.messaging.store.init-schema=false")
            .run { context ->
                assertNull(context.startupFailure)
                val store = context.getBean(MessageStore::class.java)
                assertTrue(store is MemoryMessageStore, "the in-memory store, never MongoDB behind the setting's back: $store")
                assertTrue(context.getBeansOfType(MongoTransactionManager::class.java).isEmpty(), "no MongoDB transaction manager either")
                assertSame(store.transactions, context.getBean(PlatformTransactionManager::class.java))
            }
        withMongo().withPropertyValues("kraken.messaging.store=mongo").run { context ->
            assertNull(context.startupFailure)
            assertTrue(context.getBean(MessageStore::class.java) is MongoMessageStore, "named by the setting")
        }
    }

    @Test
    fun initSchemaFalseSkipsTheIndexes() {
        withMongo().withPropertyValues("kraken.messaging.store.init-schema=false").run { context ->
            assertNull(context.startupFailure)
            context.getBean(MessageStore::class.java)
            verify(exactly = 0) { mongo.indexOps(any<String>()) }
        }
    }

    @Test
    fun retentionsComeFromTheProperties() {
        withMongo().withPropertyValues(
            "kraken.messaging.outbox.retention=1d",
            "kraken.messaging.idempotency.retention=2d",
            "kraken.messaging.external.calls.retention=3d",
            "kraken.messaging.sagas.ended-retention=4d",
            "kraken.messaging.sagas.dispatch-retention=5d",
            "kraken.messaging.deadlines.retention=6d",
            "kraken.messaging.sagas.failed-retention=8d",
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
