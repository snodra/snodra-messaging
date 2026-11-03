package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.saga.stateDoc
import com.mongodb.client.MongoClients
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.config.e2escan.E2eCounter
import io.damoreira.kraken.messaging.config.e2escan.E2ePrintAdapter
import io.damoreira.kraken.messaging.config.e2escan.E2eScanConfig
import io.damoreira.kraken.messaging.config.e2escan.E2eStarted
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.test.KrakenContainers
import org.awaitility.Awaitility.await
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.core.MongoTemplate
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory
import org.springframework.data.mongodb.core.query.Criteria
import org.springframework.data.mongodb.core.query.Query
import java.time.Duration
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * The saga toolkit wired only by the auto-configurations over a real MongoDB: an event reaches a saga through the local
 * dispatcher, the saga's command is relayed to an `@IdempotentHandler` adapter (through the interceptor the invoker got), its
 * deadline fires, and a bean the handler asks for by type is the application's.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SagaAutoConfigurationMongoTest {
    private val mongoContainer = KrakenContainers.mongo()
    private lateinit var factory: SimpleMongoClientDatabaseFactory
    private lateinit var mongo: MongoTemplate

    @BeforeAll
    fun start() {
        mongoContainer.start()
        factory = SimpleMongoClientDatabaseFactory(MongoClients.create(mongoContainer.connectionString + "/?directConnection=true"), "saga_autoconfig_test")
        mongo = MongoTemplate(factory)
    }

    @AfterAll
    fun stop() = mongoContainer.stop()

    @Test
    fun `an event reaches a saga, its command an idempotent adapter and its deadline fires, wired by auto-configuration alone`() {
        val counter = E2eCounter()
        ApplicationContextRunner()
            .withConfiguration(
                AutoConfigurations.of(
                    KrakenMessagingAutoConfiguration::class.java, HubAutoConfiguration::class.java, KafkaEventsAutoConfiguration::class.java,
                    SagaAutoConfiguration::class.java, ExternalAutoConfiguration::class.java, SagaMetricsAutoConfiguration::class.java,
                    TransactionAutoConfiguration::class.java,
                ),
            )
            .withUserConfiguration(E2eScanConfig::class.java)
            .withBean(MongoTemplate::class.java, { mongo })
            .withBean(MongoDatabaseFactory::class.java, { factory })
            .withBean(E2eCounter::class.java, { counter })
            .withBean(E2ePrintAdapter::class.java, { E2ePrintAdapter(counter) })
            .withPropertyValues(
                "kraken.messaging.service-name=e2e",
                "kraken.messaging.type-prefix=e2e",
                "kraken.messaging.hub.enabled=false",
                "kraken.messaging.kafka.bootstrap-servers=localhost:9",
                "kraken.messaging.kafka.consumer.enabled=false",
                "kraken.messaging.outbox.enabled=false",
                "kraken.messaging.sagas.relay.poll-interval=50ms",
                "kraken.messaging.deadlines.poll-interval=50ms",
            )
            .run { context ->
                assertEquals(null, context.startupFailure)
                val dispatcher = context.getBean(LocalEventDispatcher::class.java)
                val ctx = MessageContext("evt-1", "e2e.Started", "corr-1", null, emptySet(), null, mapOf("id" to "evt-1"))

                dispatcher.dispatch("evt-1", "e2e.Started", """{"jobId":"j1"}""".toByteArray(), ctx)

                await().atMost(Duration.ofSeconds(20)).untilAsserted {
                    val saga = mongo.findAll(SagaDocument::class.java).single()
                    assertEquals(true, saga.stateDoc.getBoolean("timedOut"), "the deadline fired")
                    assertEquals(1, counter.prints.get(), "the adapter's body ran once")
                    val entry = mongo.findAll(SagaDispatchDocument::class.java).single()
                    assertNotNull(entry.dispatchedAt, "the relay marked the command dispatched")
                    val call = mongo.findById(entry.id, io.damoreira.kraken.messaging.external.ExternalCallDocument::class.java)
                    assertEquals(io.damoreira.kraken.messaging.external.ExternalCallStatus.SUCCEEDED, call?.status, "recorded under sagaId:seq")
                }
                assertTrue(mongo.exists(Query(Criteria.where("_id").`is`("saga:e2e.Job:evt-1")), "processed_events"))
            }
    }
}
