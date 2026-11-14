package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.config.KrakenMessagingProperties
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.store.memory.MemoryMessageStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.boot.autoconfigure.EnableAutoConfiguration
import org.springframework.context.annotation.Import
import org.springframework.core.annotation.AliasFor
import org.springframework.test.context.TestPropertySource
import java.lang.annotation.Inherited

/**
 * The zero-infrastructure mode of a service test (ADR 0027, ADR 0028): the application runs on the in-memory event store and
 * the in-memory message store, without a broker, a database or a hub, and the test gets an [InMemoryEvents] bean to publish
 * events to the service and to read what it published and what its processors dead-lettered.
 *
 * ```kotlin
 * @SpringBootTest
 * @KrakenInMemory
 * class AuditInMemoryTest(@Autowired val events: InMemoryEvents, @Autowired val audit: AuditHandler) {
 *     @Test fun `a note is audited`() {
 *         events.publish(AuditNote("k1", "hello", Instant.now()), key = "k1")
 *         events.awaitProcessed(...)
 *     }
 * }
 * ```
 *
 * It sets `kraken.messaging.store.type=memory`, `kraken.messaging.event-store=memory` and `kraken.messaging.hub.enabled=false` (no
 * `CommandBus` or `QueryBus` then: a service that injects one declares [InMemoryBuses] or an [InProcessHub] connection), and
 * leaves out the auto-configurations named in [exclude], by default [KrakenInMemoryExclusions.DEFAULT]: the Kafka add-on (its
 * event store and its partner sources: feed a partner's events through [InMemoryEvents.publish] or test the translator on its
 * own) and Spring Boot's MongoDB client and data auto-configurations (nothing connects to a database). The exclusions are added
 * to the application's own (`spring.autoconfigure.exclude`, `@SpringBootApplication(exclude = ...)`), never in their place.
 *
 * An application that uses MongoDB itself (its own repositories next to the in-memory stores) keeps Boot's MongoDB
 * auto-configurations: `@KrakenInMemory(exclude = [KrakenInMemoryExclusions.KAFKA])`, and its test provides that database or
 * replaces those beans. Other infrastructure the application uses itself is the test's to replace.
 *
 * For a full application test (`@SpringBootTest`): the annotation enables auto-configuration on the test class (that is how its
 * exclusions merge with the application's), so it does not belong on a slice test (`@WebMvcTest` and the like).
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@Inherited
@TestPropertySource(
    properties = [
        "kraken.messaging.store.type=memory",
        "kraken.messaging.event-store=memory",
        "kraken.messaging.hub.enabled=false",
    ],
)
@EnableAutoConfiguration
@Import(KrakenInMemoryConfiguration::class)
annotation class KrakenInMemory(
    /** The auto-configurations (class names) left out, on top of the application's own exclusions. */
    @get:AliasFor(annotation = EnableAutoConfiguration::class, attribute = "excludeName")
    val exclude: Array<String> = [
        KrakenInMemoryExclusions.KAFKA,
        KrakenInMemoryExclusions.MONGO_CLIENT,
        KrakenInMemoryExclusions.MONGO_REACTIVE_CLIENT,
        KrakenInMemoryExclusions.MONGO_DATA,
        KrakenInMemoryExclusions.MONGO_REACTIVE_DATA,
        KrakenInMemoryExclusions.MONGO_REPOSITORIES,
        KrakenInMemoryExclusions.MONGO_REACTIVE_REPOSITORIES,
        KrakenInMemoryExclusions.MONGO_HEALTH,
        KrakenInMemoryExclusions.MONGO_REACTIVE_HEALTH,
        KrakenInMemoryExclusions.MONGO_METRICS,
    ],
)

/** The auto-configurations [KrakenInMemory] leaves out by default ([DEFAULT]), by class name (absent ones are ignored). */
object KrakenInMemoryExclusions {
    /** The Kafka add-on: its event store and its partner sources. */
    const val KAFKA = "io.damoreira.kraken.messaging.kafka.KafkaEventStoreAutoConfiguration"
    const val MONGO_CLIENT = "org.springframework.boot.mongodb.autoconfigure.MongoAutoConfiguration"
    const val MONGO_REACTIVE_CLIENT = "org.springframework.boot.mongodb.autoconfigure.MongoReactiveAutoConfiguration"
    const val MONGO_DATA = "org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration"
    const val MONGO_REACTIVE_DATA = "org.springframework.boot.data.mongodb.autoconfigure.DataMongoReactiveAutoConfiguration"
    const val MONGO_REPOSITORIES = "org.springframework.boot.data.mongodb.autoconfigure.DataMongoRepositoriesAutoConfiguration"
    const val MONGO_REACTIVE_REPOSITORIES = "org.springframework.boot.data.mongodb.autoconfigure.DataMongoReactiveRepositoriesAutoConfiguration"
    const val MONGO_HEALTH = "org.springframework.boot.mongodb.autoconfigure.health.MongoHealthContributorAutoConfiguration"
    const val MONGO_REACTIVE_HEALTH = "org.springframework.boot.mongodb.autoconfigure.health.MongoReactiveHealthContributorAutoConfiguration"
    const val MONGO_METRICS = "org.springframework.boot.mongodb.autoconfigure.metrics.MongoMetricsAutoConfiguration"

    /** [KAFKA] and every MongoDB one: what [KrakenInMemory] leaves out unless told otherwise. */
    val DEFAULT: List<String> = listOf(
        KAFKA, MONGO_CLIENT, MONGO_REACTIVE_CLIENT, MONGO_DATA, MONGO_REACTIVE_DATA, MONGO_REPOSITORIES, MONGO_REACTIVE_REPOSITORIES,
        MONGO_HEALTH, MONGO_REACTIVE_HEALTH, MONGO_METRICS,
    )
}

/** The [InMemoryEvents] of a [KrakenInMemory] test, over the application's in-memory stores. */
@Configuration(proxyBeanMethods = false)
class KrakenInMemoryConfiguration {
    @Bean
    fun inMemoryEvents(
        events: MemoryEventStore,
        store: MemoryMessageStore,
        properties: KrakenMessagingProperties,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
    ) = InMemoryEvents(events, store, properties.serviceName, properties.events.source, codec, typeResolver)
}
