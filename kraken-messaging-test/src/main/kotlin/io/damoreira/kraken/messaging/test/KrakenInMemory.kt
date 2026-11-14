package io.damoreira.kraken.messaging.test

import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.config.KrakenMessagingProperties
import io.damoreira.kraken.messaging.eventstore.memory.MemoryEventStore
import io.damoreira.kraken.messaging.store.memory.MemoryMessageStore
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Import
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
 * It sets `kraken.messaging.store=memory`, `kraken.messaging.event-store=memory` and `kraken.messaging.hub.enabled=false` (no
 * `CommandBus` or `QueryBus` then: a service that injects one declares [InMemoryBuses] or an [InProcessHub] connection), and
 * excludes the Kafka add-on's auto-configuration (its event store and its partner sources: feed a partner's events through
 * [InMemoryEvents.publish] or test the translator on its own) through `spring.autoconfigure.exclude`, which replaces the
 * application's own list for the test (a test that needs both lists them in its properties). Other infrastructure the application uses itself (its own
 * MongoDB repositories, for example) is the test's to replace.
 */
@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@MustBeDocumented
@Inherited
@TestPropertySource(
    properties = [
        "kraken.messaging.store=memory",
        "kraken.messaging.event-store=memory",
        "kraken.messaging.hub.enabled=false",
        "spring.autoconfigure.exclude=io.damoreira.kraken.messaging.kafka.KafkaEventStoreAutoConfiguration",
    ],
)
@Import(KrakenInMemoryConfiguration::class)
annotation class KrakenInMemory

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
