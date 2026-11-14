package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.control.ProcessorControl
import io.damoreira.kraken.messaging.deadletter.DeadLetterFactory
import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.deadletter.DeadLetters
import io.damoreira.kraken.messaging.events.DeadLetterSink
import io.damoreira.kraken.messaging.events.EventConsumer
import io.damoreira.kraken.messaging.events.EventProcessors
import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.damoreira.kraken.messaging.events.OutboxEventBus
import io.damoreira.kraken.messaging.events.OutboxPublisher
import io.damoreira.kraken.messaging.eventstore.EventStore
import io.damoreira.kraken.messaging.eventstore.SubscriptionSpec
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.Processors
import io.damoreira.kraken.messaging.metrics.MessagingMetrics
import io.damoreira.kraken.messaging.metrics.NoopMessagingMetrics
import io.damoreira.kraken.messaging.saga.SagaStage
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

/**
 * Events with the transactional outbox (ADR 0027, ADR 0028), over the application's [EventStore] (an event-store add-on's,
 * for example `kraken-messaging-kafka`, or its own bean) and [MessageStore]: [OutboxEventBus] as the [EventBus],
 * [OutboxPublisher] relaying the outbox to the event store, [EventProcessors] running the `@EventHandler`s (one [EventConsumer]
 * per processor, ADR 0022), the dead letters ([DeadLetters]) and the processor control ([ProcessorControl], P6b).
 *
 * Needs a [MessageStore] (ADR 0028): a store add-on's (`kraken-messaging-mongo` provides one over the application's
 * MongoDB, ordered before this configuration) or the application's own bean. The outbox entries and the markers join the
 * store's transactions, which the add-on also gives the application's `@Transactional` methods, so business writes and outbox
 * entries commit together. The local dispatcher and the dead letters exist with a store alone (the webhook ingress uses them);
 * the event bus, the relay, the processors and their control also need one [EventStore]. Without one, an event handler or a
 * saga fails the startup naming the missing add-on ([EventStoreGuard]), and so does a bean that needs an [EventBus]
 * ([MissingMessageStoreFailureAnalyzer]).
 *
 * The relay, the dispatcher and the consumers report to the application's [MessagingMetrics] bean (the Micrometer one of
 * [MessagingMetricsAutoConfiguration] when a registry exists), else to [NoopMessagingMetrics].
 */
@AutoConfiguration(after = [KrakenMessagingAutoConfiguration::class])
@ConditionalOnBean(MessageStore::class)
class EventsAutoConfiguration {

    /** Runs an event's ordinary handlers, then the saga stage when the saga runtime provides one (none: [SagaStage.NONE]). */
    @Bean
    @ConditionalOnMissingBean
    fun localEventDispatcher(
        registry: HandlerRegistry,
        invoker: HandlerInvoker,
        store: MessageStore,
        sagaStage: ObjectProvider<SagaStage>,
        metrics: ObjectProvider<MessagingMetrics>,
    ) = LocalEventDispatcher(registry, invoker, store, sagaStage.getIfAvailable { SagaStage.NONE }, metrics.getIfAvailable { NoopMessagingMetrics })

    /** The dead letters' admin API over the store's dead letters; in modes `event-store` and `log` every call fails naming the mode. */
    @Bean
    @ConditionalOnMissingBean
    fun deadLetters(properties: KrakenMessagingProperties, store: MessageStore, dispatcher: LocalEventDispatcher): DeadLetters {
        val mode = properties.events.deadLetter.mode
        return DeadLetters(mode, store.deadLetters.takeIf { mode == DeadLetterMode.COLLECTION }, dispatcher)
    }

    /** What needs the event store too: the event bus, the relay, the processors and their control. */
    @Configuration(proxyBeanMethods = false)
    @ConditionalOnSingleCandidate(EventStore::class)
    class OnEventStore {
        @Bean
        @ConditionalOnMissingBean(EventBus::class)
        fun outboxEventBus(store: MessageStore, codec: JsonCodec, typeResolver: MessageTypeResolver, traceHeaders: TraceHeaders): OutboxEventBus =
            OutboxEventBus(store.outbox, codec, typeResolver, traceHeaders)

        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(prefix = "kraken.messaging.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
        fun outboxPublisher(
            store: MessageStore,
            events: EventStore,
            properties: KrakenMessagingProperties,
            metrics: ObjectProvider<MessagingMetrics>,
        ) = OutboxPublisher(
            store.outbox, events,
            source = properties.events.source,
            service = properties.serviceName,
            pollInterval = properties.outbox.pollInterval,
            batchSize = properties.outbox.batchSize,
            maxAttempts = properties.outbox.maxAttempts,
            metrics = metrics.getIfAvailable { NoopMessagingMetrics },
            lease = properties.outbox.lease,
        )

        /**
         * The control of the event processors (P6b, ADR 0023): what their `processor_control` documents say, applied at start and
         * every 5 s, and the hub's control requests (pause, resume, reset through the event store, dead letters).
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(prefix = "kraken.messaging.events.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
        fun processorControl(store: MessageStore, registry: HandlerRegistry, deadLetters: DeadLetters, events: EventStore) =
            ProcessorControl(store.processorControl, registry, deadLetters, events)

        /**
         * The event processors (ADR 0022): one consumer per processor that runs here, each in the subscription
         * `<prefix>.<name>` (`events.subscription-prefix`, else the service name; `default` included) of `events.source`.
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(prefix = "kraken.messaging.events.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
        fun eventProcessors(
            events: EventStore,
            codec: JsonCodec,
            dispatcher: LocalEventDispatcher,
            properties: KrakenMessagingProperties,
            store: MessageStore,
            clock: ObjectProvider<Clock>,
            control: ObjectProvider<ProcessorControl>,
            metrics: ObjectProvider<MessagingMetrics>,
        ): EventProcessors {
            val settings = properties.events
            val sink = when (settings.deadLetter.mode) {
                DeadLetterMode.COLLECTION ->
                    DeadLetterSink.Collection(store.deadLetters, DeadLetterFactory(settings.deadLetter.retention, clock.getIfAvailable { Clock.systemUTC() }))
                DeadLetterMode.EVENT_STORE -> DeadLetterSink.EventStore(events)
                DeadLetterMode.LOG -> DeadLetterSink.Log
            }
            val prefix = settings.subscriptionPrefix(properties.serviceName)
            val messagingMetrics = metrics.getIfAvailable { NoopMessagingMetrics }
            return EventProcessors(
                dispatcher,
                consumerFor = { processor ->
                    EventConsumer(
                        events,
                        SubscriptionSpec(Processors.subscriptionName(prefix, processor), settings.source),
                        dispatcher = dispatcher,
                        maxAttempts = settings.consumer.maxAttempts,
                        codec = codec,
                        processor = processor,
                        deadLetters = sink,
                        metrics = messagingMetrics,
                    )
                },
                control = control.getIfAvailable(),
            )
        }
    }
}
