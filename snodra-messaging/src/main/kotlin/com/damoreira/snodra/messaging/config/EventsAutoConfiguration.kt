package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.EventBus
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.control.ProcessorControl
import com.damoreira.snodra.messaging.deadletter.DeadLetterFactory
import com.damoreira.snodra.messaging.deadletter.DeadLetterMode
import com.damoreira.snodra.messaging.deadletter.DeadLetters
import com.damoreira.snodra.messaging.events.DeadLetterSink
import com.damoreira.snodra.messaging.events.EventConsumer
import com.damoreira.snodra.messaging.events.EventProcessors
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.events.OutboxEventBus
import com.damoreira.snodra.messaging.events.OutboxPublisher
import com.damoreira.snodra.messaging.eventstore.EventStore
import com.damoreira.snodra.messaging.eventstore.SubscriptionSpec
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.Processors
import com.damoreira.snodra.messaging.metrics.MessagingMetrics
import com.damoreira.snodra.messaging.metrics.NoopMessagingMetrics
import com.damoreira.snodra.messaging.saga.SagaStage
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import java.time.Clock

/**
 * Events with the transactional outbox, over the application's [EventStore] (an event-store add-on's,
 * for example `snodra-messaging-kafka`, or its own bean) and [MessageStore]: [OutboxEventBus] as the [EventBus],
 * [OutboxPublisher] relaying the outbox to the event store, [EventProcessors] running the `@EventHandler`s (one [EventConsumer]
 * per processor), the dead letters ([DeadLetters]) and the processor control ([ProcessorControl]).
 *
 * Needs a [MessageStore]: a store add-on's (`snodra-messaging-mongo` provides one over the application's
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
@AutoConfiguration(after = [SnodraMessagingAutoConfiguration::class])
@ConditionalOnSingleCandidate(MessageStore::class) // two: the MessageStoreGuard asks for snodra.messaging.store.type
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
    fun deadLetters(properties: SnodraMessagingProperties, store: MessageStore, dispatcher: LocalEventDispatcher): DeadLetters {
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
        @ConditionalOnProperty(prefix = "snodra.messaging.outbox", name = ["enabled"], havingValue = "true", matchIfMissing = true)
        fun outboxPublisher(
            store: MessageStore,
            events: EventStore,
            properties: SnodraMessagingProperties,
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
         * The control of the event processors: what their `processor_control` documents say, applied at start and
         * every 5 s, and the hub's control requests (pause, resume, reset through the event store, dead letters).
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(prefix = "snodra.messaging.events.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
        fun processorControl(store: MessageStore, registry: HandlerRegistry, deadLetters: DeadLetters, events: EventStore) =
            ProcessorControl(store.processorControl, registry, deadLetters, events)

        /**
         * The event processors: one consumer per processor that runs here, each in the subscription
         * `<prefix>.<name>` (`events.subscription-prefix`, else the service name; `default` included) of `events.source`.
         */
        @Bean
        @ConditionalOnMissingBean
        @ConditionalOnProperty(prefix = "snodra.messaging.events.consumer", name = ["enabled"], havingValue = "true", matchIfMissing = true)
        fun eventProcessors(
            events: EventStore,
            codec: JsonCodec,
            dispatcher: LocalEventDispatcher,
            properties: SnodraMessagingProperties,
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
