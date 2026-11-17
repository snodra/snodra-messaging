package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.RetryPolicy
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.deadline.DeadlineScheduler
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.hub.HubBus
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.events.OutboxEventBus
import com.damoreira.snodra.messaging.saga.LocalRawCommandSender
import com.damoreira.snodra.messaging.saga.NoopSagaMetrics
import com.damoreira.snodra.messaging.saga.RawEventPublisher
import com.damoreira.snodra.messaging.saga.SagaDispatchRelay
import com.damoreira.snodra.messaging.saga.SagaFailures
import com.damoreira.snodra.messaging.saga.SagaMetrics
import com.damoreira.snodra.messaging.saga.SagaModelScanner
import com.damoreira.snodra.messaging.saga.SagaRetry
import com.damoreira.snodra.messaging.saga.SagaRuntime
import com.damoreira.snodra.messaging.saga.SagaType
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Import
import org.springframework.core.env.Environment
import org.springframework.core.io.ResourceLoader
import org.springframework.transaction.support.TransactionTemplate
import java.time.Clock
import java.time.Duration

/**
 * The saga types of this service, scanned from the application's packages (`AutoConfigurationPackages`) and validated at
 * startup by the [SagaModelScanner]; the runtime, the metrics and the tests read it.
 */
class SagaCatalog(val types: List<SagaType>)

/**
 * Sagas, deadlines and the dispatch relay (S1 plan, Task 13): the [SagaRuntime] (handed to the [LocalEventDispatcher] as its
 * saga stage), the [SagaDispatchRelay] and the [DeadlineScheduler] as lifecycles in phases after the outbox publisher starts
 * and before the event consumer stops, and [SagaFailures], all over the message store's saga ports ([MessageStore.sagas]).
 *
 * Everything here exists only when a class in the application's packages is annotated `@Saga` and
 * `snodra.messaging.sagas.enabled` is not false; a service without sagas gets none of it. A saga that violates the model's
 * rules fails the startup (see the `SagaModelScanner`); without a message store a saga fails the startup naming the missing
 * store ([MessageStoreGuard]). Needs what [EventsAutoConfiguration] provides (the store, the local dispatcher). Commands
 * go through the [HubBus] when the hub is enabled, else to this service's own handlers; events go through the outbox.
 */
@AutoConfiguration(after = [SnodraMessagingAutoConfiguration::class, HubAutoConfiguration::class, EventsAutoConfiguration::class])
@ConditionalOnBean(MessageStore::class, LocalEventDispatcher::class)
@ConditionalOnProperty(prefix = "snodra.messaging.sagas", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@Conditional(OnSagaClasses::class)
@Import(SnodraClockConfiguration::class)
class SagaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun sagaCatalog(typeResolver: MessageTypeResolver, codec: JsonCodec, beanFactory: BeanFactory, environment: Environment, resourceLoader: ResourceLoader) =
        SagaCatalog(SagaModelScanner(typeResolver, codec).scan(ApplicationClassScanning.sagaClasses(beanFactory, environment, resourceLoader).map { it.kotlin }))

    @Bean
    @ConditionalOnMissingBean
    fun sagaRuntime(
        catalog: SagaCatalog,
        store: MessageStore,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        beanFactory: BeanFactory,
        clock: Clock,
        properties: SnodraMessagingProperties,
        metrics: ObjectProvider<SagaMetrics>,
    ) = SagaRuntime(
        types = catalog.types,
        ports = store.sagas,
        tx = TransactionTemplate(store.transactions),
        codec = codec,
        typeResolver = typeResolver,
        beans = { beanFactory.getBean(it.java) }, // handler parameters: any Spring bean by type, resolved per call
        clock = clock,
        retry = SagaRetry(RetryPolicy(properties.sagas.maxAttempts, Duration.ofMillis(200), 2.0, Duration.ofSeconds(5))),
        metrics = metrics.getIfAvailable { NoopSagaMetrics },
    )

    @Bean
    @ConditionalOnMissingBean
    fun sagaFailures(store: MessageStore, runtime: SagaRuntime) = SagaFailures(store.sagas.failures, runtime)

    @Bean
    @ConditionalOnMissingBean
    fun sagaDispatchRelay(
        store: MessageStore,
        @Qualifier("hubBus") hubBus: ObjectProvider<HubBus>,
        invoker: HandlerInvoker,
        events: ObjectProvider<RawEventPublisher>,
        dispatcher: LocalEventDispatcher,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        traceHeaders: TraceHeaders,
        clock: Clock,
        properties: SnodraMessagingProperties,
        metrics: ObjectProvider<SagaMetrics>,
    ) = SagaDispatchRelay(
        store = store.sagas.dispatch,
        tx = TransactionTemplate(store.transactions),
        commands = hubBus.getIfAvailable() ?: LocalRawCommandSender(invoker),
        events = events.getIfAvailable { OutboxEventBus(store.outbox, codec, typeResolver, traceHeaders) },
        local = dispatcher,
        codec = codec,
        typeResolver = typeResolver,
        clock = clock,
        settings = properties.sagas.relay.toSettings(),
        metrics = metrics.getIfAvailable { NoopSagaMetrics },
    )

    @Bean
    @ConditionalOnMissingBean
    fun deadlineScheduler(
        store: MessageStore,
        runtime: SagaRuntime,
        clock: Clock,
        properties: SnodraMessagingProperties,
        metrics: ObjectProvider<SagaMetrics>,
    ) = DeadlineScheduler(store.sagas.deadlines, runtime, clock, properties.deadlines.toSettings(), metrics.getIfAvailable { NoopSagaMetrics })
}
