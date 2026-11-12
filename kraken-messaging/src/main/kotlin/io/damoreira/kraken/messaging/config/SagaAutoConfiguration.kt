package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.deadline.DeadlineScheduler
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.kafka.LocalEventDispatcher
import io.damoreira.kraken.messaging.kafka.OutboxEventBus
import io.damoreira.kraken.messaging.saga.LocalRawCommandSender
import io.damoreira.kraken.messaging.saga.NoopSagaMetrics
import io.damoreira.kraken.messaging.saga.RawEventPublisher
import io.damoreira.kraken.messaging.saga.SagaDispatchRelay
import io.damoreira.kraken.messaging.saga.SagaFailures
import io.damoreira.kraken.messaging.saga.SagaIndexes
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaModelScanner
import io.damoreira.kraken.messaging.saga.SagaRetry
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.SagaType
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import org.springframework.beans.factory.BeanFactory
import org.springframework.beans.factory.ObjectProvider
import org.springframework.beans.factory.annotation.Qualifier
import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Conditional
import org.springframework.context.annotation.Import
import org.springframework.core.env.Environment
import org.springframework.core.io.ResourceLoader
import org.springframework.data.mongodb.core.MongoTemplate
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
 * and before the event consumer stops, [SagaFailures] and the saga indexes.
 *
 * Everything here exists only when a class in the application's packages is annotated `@Saga` and
 * `kraken.messaging.sagas.enabled` is not false; a service without sagas gets none of it. A saga that violates the model's
 * rules fails the startup (see the `SagaModelScanner`). Needs what [KafkaEventsAutoConfiguration] provides (the Mongo
 * transaction template, the local dispatcher). Commands go through the [HubBus] when the hub is enabled, else to this
 * service's own handlers; events go through the outbox.
 */
@AutoConfiguration(
    after = [KrakenMessagingAutoConfiguration::class, HubAutoConfiguration::class, KafkaEventsAutoConfiguration::class],
    afterName = ["org.springframework.boot.data.mongodb.autoconfigure.DataMongoAutoConfiguration"],
)
@ConditionalOnClass(MongoTemplate::class)
@ConditionalOnBean(LocalEventDispatcher::class)
@ConditionalOnProperty(prefix = "kraken.messaging.sagas", name = ["enabled"], havingValue = "true", matchIfMissing = true)
@Conditional(OnSagaClasses::class)
@Import(KrakenClockConfiguration::class)
class SagaAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    fun sagaCatalog(typeResolver: MessageTypeResolver, codec: JsonCodec, beanFactory: BeanFactory, environment: Environment, resourceLoader: ResourceLoader) =
        SagaCatalog(SagaModelScanner(typeResolver, codec).scan(ApplicationClassScanning.sagaClasses(beanFactory, environment, resourceLoader).map { it.kotlin }))

    @Bean
    @ConditionalOnMissingBean
    fun sagaIndexes(mongo: MongoTemplate, properties: KrakenMessagingProperties) =
        SagaIndexes(mongo, properties.sagas.endedRetention, properties.sagas.dispatchRetention, properties.deadlines.retention, properties.sagas.failedRetention)

    @Bean
    @ConditionalOnMissingBean
    fun sagaRuntime(
        catalog: SagaCatalog,
        mongo: MongoTemplate,
        tx: TransactionTemplate,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        beanFactory: BeanFactory,
        clock: Clock,
        properties: KrakenMessagingProperties,
        metrics: ObjectProvider<SagaMetrics>,
    ) = SagaRuntime(
        types = catalog.types,
        mongo = mongo,
        tx = tx,
        codec = codec,
        typeResolver = typeResolver,
        beans = { beanFactory.getBean(it.java) }, // handler parameters: any Spring bean by type, resolved per call
        clock = clock,
        retry = SagaRetry(RetryPolicy(properties.sagas.maxAttempts, Duration.ofMillis(200), 2.0, Duration.ofSeconds(5))),
        metrics = metrics.getIfAvailable { NoopSagaMetrics },
    )

    @Bean
    @ConditionalOnMissingBean
    fun sagaFailures(mongo: MongoTemplate, runtime: SagaRuntime) = SagaFailures(mongo, runtime)

    @Bean
    @ConditionalOnMissingBean
    fun sagaDispatchRelay(
        mongo: MongoTemplate,
        tx: TransactionTemplate,
        @Qualifier("hubBus") hubBus: ObjectProvider<HubBus>,
        invoker: HandlerInvoker,
        events: ObjectProvider<RawEventPublisher>,
        dispatcher: LocalEventDispatcher,
        codec: JsonCodec,
        typeResolver: MessageTypeResolver,
        traceHeaders: TraceHeaders,
        clock: Clock,
        properties: KrakenMessagingProperties,
        metrics: ObjectProvider<SagaMetrics>,
    ) = SagaDispatchRelay(
        mongo = mongo,
        tx = tx,
        commands = hubBus.getIfAvailable() ?: LocalRawCommandSender(invoker),
        events = events.getIfAvailable { OutboxEventBus(mongo, codec, typeResolver, traceHeaders) },
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
        mongo: MongoTemplate,
        runtime: SagaRuntime,
        clock: Clock,
        properties: KrakenMessagingProperties,
        metrics: ObjectProvider<SagaMetrics>,
    ) = DeadlineScheduler(mongo, runtime, clock, properties.deadlines.toSettings(), metrics.getIfAvailable { NoopSagaMetrics })
}
