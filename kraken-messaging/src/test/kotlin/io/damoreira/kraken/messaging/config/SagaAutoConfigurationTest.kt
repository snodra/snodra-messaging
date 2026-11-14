package io.damoreira.kraken.messaging.config

import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.config.emptyscan.EmptyScanConfig
import io.damoreira.kraken.messaging.config.idemscan.IdemBeanConfig
import io.damoreira.kraken.messaging.config.idemscan.IdemScanConfig
import io.damoreira.kraken.messaging.config.sagascan.SagaScanConfig
import io.damoreira.kraken.messaging.deadline.DeadlineScheduler
import io.damoreira.kraken.messaging.external.ExternalEventIngress
import io.damoreira.kraken.messaging.external.ExternalSources
import io.damoreira.kraken.messaging.kafka.KafkaEventStoreAutoConfiguration
import io.damoreira.kraken.messaging.kafka.KafkaExternalSource
import io.damoreira.kraken.messaging.external.ExternalRecordTranslator
import io.damoreira.kraken.messaging.external.IdempotencyInterceptor
import io.damoreira.kraken.messaging.external.WebhookSignatureVerifier
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.hub.HubBus
import io.damoreira.kraken.messaging.events.EventConsumer
import io.damoreira.kraken.messaging.events.LocalEventDispatcher
import io.damoreira.kraken.messaging.saga.LocalRawCommandSender
import io.damoreira.kraken.messaging.saga.MicrometerSagaMetrics
import io.damoreira.kraken.messaging.saga.NoopSagaMetrics
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaDispatchRelay
import io.damoreira.kraken.messaging.saga.SagaFailures
import io.damoreira.kraken.messaging.saga.SagaGaugeSampler
import io.damoreira.kraken.messaging.saga.SagaMetrics
import io.damoreira.kraken.messaging.saga.SagaRuntime
import io.damoreira.kraken.messaging.saga.SagaStage
import io.damoreira.kraken.messaging.test.InProcessHub
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.context.SmartLifecycle
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue

class SagaAutoConfigurationTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                KrakenMessagingAutoConfiguration::class.java,
                HubAutoConfiguration::class.java,
                EventsAutoConfiguration::class.java,
                SagaAutoConfiguration::class.java,
                ExternalAutoConfiguration::class.java,
                SagaMetricsAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java,
                KafkaEventStoreAutoConfiguration::class.java,
            ),
        )
        .withUserConfiguration(MessageStoreMockConfig::class.java)
        .withPropertyValues(
            "kraken.messaging.service-name=orders",
            "kraken.messaging.type-prefix=test",
            "kraken.messaging.hub.enabled=false",
            "kraken.messaging.kafka.bootstrap-servers=localhost:9",
            "kraken.messaging.events.consumer.enabled=false",
            "kraken.messaging.outbox.enabled=false",
            // the mocked store answers the pollers with nothing useful: keep them from spinning
            "kraken.messaging.sagas.relay.poll-interval=1h",
            "kraken.messaging.deadlines.poll-interval=1h",
        )

    private fun field(target: Any, name: String): Any? =
        target::class.java.getDeclaredField(name).also { it.isAccessible = true }.get(target)

    @Test fun `no @Saga, no saga beans`() {
        // no application package at all, and a package with nothing in it
        for (r in listOf(runner, runner.withUserConfiguration(EmptyScanConfig::class.java))) {
            r.run { context ->
                assertEquals(null, context.startupFailure)
                for (type in listOf(SagaRuntime::class.java, SagaDispatchRelay::class.java, DeadlineScheduler::class.java, SagaFailures::class.java, SagaGaugeSampler::class.java)) {
                    assertTrue(context.getBeansOfType(type).isEmpty(), "no ${type.simpleName}")
                }
                assertTrue(context.getBeansOfType(SagaStage::class.java).isEmpty())
                // the dispatcher stays the plain one: it takes no interest in a saga's event type
                assertTrue(!context.getBean(LocalEventDispatcher::class.java).interested("scan.Started"))
                assertTrue(context.getBeansOfType(IdempotencyInterceptor::class.java).isEmpty())
            }
        }
    }

    @Test fun `a @Saga class brings the runtime, relay and scheduler`() {
        runner.withUserConfiguration(SagaScanConfig::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            val runtime = context.getBean(SagaRuntime::class.java)
            assertTrue(runtime.interested("scan.Started"))
            assertNotNull(context.getBean(SagaFailures::class.java))
            // the runtime is the dispatcher's saga stage: an event no ordinary handler wants still reaches it
            assertTrue(context.getBean(LocalEventDispatcher::class.java).interested("scan.Started"))
            assertTrue(context.getBean(SagaStage::class.java) === runtime)
            // no hub: the relay sends commands to this service's own handlers
            assertIs<LocalRawCommandSender>(field(context.getBean(SagaDispatchRelay::class.java), "commands"))
            // the type of the library's failure event is the wire name the Rust side shares
            assertEquals("kraken.SagaCommandFailed", context.getBean(MessageTypeResolver::class.java).typeNameOf(SagaCommandFailed::class))
            // the default Clock, and the no-op metrics without a registry
            assertEquals(ZoneOffset.UTC, context.getBean(Clock::class.java).zone)
            assertTrue(context.getBeansOfType(SagaMetrics::class.java).isEmpty())
            assertTrue(field(runtime, "metrics") === NoopSagaMetrics)
            assertTrue(context.getBeansOfType(SagaGaugeSampler::class.java).isEmpty())
        }
    }

    @Test fun `the relay and scheduler start after the outbox publisher and stop before the event consumer`() {
        runner.withUserConfiguration(SagaScanConfig::class.java).run { context ->
            val relay = context.getBean(SagaDispatchRelay::class.java)
            val scheduler = context.getBean(DeadlineScheduler::class.java)
            assertEquals(SmartLifecycle.DEFAULT_PHASE - 1850, relay.phase)
            assertEquals(relay.phase, scheduler.phase)
            assertTrue(relay.phase > EventConsumer.PHASE, "stops before the consumer (the higher phase stops first)")
        }
    }

    @Test fun `the relay sends commands through the hub bus when there is a hub`() {
        val hub = InProcessHub().start()
        try {
            runner.withUserConfiguration(SagaScanConfig::class.java)
                .withPropertyValues("kraken.messaging.hub.enabled=true")
                .withBean("krakenHubChannelFactory", io.damoreira.kraken.messaging.hub.HubChannelFactory::class.java, { io.damoreira.kraken.messaging.hub.HubChannelFactory { hub.channel() } })
                .run { context ->
                    assertEquals(null, context.startupFailure)
                    assertIs<HubBus>(field(context.getBean(SagaDispatchRelay::class.java), "commands"))
                }
        } finally {
            hub.close()
        }
    }

    @Test fun `sagas enabled false disables them`() {
        runner.withUserConfiguration(SagaScanConfig::class.java).withPropertyValues("kraken.messaging.sagas.enabled=false").run { context ->
            assertEquals(null, context.startupFailure)
            assertTrue(context.getBeansOfType(SagaRuntime::class.java).isEmpty())
            assertTrue(context.getBeansOfType(SagaDispatchRelay::class.java).isEmpty())
            assertTrue(context.getBeansOfType(DeadlineScheduler::class.java).isEmpty())
        }
    }

    @Test fun `an @IdempotentHandler brings the interceptor`() {
        runner.withUserConfiguration(IdemScanConfig::class.java).run { context ->
            assertEquals(null, context.startupFailure)
            val interceptor = context.getBean(IdempotencyInterceptor::class.java)
            // the invoker runs idempotent handlers through it, else such a handler fails at call time
            assertTrue(field(context.getBean(HandlerInvoker::class.java), "idempotency") === interceptor)
            // and no saga beans without a @Saga
            assertTrue(context.getBeansOfType(SagaRuntime::class.java).isEmpty())
        }
        runner.withUserConfiguration(EmptyScanConfig::class.java).run { context ->
            assertNull(field(context.getBean(HandlerInvoker::class.java), "idempotency"))
        }
    }

    @Test fun `external calls enabled forces the interceptor and false removes it`() {
        runner.withUserConfiguration(EmptyScanConfig::class.java).withPropertyValues("kraken.messaging.external.calls.enabled=true").run { context ->
            assertEquals(1, context.getBeansOfType(IdempotencyInterceptor::class.java).size)
        }
        runner.withUserConfiguration(IdemScanConfig::class.java).withPropertyValues("kraken.messaging.external.calls.enabled=false").run { context ->
            assertTrue(context.getBeansOfType(IdempotencyInterceptor::class.java).isEmpty())
        }
    }

    @Test fun `an @IdempotentHandler bean without an interceptor fails the startup naming the handler and the property`() {
        runner.withUserConfiguration(IdemBeanConfig::class.java).withPropertyValues("kraken.messaging.external.calls.enabled=false").run { context ->
            val failure = context.startupFailure
            assertNotNull(failure, "the context must not start with an adapter that would fail at call time")
            val messages = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.toList()
            assertTrue(messages.any { "ScanAdapter.handle" in it && "kraken.messaging.external.calls.enabled" in it }, messages.toString())
        }
        // with the interceptor (the default for a class in the application's packages) the same bean starts
        runner.withUserConfiguration(IdemBeanConfig::class.java).run { context -> assertEquals(null, context.startupFailure) }
    }

    @Test fun `a configured source without its translator fails the startup`() {
        runner.withPropertyValues("kraken.messaging.external.kafka.sources.printer.topic=printer.events").run { context ->
            val failure = context.startupFailure
            assertNotNull(failure)
            val messages = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.toList()
            assertTrue(messages.any { "printer" in it && "ExternalRecordTranslator" in it }, messages.toString())
        }
    }

    @Test fun `a configured source with its translator brings its consumer and none brings none`() {
        runner.withPropertyValues(
            "kraken.messaging.external.kafka.sources.printer.topic=printer.events",
            "kraken.messaging.external.kafka.sources.printer.bootstrap-servers=localhost:9",
        ).withBean("printer", ExternalRecordTranslator::class.java, { ExternalRecordTranslator { null } }).run { context ->
            assertEquals(null, context.startupFailure)
            assertNotNull(context.getBean(ExternalSources::class.java))
            val provider = context.getBean(io.damoreira.kraken.messaging.external.ExternalSourceProviders::class.java).providers().getValue("printer")
            assertIs<KafkaExternalSource>(provider)
            assertEquals("orders.external.printer", provider.subscription)
            assertEquals("printer.events", provider.source)
        }
        runner.run { context ->
            assertTrue(context.getBeansOfType(ExternalSources::class.java).isEmpty())
            // the webhook side is always there: a verifier and the ingress over the local dispatcher
            assertNotNull(context.getBean(WebhookSignatureVerifier::class.java))
            assertNotNull(context.getBean(ExternalEventIngress::class.java))
        }
    }

    @Test fun `invalid durations fail the startup`() {
        val bad = mapOf(
            "kraken.messaging.sagas.relay.poll-interval=0s" to "sagas.relay",
            "kraken.messaging.sagas.relay.lease=-5s" to "sagas.relay",
            "kraken.messaging.sagas.relay.max-attempts=0" to "sagas.relay",
            "kraken.messaging.sagas.relay.send-timeout=0s" to "sagas.relay",
            "kraken.messaging.sagas.max-attempts=0" to "sagas.max-attempts",
            "kraken.messaging.sagas.ended-retention=0s" to "sagas.ended-retention",
            "kraken.messaging.sagas.dispatch-retention=-1d" to "sagas.dispatch-retention",
            "kraken.messaging.sagas.failed-retention=0s" to "sagas.failed-retention",
            "kraken.messaging.deadlines.poll-interval=0s" to "deadlines.poll-interval",
            "kraken.messaging.deadlines.lease=0s" to "deadlines.lease",
            "kraken.messaging.deadlines.retention=-1d" to "deadlines.retention",
            "kraken.messaging.external.calls.lease=0s" to "external.calls.lease",
            "kraken.messaging.external.calls.retention=0s" to "external.calls.retention",
        )
        for ((property, named) in bad) {
            // also without any saga: the value is checked when the properties bind
            runner.withPropertyValues(property).run { context ->
                val failure = context.startupFailure
                assertNotNull(failure, property)
                val messages = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.toList()
                assertTrue(messages.any { named in it }, "$property: $messages")
            }
        }
    }

    @Test fun `the saga properties have the plan's defaults`() {
        val p = KrakenMessagingProperties(serviceName = "orders", typePrefix = "test")
        assertTrue(p.sagas.enabled)
        assertEquals(5, p.sagas.maxAttempts)
        assertEquals(Duration.ofDays(30), p.sagas.endedRetention)
        assertEquals(Duration.ofDays(7), p.sagas.dispatchRetention)
        assertEquals(Duration.ofDays(30), p.sagas.failedRetention)
        assertEquals(Duration.ofMillis(500), p.sagas.relay.pollInterval)
        assertEquals(Duration.ofSeconds(30), p.sagas.relay.lease)
        assertEquals(10, p.sagas.relay.maxAttempts)
        assertEquals(Duration.ofSeconds(1), p.sagas.relay.initialBackoff)
        assertEquals(Duration.ofMinutes(5), p.sagas.relay.maxBackoff)
        assertEquals(Duration.ofMinutes(1), p.sagas.relay.sendTimeout)
        assertEquals(Duration.ofSeconds(1), p.deadlines.pollInterval)
        assertEquals(Duration.ofSeconds(30), p.deadlines.lease)
        assertEquals(Duration.ofDays(7), p.deadlines.retention)
        assertNull(p.external.calls.enabled)
        assertEquals(Duration.ofMinutes(5), p.external.calls.lease)
        assertEquals(Duration.ofDays(30), p.external.calls.retention)
    }

    @Test fun `the saga properties bind from the environment`() {
        runner.withPropertyValues(
            "kraken.messaging.sagas.relay.max-attempts=3",
            "kraken.messaging.sagas.relay.initial-backoff=2s",
            "kraken.messaging.sagas.ended-retention=10d",
            "kraken.messaging.deadlines.lease=45s",
            "kraken.messaging.external.calls.retention=10d",
        ).run { context ->
            val p = context.getBean(KrakenMessagingProperties::class.java)
            assertEquals(3, p.sagas.relay.maxAttempts)
            assertEquals(Duration.ofSeconds(2), p.sagas.relay.initialBackoff)
            assertEquals(Duration.ofDays(10), p.sagas.endedRetention)
            assertEquals(Duration.ofSeconds(45), p.deadlines.lease)
            assertEquals(Duration.ofDays(10), p.external.calls.retention)
            assertEquals(Duration.ofHours(1), p.sagas.relay.pollInterval, "the runner's own override")
        }
    }

    @Test fun `a user Clock bean replaces the default`() {
        val fixed = Clock.fixed(Instant.parse("2026-11-03T10:00:00Z"), ZoneOffset.UTC)
        runner.withUserConfiguration(SagaScanConfig::class.java).withBean(Clock::class.java, { fixed }).run { context ->
            assertEquals(1, context.getBeansOfType(Clock::class.java).size)
            assertTrue(context.getBean(Clock::class.java) === fixed)
        }
    }

    @Test fun `a Micrometer registry brings the saga metrics and the gauge sampler, only with sagas`() {
        runner.withUserConfiguration(SagaScanConfig::class.java).withBean(SimpleMeterRegistry::class.java, { SimpleMeterRegistry() }).run { context ->
            assertEquals(null, context.startupFailure)
            assertIs<MicrometerSagaMetrics>(context.getBean(SagaMetrics::class.java))
            assertNotNull(context.getBean(SagaGaugeSampler::class.java))
            val registry = context.getBean(SimpleMeterRegistry::class.java)
            // a gauge for each saga type exists from the start
            assertNotNull(registry.find("kraken.sagas.active").tag("type", "scan.Order").gauge())
            assertNotNull(registry.find("kraken.saga.dispatch.pending").gauge())
        }
        // a registry and no saga: no saga meters at all
        runner.withUserConfiguration(EmptyScanConfig::class.java).withBean(SimpleMeterRegistry::class.java, { SimpleMeterRegistry() }).run { context ->
            assertNull(context.getBean(SimpleMeterRegistry::class.java).find("kraken.saga.dispatch.pending").gauge())
            assertTrue(context.getBeansOfType(SagaGaugeSampler::class.java).isEmpty())
        }
    }

    @Test fun `the configuration metadata describes every saga, deadline, external and events key`() {
        val json = checkNotNull(javaClass.getResourceAsStream("/META-INF/spring-configuration-metadata.json")).use { io.damoreira.kraken.messaging.codec.JsonCodec().decodeTree(it.readAllBytes()) }
        val described = json["properties"].associate { it["name"].asString() to it["description"]?.asString() }
        val keys = listOf(
            "sagas.enabled", "sagas.max-attempts", "sagas.ended-retention", "sagas.dispatch-retention", "sagas.failed-retention",
            "sagas.relay.poll-interval", "sagas.relay.lease", "sagas.relay.max-attempts", "sagas.relay.initial-backoff", "sagas.relay.max-backoff", "sagas.relay.send-timeout",
            "deadlines.poll-interval", "deadlines.lease", "deadlines.retention",
            "external.calls.enabled", "external.calls.lease", "external.calls.retention",
            "event-store", "events.source", "events.subscription-prefix", "events.consumer.enabled", "events.consumer.max-attempts",
            "events.dead-letter.mode", "events.dead-letter.retention",
        )
        for (key in keys) {
            val name = "kraken.messaging.$key"
            assertTrue(name in described, "$name is in the metadata")
            assertTrue(!described[name].isNullOrBlank(), "$name has a description")
        }
        val groups = json["groups"].toList().map { it["name"].asString() }
        assertTrue(listOf("sagas", "sagas.relay", "deadlines", "external", "external.calls", "events", "events.consumer", "events.dead-letter").all { "kraken.messaging.$it" in groups })
    }
}
