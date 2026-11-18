package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.config.emptyscan.EmptyScanConfig
import com.damoreira.snodra.messaging.config.idemscan.IdemBeanConfig
import com.damoreira.snodra.messaging.config.idemscan.IdemScanConfig
import com.damoreira.snodra.messaging.config.sagascan.SagaScanConfig
import com.damoreira.snodra.messaging.deadline.DeadlineScheduler
import com.damoreira.snodra.messaging.events.EventConsumer
import com.damoreira.snodra.messaging.events.LocalEventDispatcher
import com.damoreira.snodra.messaging.external.ExternalEventIngress
import com.damoreira.snodra.messaging.external.ExternalRecordTranslator
import com.damoreira.snodra.messaging.external.ExternalSourceProviders
import com.damoreira.snodra.messaging.external.ExternalSources
import com.damoreira.snodra.messaging.external.IdempotencyInterceptor
import com.damoreira.snodra.messaging.external.WebhookSignatureVerifier
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.hub.HubBus
import com.damoreira.snodra.messaging.kafka.KafkaEventStoreAutoConfiguration
import com.damoreira.snodra.messaging.kafka.KafkaExternalSource
import com.damoreira.snodra.messaging.saga.LocalRawCommandSender
import com.damoreira.snodra.messaging.saga.MicrometerSagaMetrics
import com.damoreira.snodra.messaging.saga.NoopSagaMetrics
import com.damoreira.snodra.messaging.saga.SagaCommandFailed
import com.damoreira.snodra.messaging.saga.SagaDispatchRelay
import com.damoreira.snodra.messaging.saga.SagaFailures
import com.damoreira.snodra.messaging.saga.SagaGaugeSampler
import com.damoreira.snodra.messaging.saga.SagaMetrics
import com.damoreira.snodra.messaging.saga.SagaRuntime
import com.damoreira.snodra.messaging.saga.SagaStage
import com.damoreira.snodra.messaging.test.InProcessHub
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
                SnodraMessagingAutoConfiguration::class.java,
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
            "snodra.messaging.service-name=orders",
            "snodra.messaging.type-prefix=test",
            "snodra.messaging.hub.enabled=false",
            "snodra.messaging.kafka.bootstrap-servers=localhost:9",
            "snodra.messaging.events.consumer.enabled=false",
            "snodra.messaging.outbox.enabled=false",
            // the mocked store answers the pollers with nothing useful: keep them from spinning
            "snodra.messaging.sagas.relay.poll-interval=1h",
            "snodra.messaging.deadlines.poll-interval=1h",
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
            assertEquals("snodra.SagaCommandFailed", context.getBean(MessageTypeResolver::class.java).typeNameOf(SagaCommandFailed::class))
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
                .withPropertyValues("snodra.messaging.hub.enabled=true")
                .withBean("snodraHubChannelFactory", com.damoreira.snodra.messaging.hub.HubChannelFactory::class.java, { com.damoreira.snodra.messaging.hub.HubChannelFactory { hub.channel() } })
                .run { context ->
                    assertEquals(null, context.startupFailure)
                    assertIs<HubBus>(field(context.getBean(SagaDispatchRelay::class.java), "commands"))
                }
        } finally {
            hub.close()
        }
    }

    @Test fun `sagas enabled false disables them`() {
        runner.withUserConfiguration(SagaScanConfig::class.java).withPropertyValues("snodra.messaging.sagas.enabled=false").run { context ->
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
        runner.withUserConfiguration(EmptyScanConfig::class.java).withPropertyValues("snodra.messaging.external.calls.enabled=true").run { context ->
            assertEquals(1, context.getBeansOfType(IdempotencyInterceptor::class.java).size)
        }
        runner.withUserConfiguration(IdemScanConfig::class.java).withPropertyValues("snodra.messaging.external.calls.enabled=false").run { context ->
            assertTrue(context.getBeansOfType(IdempotencyInterceptor::class.java).isEmpty())
        }
    }

    @Test fun `an @IdempotentHandler bean without an interceptor fails the startup naming the handler and the property`() {
        runner.withUserConfiguration(IdemBeanConfig::class.java).withPropertyValues("snodra.messaging.external.calls.enabled=false").run { context ->
            val failure = context.startupFailure
            assertNotNull(failure, "the context must not start with an adapter that would fail at call time")
            val messages = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.toList()
            assertTrue(messages.any { "ScanAdapter.handle" in it && "snodra.messaging.external.calls.enabled" in it }, messages.toString())
        }
        // with the interceptor (the default for a class in the application's packages) the same bean starts
        runner.withUserConfiguration(IdemBeanConfig::class.java).run { context -> assertEquals(null, context.startupFailure) }
    }

    @Test fun `a configured source without its translator fails the startup`() {
        runner.withPropertyValues("snodra.messaging.external.kafka.sources.printer.topic=printer.events").run { context ->
            val failure = context.startupFailure
            assertNotNull(failure)
            val messages = generateSequence<Throwable>(failure) { it.cause }.mapNotNull { it.message }.toList()
            assertTrue(messages.any { "printer" in it && "ExternalRecordTranslator" in it }, messages.toString())
        }
    }

    @Test fun `a configured source with its translator brings its consumer and none brings none`() {
        runner.withPropertyValues(
            "snodra.messaging.external.kafka.sources.printer.topic=printer.events",
            "snodra.messaging.external.kafka.sources.printer.bootstrap-servers=localhost:9",
        ).withBean("printer", ExternalRecordTranslator::class.java, { ExternalRecordTranslator { null } }).run { context ->
            assertEquals(null, context.startupFailure)
            assertNotNull(context.getBean(ExternalSources::class.java))
            val provider = context.getBean(ExternalSourceProviders::class.java).providers().getValue("printer")
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
            "snodra.messaging.sagas.relay.poll-interval=0s" to "sagas.relay",
            "snodra.messaging.sagas.relay.lease=-5s" to "sagas.relay",
            "snodra.messaging.sagas.relay.max-attempts=0" to "sagas.relay",
            "snodra.messaging.sagas.relay.send-timeout=0s" to "sagas.relay",
            "snodra.messaging.sagas.max-attempts=0" to "sagas.max-attempts",
            "snodra.messaging.sagas.ended-retention=0s" to "sagas.ended-retention",
            "snodra.messaging.sagas.dispatch-retention=-1d" to "sagas.dispatch-retention",
            "snodra.messaging.sagas.failed-retention=0s" to "sagas.failed-retention",
            "snodra.messaging.deadlines.poll-interval=0s" to "deadlines.poll-interval",
            "snodra.messaging.deadlines.lease=0s" to "deadlines.lease",
            "snodra.messaging.deadlines.retention=-1d" to "deadlines.retention",
            "snodra.messaging.external.calls.lease=0s" to "external.calls.lease",
            "snodra.messaging.external.calls.retention=0s" to "external.calls.retention",
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
        val p = SnodraMessagingProperties(serviceName = "orders", typePrefix = "test")
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
            "snodra.messaging.sagas.relay.max-attempts=3",
            "snodra.messaging.sagas.relay.initial-backoff=2s",
            "snodra.messaging.sagas.ended-retention=10d",
            "snodra.messaging.deadlines.lease=45s",
            "snodra.messaging.external.calls.retention=10d",
        ).run { context ->
            val p = context.getBean(SnodraMessagingProperties::class.java)
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
            assertNotNull(registry.find("snodra.sagas.active").tag("type", "scan.Order").gauge())
            assertNotNull(registry.find("snodra.saga.dispatch.pending").gauge())
        }
        // a registry and no saga: no saga meters at all
        runner.withUserConfiguration(EmptyScanConfig::class.java).withBean(SimpleMeterRegistry::class.java, { SimpleMeterRegistry() }).run { context ->
            assertNull(context.getBean(SimpleMeterRegistry::class.java).find("snodra.saga.dispatch.pending").gauge())
            assertTrue(context.getBeansOfType(SagaGaugeSampler::class.java).isEmpty())
        }
    }

    @Test fun `the configuration metadata describes every saga, deadline, external and events key`() {
        val json = checkNotNull(javaClass.getResourceAsStream("/META-INF/spring-configuration-metadata.json")).use { com.damoreira.snodra.messaging.codec.JsonCodec().decodeTree(it.readAllBytes()) }
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
            val name = "snodra.messaging.$key"
            assertTrue(name in described, "$name is in the metadata")
            assertTrue(!described[name].isNullOrBlank(), "$name has a description")
        }
        val groups = json["groups"].toList().map { it["name"].asString() }
        assertTrue(listOf("sagas", "sagas.relay", "deadlines", "external", "external.calls", "events", "events.consumer", "events.dead-letter").all { "snodra.messaging.$it" in groups })
    }
}
