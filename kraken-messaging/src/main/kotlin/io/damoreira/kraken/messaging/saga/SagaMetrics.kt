package io.damoreira.kraken.messaging.saga

/**
 * What the saga runtime reports; implemented by Micrometer when a registry exists (Task 12), else [NoopSagaMetrics].
 * Methods have empty defaults so more can be added without breaking implementations.
 */
interface SagaMetrics {
    /** The result of one (event, saga type) handling, `kraken_saga_events_total{type,outcome}`. */
    enum class Outcome { HANDLED, NO_MATCH, CONFLICT, PARKED }

    fun sagaStarted(type: String) = Unit
    fun sagaEnded(type: String) = Unit
    fun event(type: String, outcome: Outcome) = Unit

    /** A deadline of [name] ran its handler. */
    fun deadlineFired(name: String) = Unit

    /** The SCHEDULED deadlines past their due time after a scheduler poll, per deadline name (empty when none): `kraken_deadlines_due{name}`. */
    fun deadlinesOverdue(byName: Map<String, Long>) = Unit

    /** A `SagaCommandFailed` for [commandType] reached no saga or handler: logged at error, `kraken_saga_command_failures_total`. */
    fun commandFailureUnhandled(commandType: String, reason: SagaCommandFailed.Reason) = Unit
}

/** No metrics: used when no `MeterRegistry` bean exists. */
object NoopSagaMetrics : SagaMetrics
