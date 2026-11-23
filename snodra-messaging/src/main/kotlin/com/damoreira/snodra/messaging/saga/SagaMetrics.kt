package com.damoreira.snodra.messaging.saga

/**
 * What the saga runtime reports; implemented by Micrometer when a registry exists, else [NoopSagaMetrics].
 * Methods have empty defaults so more can be added without breaking implementations.
 */
interface SagaMetrics {
    /** The result of one (event, saga type) handling, `snodra_saga_events_total{type,outcome}`. */
    enum class Outcome { HANDLED, NO_MATCH, CONFLICT, PARKED }

    fun sagaStarted(type: String) = Unit
    fun sagaEnded(type: String) = Unit
    fun event(type: String, outcome: Outcome) = Unit

    /** A deadline of [name] ran its handler. */
    fun deadlineFired(name: String) = Unit

    /** The SCHEDULED deadlines past their due time after a scheduler poll, per deadline name (empty when none): `snodra_deadlines_due{name}`. */
    fun deadlinesOverdue(byName: Map<String, Long>) = Unit

    /** How one call of an `@IdempotentHandler` ended, `snodra_external_calls_total{handler,outcome}`. */
    enum class ExternalOutcome {
        /** The body ran and succeeded. */
        SUCCEEDED,

        /** The stored reply of an earlier success was returned. */
        REPLAYED,

        /** The body failed for good (stored; later calls fail the same way), or a stored failure was re-thrown. */
        FAILED,

        /** The body asked for a later attempt, or another call of the same key was in flight. */
        RETRYABLE,

        /** An earlier call crashed and its outcome is unknown. */
        UNKNOWN,
    }

    fun externalCall(handler: String, outcome: ExternalOutcome) = Unit

    /**
     * A `SagaCommandFailed` for [commandType] reached no saga or handler, or a saga's event of that type could not be
     * published (reason INVALID): logged at error, `snodra_saga_command_failures_total`.
     */
    fun commandFailureUnhandled(commandType: String, reason: SagaCommandFailed.Reason) = Unit
}

/** No metrics: used when no `MeterRegistry` bean exists. */
object NoopSagaMetrics : SagaMetrics
