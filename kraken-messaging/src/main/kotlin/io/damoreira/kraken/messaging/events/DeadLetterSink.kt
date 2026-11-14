package io.damoreira.kraken.messaging.events

import io.damoreira.kraken.messaging.deadletter.DeadLetterFactory
import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.eventstore.Capability
import io.damoreira.kraken.messaging.eventstore.deadLetterSource
import io.damoreira.kraken.messaging.store.DeadLetterStore
import java.time.Duration

/** Where an [EventConsumer] puts a record it cannot handle (`kraken.messaging.events.dead-letter.mode`, ADR 0022). */
sealed interface DeadLetterSink {
    val mode: DeadLetterMode

    /** Log it (never the payload) and skip it. */
    data object Log : DeadLetterSink {
        override val mode = DeadLetterMode.LOG
    }

    /** Park it in the message store's dead letters, built by [letters], and sequence its key's later records behind it. */
    class Collection(val store: DeadLetterStore, val letters: DeadLetterFactory) : DeadLetterSink {
        override val mode = DeadLetterMode.COLLECTION
    }

    /**
     * Publish it to [events]' dead-letter source of the source it was read from ([deadLetterSource]); the record is acknowledged
     * only after the event store took it (within [sendTimeout]). [events] must declare [Capability.NATIVE_DEAD_LETTER].
     */
    class EventStore(val events: io.damoreira.kraken.messaging.eventstore.EventStore, val sendTimeout: Duration = Duration.ofSeconds(30)) : DeadLetterSink {
        override val mode = DeadLetterMode.EVENT_STORE

        init {
            require(events.supports(Capability.NATIVE_DEAD_LETTER)) {
                "the dead-letter mode event-store needs an event store with the capability ${Capability.NATIVE_DEAD_LETTER}: ${events.name} lacks it"
            }
        }
    }
}
