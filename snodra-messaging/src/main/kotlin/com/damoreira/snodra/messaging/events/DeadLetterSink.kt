package com.damoreira.snodra.messaging.events

import com.damoreira.snodra.messaging.deadletter.DeadLetterFactory
import com.damoreira.snodra.messaging.deadletter.DeadLetterMode
import com.damoreira.snodra.messaging.eventstore.Capability
import com.damoreira.snodra.messaging.eventstore.deadLetterSource
import com.damoreira.snodra.messaging.store.DeadLetterStore
import java.time.Duration

/** Where an [EventConsumer] puts a record it cannot handle (`snodra.messaging.events.dead-letter.mode`, ADR 0022). */
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
    class EventStore(val events: com.damoreira.snodra.messaging.eventstore.EventStore, val sendTimeout: Duration = Duration.ofSeconds(30)) : DeadLetterSink {
        override val mode = DeadLetterMode.EVENT_STORE

        init {
            require(events.supports(Capability.NATIVE_DEAD_LETTER)) {
                "the dead-letter mode event-store needs an event store with the capability ${Capability.NATIVE_DEAD_LETTER}: ${events.name} lacks it"
            }
        }
    }
}
