package io.damoreira.kraken.messaging.kafka

import io.damoreira.kraken.messaging.deadletter.DeadLetterMode
import io.damoreira.kraken.messaging.deadletter.DeadLetterStore
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration

/** Where an [EventConsumer] puts a record it cannot handle (`kraken.messaging.kafka.dead-letter.mode`, ADR 0022). */
sealed interface DeadLetterSink {
    val mode: DeadLetterMode

    /** Log it (never the payload) and skip it. */
    data object Log : DeadLetterSink {
        override val mode = DeadLetterMode.LOG
    }

    /** Park it in `dead_letters` and sequence its key's later records behind it. */
    class Collection(val store: DeadLetterStore) : DeadLetterSink {
        override val mode = DeadLetterMode.COLLECTION
    }

    /** Produce it to `<topic>.dlt`; the offset is acknowledged only after the broker acknowledged the produce ([sendTimeout]). */
    class Topic(val kafka: KafkaTemplate<String, ByteArray>, val sendTimeout: Duration = Duration.ofSeconds(30)) : DeadLetterSink {
        override val mode = DeadLetterMode.TOPIC
    }
}
