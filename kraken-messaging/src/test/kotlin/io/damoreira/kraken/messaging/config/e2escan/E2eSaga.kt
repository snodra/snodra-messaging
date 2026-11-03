package io.damoreira.kraken.messaging.config.e2escan

import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Event
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.external.IdempotentHandler
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.saga.DeadlineHandler
import io.damoreira.kraken.messaging.saga.DeadlineManager
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaCommands
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.StartSaga
import org.springframework.boot.autoconfigure.AutoConfigurationPackage
import org.springframework.context.annotation.Configuration
import java.time.Duration
import java.util.concurrent.atomic.AtomicInteger

@MessageType("e2e.Started")
data class E2eStarted(val jobId: String) : Event

@MessageType("e2e.Print")
data class E2ePrint(val jobId: String) : Command<Unit>

/** A Spring bean the saga handler asks for by type: the adapter's call counter. */
class E2eCounter {
    val prints = AtomicInteger()
}

/** Sends a command to its own service's adapter and schedules a deadline: the relay, the scheduler and the interceptor all take part. */
@Saga(name = "e2e.Job")
class E2eSaga {
    var jobId: String? = null
    var timedOut = false

    @StartSaga
    @SagaEventHandler(associationProperty = "jobId")
    fun on(e: E2eStarted, commands: SagaCommands, deadlines: DeadlineManager, counter: E2eCounter) {
        jobId = e.jobId
        commands.send(E2ePrint(e.jobId))
        deadlines.schedule(Duration.ofMillis(300), "timeout")
    }

    @DeadlineHandler("timeout")
    fun timeout() {
        timedOut = true
    }
}

class E2ePrintAdapter(private val counter: E2eCounter) {
    @CommandHandler
    @IdempotentHandler
    fun handle(c: E2ePrint) {
        counter.prints.incrementAndGet()
    }
}

@Configuration(proxyBeanMethods = false)
@AutoConfigurationPackage
class E2eScanConfig
