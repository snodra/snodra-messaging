package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.GreetingDelivered
import io.damoreira.kraken.examples.api.PollPrintJob
import io.damoreira.kraken.examples.api.PostcardFailed
import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.examples.api.PrintPostcard
import io.damoreira.kraken.messaging.saga.DeadlineHandler
import io.damoreira.kraken.messaging.saga.DeadlineManager
import io.damoreira.kraken.messaging.saga.EndSaga
import io.damoreira.kraken.messaging.saga.Saga
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.saga.SagaCommands
import io.damoreira.kraken.messaging.saga.SagaEventHandler
import io.damoreira.kraken.messaging.saga.SagaLifecycle
import io.damoreira.kraken.messaging.saga.StartSaga
import java.time.Duration

/**
 * Gets the postcard of a delivered greeting printed by an external printer, whatever way the printer answers.
 *
 * It starts on [GreetingDelivered] (the greeting's id is the job id), sends [PrintPostcard] to the printer adapter and
 * gives the printer 30 seconds. The printer's answer arrives as [PostcardPrinted] by one of three channels, none of which
 * the saga knows: a webhook (`PrinterWebhookController`), the printer's own Kafka topic (`PrinterTranslator`), or a poll the
 * saga asks for (a greeting named `poll-...` makes it send [PollPrintJob] every 2 seconds; the adapter publishes
 * [PostcardPrinted] when the job is done). Printed ends the saga, which cancels its deadlines. Otherwise the saga
 * publishes [PostcardFailed] and ends: when the timeout falls due (`TIMEOUT`), or when a command it sent failed for good
 * (`SagaCommandFailed`, for example `UNKNOWN_OUTCOME` when the printer crashed in the middle of the call: the saga cannot
 * tell whether a postcard was printed, and in this example it gives up and says so).
 */
@Saga(name = "examples.GreetingDelivery")
class GreetingDeliverySaga {
    var jobId: String? = null
    var name: String? = null
    var polls = 0

    @StartSaga
    @SagaEventHandler(associationProperty = "id", keyName = "jobId")
    fun on(e: GreetingDelivered, saga: SagaLifecycle, commands: SagaCommands, deadlines: DeadlineManager) {
        jobId = e.id
        name = e.name
        saga.associateWith("sagaId", saga.sagaId) // routes the SagaCommandFailed of this saga's commands back to it
        commands.send(PrintPostcard(e.id, e.name))
        deadlines.schedule(TIMEOUT, TIMEOUT_DEADLINE)
        if (e.name.startsWith(POLL_PREFIX)) deadlines.schedule(POLL_EVERY, POLL_DEADLINE)
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "jobId")
    fun on(e: PostcardPrinted) = Unit

    @DeadlineHandler(POLL_DEADLINE)
    fun poll(commands: SagaCommands, deadlines: DeadlineManager) {
        commands.send(PollPrintJob(jobId!!, ++polls))
        deadlines.schedule(POLL_EVERY, POLL_DEADLINE)
    }

    @EndSaga
    @DeadlineHandler(TIMEOUT_DEADLINE)
    fun timeout(commands: SagaCommands) {
        commands.publish(PostcardFailed(jobId!!, name!!, "TIMEOUT"), key = name)
    }

    @EndSaga
    @SagaEventHandler(associationProperty = "sagaId")
    fun on(e: SagaCommandFailed, commands: SagaCommands) {
        commands.publish(PostcardFailed(jobId!!, name!!, e.reason.name), key = name)
    }

    private companion object {
        val TIMEOUT: Duration = Duration.ofSeconds(30)
        val POLL_EVERY: Duration = Duration.ofSeconds(2)
        const val TIMEOUT_DEADLINE = "print-timeout"
        const val POLL_DEADLINE = "poll-printer"
        const val POLL_PREFIX = "poll-"
    }
}
