package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.PollPrintJob
import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.examples.api.PrintPostcard
import io.damoreira.kraken.messaging.EventBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.external.IdempotentHandler
import io.damoreira.kraken.messaging.handler.CommandHandler
import org.springframework.stereotype.Component

/**
 * The edge of service-b towards the printer: ordinary command handlers made idempotent by `@IdempotentHandler`, so a command
 * the saga relay sends twice (a crash between sending and marking it sent) runs once. They are plain blocking methods: the
 * external call must not run inside a coroutine's thread hop or a Mongo transaction.
 */
@Component
class PrinterAdapter(private val printer: Printer, private val events: EventBus) {

    /** Submits the postcard; the command's `idempotency-key` (`<sagaId>:<seq>`) goes to the printer too, which de-duplicates by it. */
    @CommandHandler
    @IdempotentHandler
    fun print(cmd: PrintPostcard, ctx: MessageContext) {
        val key = checkNotNull(ctx.headers["idempotency-key"]) { "PrintPostcard ${cmd.jobId} arrived without an idempotency-key header" }
        printer.submit(cmd.jobId, cmd.name, key)
    }

    /** Asks the printer about the job; publishes [PostcardPrinted] when it is done. One call per attempt number, however often it is sent. */
    @CommandHandler
    @IdempotentHandler(key = "'poll:' + message.jobId + ':' + message.attempt")
    fun poll(cmd: PollPrintJob) {
        val job = printer.status(cmd.jobId) ?: return
        if (job.done) events.publishBlocking(PostcardPrinted(job.jobId, job.name), key = job.name)
    }
}
