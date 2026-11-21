package com.damoreira.snodra.examples.api

import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType

/*
 * The postcard saga of service-b: every delivered greeting is printed on a postcard by an external printer
 * (the fake-printer example). Service-b's GreetingDeliverySaga drives it; these are the messages it sends and reacts to.
 */

/** Asks the printer adapter (service-b) to submit the postcard of greeting [jobId] to the printer, with an idempotency key. */
@MessageType("examples.PrintPostcard")
data class PrintPostcard(val jobId: String, val name: String) : Command<Unit>

/** Asks the printer adapter to look at the printer's job [jobId]: poll number [attempt]; it publishes [PostcardPrinted] when the job is done. */
@MessageType("examples.PollPrintJob")
data class PollPrintJob(val jobId: String, val attempt: Int) : Command<Unit>

/** The printer finished the postcard of [jobId]: reported by webhook, by its Kafka topic, or found by polling. */
@MessageType("examples.PostcardPrinted")
data class PostcardPrinted(val jobId: String, val name: String) : Event

/** The postcard of [jobId] was not printed: [reason] is `TIMEOUT` or the name of a `SagaCommandFailed.Reason` (for example `UNKNOWN_OUTCOME`). */
@MessageType("examples.PostcardFailed")
data class PostcardFailed(val jobId: String, val name: String, val reason: String) : Event
