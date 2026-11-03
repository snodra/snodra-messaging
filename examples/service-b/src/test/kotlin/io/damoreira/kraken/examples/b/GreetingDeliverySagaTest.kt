package io.damoreira.kraken.examples.b

import io.damoreira.kraken.examples.api.GreetingDelivered
import io.damoreira.kraken.examples.api.PollPrintJob
import io.damoreira.kraken.examples.api.PostcardFailed
import io.damoreira.kraken.examples.api.PostcardPrinted
import io.damoreira.kraken.examples.api.PrintPostcard
import io.damoreira.kraken.messaging.saga.SagaCommandFailed
import io.damoreira.kraken.messaging.test.SagaTestFixture
import java.time.Duration
import java.time.Instant
import kotlin.test.Test

class GreetingDeliverySagaTest {
    private val t0 = Instant.parse("2026-11-03T10:00:00Z")

    private fun saga() = SagaTestFixture(GreetingDeliverySaga::class).givenCurrentTime(t0)
    private fun delivered(id: String, name: String) = GreetingDelivered(id, name, t0)

    @Test fun `a delivered greeting sends PrintPostcard and schedules the 30 second timeout`() {
        saga().whenEvent(delivered("g1", "webhook-ada"))
            .expectActiveSagas(1)
            .expectAssociation("jobId", "g1")
            .expectDispatchedCommands(PrintPostcard("g1", "webhook-ada"))
            .expectScheduledDeadline("print-timeout", t0.plusSeconds(30))
    }

    @Test fun `a printed postcard reported by webhook or topic ends the saga and cancels the timeout`() {
        saga().givenEvents(delivered("g1", "webhook-ada"))
            .whenEvent(PostcardPrinted("g1", "webhook-ada"))
            .expectSagaEnded()
            .expectNoScheduledDeadlines()
            .expectDispatchedCommands()
            .expectPublishedEvents()
    }

    @Test fun `a printed postcard of another job leaves the saga alone`() {
        saga().givenEvents(delivered("g1", "webhook-ada"))
            .whenEvent(PostcardPrinted("g2", "webhook-bob"))
            .expectActiveSagas(1)
    }

    @Test fun `a poll greeting also schedules a polling deadline every 2 seconds`() {
        saga().whenEvent(delivered("g1", "poll-ada"))
            .expectScheduledDeadline("print-timeout", t0.plusSeconds(30))
            .expectScheduledDeadline("poll-printer", t0.plusSeconds(2))
    }

    @Test fun `polling sends PollPrintJob with a growing attempt each 2 seconds until the printer reports`() {
        saga().givenEvents(delivered("g1", "poll-ada"))
            .whenTimeElapses(Duration.ofSeconds(5))
            .expectDispatchedCommands(PollPrintJob("g1", 1), PollPrintJob("g1", 2))
            .expectScheduledDeadline("poll-printer", t0.plusSeconds(6))
            .whenEvent(PostcardPrinted("g1", "poll-ada"))
            .expectSagaEnded()
            .expectNoScheduledDeadlines()
            .whenTimeElapses(Duration.ofSeconds(60))
            .expectDispatchedCommands(PollPrintJob("g1", 1), PollPrintJob("g1", 2))
    }

    @Test fun `a printer that never answers fails the postcard with TIMEOUT after 30 seconds`() {
        saga().givenEvents(delivered("g1", "silent-ada"))
            .whenTimeElapses(Duration.ofSeconds(29))
            .expectActiveSagas(1)
            .expectPublishedEvents()
            .whenTimeElapses(Duration.ofSeconds(1))
            .expectPublishedEvents(PostcardFailed("g1", "silent-ada", "TIMEOUT"))
            .expectSagaEnded()
            .expectNoScheduledDeadlines()
    }

    @Test fun `polling that never finds the job also ends in TIMEOUT and stops polling`() {
        saga().givenEvents(delivered("g1", "poll-ada"))
            .whenTimeElapses(Duration.ofSeconds(30))
            .expectPublishedEvents(PostcardFailed("g1", "poll-ada", "TIMEOUT"))
            .expectSagaEnded()
            .expectNoScheduledDeadlines()
    }

    @Test fun `an unknown outcome from the adapter fails the postcard with UNKNOWN_OUTCOME and cancels the timeout`() {
        saga().givenEvents(delivered("g1", "crash-ada"))
            .whenCommandFailed(PrintPostcard("g1", "crash-ada"), SagaCommandFailed.Reason.UNKNOWN_OUTCOME, "the printer crashed")
            .expectPublishedEvents(PostcardFailed("g1", "crash-ada", "UNKNOWN_OUTCOME"))
            .expectSagaEnded()
            .expectNoScheduledDeadlines()
    }

    @Test fun `any other command failure fails the postcard with the failure's reason`() {
        saga().givenEvents(delivered("g1", "webhook-ada"))
            .whenCommandFailed(PrintPostcard("g1", "webhook-ada"), SagaCommandFailed.Reason.EXHAUSTED, "the printer is down")
            .expectPublishedEvents(PostcardFailed("g1", "webhook-ada", "EXHAUSTED"))
            .expectSagaEnded()
    }

    @Test fun `two greetings are two independent sagas`() {
        saga().givenEvents(delivered("g1", "webhook-ada"), delivered("g2", "webhook-bob"))
            .whenEvent(PostcardPrinted("g1", "webhook-ada"))
            .expectActiveSagas(1)
            .expectAssociation("jobId", "g2")
    }
}
