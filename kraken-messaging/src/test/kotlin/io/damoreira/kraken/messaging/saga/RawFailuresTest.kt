package io.damoreira.kraken.messaging.saga

import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.saga.SagaCommandFailed.Reason
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

/** The retry taxonomy of the spec ("Delivery and consistency"): which failures the relay retries and how the others are named. */
class RawFailuresTest {
    private fun permanent(e: Throwable): Reason = assertIs<RawOutcome.Permanent>(RawFailures.classify(e), "$e").reason

    @Test
    fun `an unreachable hub, a timeout, a lost handler or connection and a busy handler are transient`() {
        listOf(
            HubUnavailableException("no stream"), MessageTimeoutException("t"), HandlerDisconnectedException("t"),
            HubConnectionLostException("broken"), HandlerBusyException("t"),
        ).forEach { assertIs<RawOutcome.Transient>(RawFailures.classify(it), "$it") }
    }

    @Test
    fun `an adapter failure marked retryable is transient`() {
        val details = """{"kraken":"retryable"}""".toByteArray()
        assertIs<RawOutcome.Transient>(RawFailures.classify(RemoteHandlerException("t", "RetryableExternalException: busy", details)))
    }

    @Test
    fun `permanent failures are named by the taxonomy`() {
        assertEquals(Reason.NO_HANDLER, permanent(NoHandlerException("t")))
        assertEquals(Reason.DENIED, permanent(UnauthenticatedException("no token")))
        assertEquals(Reason.DENIED, permanent(AccessDeniedException("no")))
        assertEquals(Reason.INVALID, permanent(InvalidMessageException("bad")))
        assertEquals(Reason.FAILED, permanent(RemoteHandlerException("t", "IllegalStateException: no", ByteArray(0))))
        assertEquals(Reason.FAILED, permanent(StreamCancelledException("cancelled")), "any other messaging failure is a handler error")
    }

    @Test
    fun `an unknown outcome reported by an adapter is UNKNOWN_OUTCOME`() {
        val details = """{"kraken":"unknown-outcome"}""".toByteArray()
        assertEquals(Reason.UNKNOWN_OUTCOME, permanent(RemoteHandlerException("t", "outcome unknown", details)))
    }

    @Test
    fun `the message of the failure is kept`() {
        assertEquals("No handler registered for 'x.Y'", (RawFailures.classify(NoHandlerException("x.Y")) as RawOutcome.Permanent).message)
    }
}
