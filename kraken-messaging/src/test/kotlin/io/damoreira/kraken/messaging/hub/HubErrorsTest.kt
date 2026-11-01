package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.UnsupportedByHubException
import org.junit.jupiter.api.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs

class HubErrorsTest {

    @Test
    fun `permission denied maps to AccessDeniedException`() {
        val e = HubErrors.toException("c.R", ErrorCode.PERMISSION_DENIED, "handler X: access denied", ByteArray(0))

        assertIs<AccessDeniedException>(e)
        assertEquals("handler X: access denied", e.message)
    }

    @Test
    fun `unauthenticated keeps mapping to UnauthenticatedException`() {
        val e = HubErrors.toException("c.R", ErrorCode.UNAUTHENTICATED, "handler X requires authentication", ByteArray(0))

        assertIs<UnauthenticatedException>(e)
    }

    @Test
    fun `resource exhausted maps to HandlerBusyException`() {
        val e = HubErrors.toException("c.R", ErrorCode.RESOURCE_EXHAUSTED, "queue of i-1 is full", ByteArray(0))

        assertIs<HandlerBusyException>(e)
        assertEquals("c.R", e.type)
        // RESOURCE_EXHAUSTED also answers a full session buffer, not only a full queue.
        assertEquals("Handler for 'c.R' is busy: its hub queue or session buffer is full", e.message)
    }

    @Test
    fun `unsupported maps to UnsupportedByHubException, an unmapped code keeps its name`() {
        val e = HubErrors.toException("c.R", ErrorCode.UNSUPPORTED, "mode STREAMING is not supported", ByteArray(0))
        assertIs<UnsupportedByHubException>(e)
        assertEquals("mode STREAMING is not supported", e.message)

        val streaming = HubErrors.toException("c.R", ErrorCode.UNSUPPORTED, "no capable instance", ByteArray(0), feature = "streaming")
        assertEquals("streaming", assertIs<UnsupportedByHubException>(streaming).feature)
        val cancelled = HubErrors.toException("c.R", ErrorCode.CANCELLED, "max stream duration (600000 ms) exceeded", ByteArray(0))
        assertEquals("max stream duration (600000 ms) exceeded", assertIs<StreamCancelledException>(cancelled).message)
        val other = HubErrors.toException("c.R", ErrorCode.ERROR_CODE_UNSPECIFIED, "odd", ByteArray(0))
        assertEquals("ERROR_CODE_UNSPECIFIED", assertIs<RemoteHandlerException>(other).code)
        assertEquals("HANDLER_ERROR", assertIs<RemoteHandlerException>(HubErrors.toException("c.R", ErrorCode.HANDLER_ERROR, "x", ByteArray(0))).code)
    }
}
