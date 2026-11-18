package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.messaging.AccessDeniedException
import com.damoreira.snodra.messaging.HandlerBusyException
import com.damoreira.snodra.messaging.RemoteHandlerException
import com.damoreira.snodra.messaging.StreamCancelledException
import com.damoreira.snodra.messaging.UnauthenticatedException
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
    fun `a subscription's resource exhausted keeps the hub's reason`() {
        val e = HubErrors.toException("c.R", ErrorCode.RESOURCE_EXHAUSTED, "subscriber buffer (256) exceeded", ByteArray(0), subscription = true)
        assertIs<HandlerBusyException>(e)
        assertEquals("c.R", e.type)
        assertEquals("Handler for 'c.R' is busy: subscriber buffer (256) exceeded", e.message)
    }

    @Test
    fun `cancelled maps to StreamCancelledException, an unmapped code keeps its name`() {
        val cancelled = HubErrors.toException("c.R", ErrorCode.CANCELLED, "max stream duration (600000 ms) exceeded", ByteArray(0))
        assertEquals("max stream duration (600000 ms) exceeded", assertIs<StreamCancelledException>(cancelled).message)
        val other = HubErrors.toException("c.R", ErrorCode.ERROR_CODE_UNSPECIFIED, "odd", ByteArray(0))
        assertEquals("ERROR_CODE_UNSPECIFIED", assertIs<RemoteHandlerException>(other).code)
        assertEquals("HANDLER_ERROR", assertIs<RemoteHandlerException>(HubErrors.toException("c.R", ErrorCode.HANDLER_ERROR, "x", ByteArray(0))).code)
    }
}
