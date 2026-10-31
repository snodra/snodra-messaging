package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.UnauthenticatedException
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
    }
}
