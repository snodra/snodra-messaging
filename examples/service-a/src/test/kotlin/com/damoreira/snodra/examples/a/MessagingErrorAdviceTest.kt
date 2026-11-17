package com.damoreira.snodra.examples.a

import com.damoreira.snodra.messaging.AccessDeniedException
import com.damoreira.snodra.messaging.HandlerBusyException
import com.damoreira.snodra.messaging.HandlerDisconnectedException
import com.damoreira.snodra.messaging.HubUnavailableException
import com.damoreira.snodra.messaging.MessageTimeoutException
import com.damoreira.snodra.messaging.NoHandlerException
import com.damoreira.snodra.messaging.StreamCancelledException
import org.springframework.http.HttpStatus
import kotlin.test.Test
import kotlin.test.assertEquals

class MessagingErrorAdviceTest {
    private val advice = MessagingErrorAdvice()

    @Test fun `no handler is 503 with the exception name and message`() {
        val response = advice.handle(NoHandlerException("examples.SendGreeting"))
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, response.statusCode)
        assertEquals(ErrorBody("NoHandlerException", "No handler registered for 'examples.SendGreeting'"), response.body)
    }

    @Test fun `handler disconnected is 503`() {
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, advice.handle(HandlerDisconnectedException("x")).statusCode)
    }

    @Test fun `busy handler is 503`() {
        assertEquals(HttpStatus.SERVICE_UNAVAILABLE, advice.handle(HandlerBusyException("x")).statusCode)
    }

    @Test fun `timeout is 504`() {
        assertEquals(HttpStatus.GATEWAY_TIMEOUT, advice.handle(MessageTimeoutException("x")).statusCode)
    }

    @Test fun `a stream the hub cancelled is 502`() {
        val response = advice.handle(StreamCancelledException("max stream duration (10m) exceeded"))
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode)
        assertEquals(ErrorBody("StreamCancelledException", "max stream duration (10m) exceeded"), response.body)
    }

    @Test fun `a request the hub's access control refuses is 403`() {
        val response = advice.handle(AccessDeniedException("type examples.ForbiddenProbe is not allowed to send"))
        assertEquals(HttpStatus.FORBIDDEN, response.statusCode)
        assertEquals(ErrorBody("AccessDeniedException", "type examples.ForbiddenProbe is not allowed to send"), response.body)
    }

    @Test fun `any other messaging failure is 502`() {
        val response = advice.handle(HubUnavailableException("down"))
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode)
        assertEquals("HubUnavailableException", response.body?.error)
    }
}
