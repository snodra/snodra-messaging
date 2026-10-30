package io.damoreira.kraken.examples.a

import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.NoHandlerException
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

    @Test fun `timeout is 504`() {
        assertEquals(HttpStatus.GATEWAY_TIMEOUT, advice.handle(MessageTimeoutException("x")).statusCode)
    }

    @Test fun `any other messaging failure is 502`() {
        val response = advice.handle(HubUnavailableException("down"))
        assertEquals(HttpStatus.BAD_GATEWAY, response.statusCode)
        assertEquals("HubUnavailableException", response.body?.error)
    }
}
