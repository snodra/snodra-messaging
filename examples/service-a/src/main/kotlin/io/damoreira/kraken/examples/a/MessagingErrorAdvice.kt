package io.damoreira.kraken.examples.a

import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.MessagingException
import io.damoreira.kraken.messaging.NoHandlerException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

data class ErrorBody(val error: String, val message: String)

/** Maps hub failures to HTTP: nobody owns the type (503), the owner vanished (503), no reply in time (504), else 502. */
@RestControllerAdvice
class MessagingErrorAdvice {
    @ExceptionHandler(MessagingException::class)
    fun handle(e: MessagingException): ResponseEntity<ErrorBody> {
        val status = when (e) {
            is NoHandlerException, is HandlerDisconnectedException -> HttpStatus.SERVICE_UNAVAILABLE
            is MessageTimeoutException -> HttpStatus.GATEWAY_TIMEOUT
            else -> HttpStatus.BAD_GATEWAY
        }
        return ResponseEntity.status(status).body(ErrorBody(e::class.simpleName ?: "MessagingException", e.message ?: ""))
    }
}
