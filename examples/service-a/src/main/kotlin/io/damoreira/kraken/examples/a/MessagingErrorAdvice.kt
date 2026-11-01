package io.damoreira.kraken.examples.a

import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.MessagingException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.UnsupportedByHubException
import org.springframework.http.HttpStatus
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

data class ErrorBody(val error: String, val message: String)

/**
 * Maps hub failures to HTTP: nobody owns the type (503), the owner vanished (503), the owner's hub queue is full (503),
 * no reply in time (504), a hub without the feature (scatter-gather or streaming on an older hub: 501), a stream the hub
 * ended for outliving its maximum duration (502: the upstream ended it, the caller did not), else 502.
 */
@RestControllerAdvice
class MessagingErrorAdvice {
    @ExceptionHandler(MessagingException::class)
    fun handle(e: MessagingException): ResponseEntity<ErrorBody> {
        val status = when (e) {
            is NoHandlerException, is HandlerDisconnectedException, is HandlerBusyException -> HttpStatus.SERVICE_UNAVAILABLE
            is MessageTimeoutException -> HttpStatus.GATEWAY_TIMEOUT
            is UnsupportedByHubException -> HttpStatus.NOT_IMPLEMENTED
            is StreamCancelledException -> HttpStatus.BAD_GATEWAY
            else -> HttpStatus.BAD_GATEWAY
        }
        return ResponseEntity.status(status).body(ErrorBody(e::class.simpleName ?: "MessagingException", e.message ?: ""))
    }
}
