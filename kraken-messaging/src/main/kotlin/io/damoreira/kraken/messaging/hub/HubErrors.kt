package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.MessagingException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.UnauthenticatedException

/** Maps hub [ErrorCode]s to the exceptions thrown to callers of the buses. */
object HubErrors {
    fun toException(type: String, code: ErrorCode, message: String, details: ByteArray): MessagingException = when (code) {
        ErrorCode.NO_HANDLER -> NoHandlerException(type)
        ErrorCode.TIMEOUT -> MessageTimeoutException(type)
        ErrorCode.HANDLER_DISCONNECTED -> HandlerDisconnectedException(type)
        ErrorCode.HANDLER_ERROR -> RemoteHandlerException(type, message, details)
        ErrorCode.UNAUTHENTICATED -> UnauthenticatedException(message)
        ErrorCode.INVALID_REQUEST -> InvalidMessageException(message)
        ErrorCode.PERMISSION_DENIED -> AccessDeniedException(message)
        else -> RemoteHandlerException(type, message, details)
    }
}
