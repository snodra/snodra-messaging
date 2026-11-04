package io.damoreira.kraken.messaging.hub

import io.damoreira.kraken.hub.v1.ErrorCode
import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.MessagingException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.UnauthenticatedException

/**
 * Maps hub [ErrorCode]s to the exceptions thrown to callers of the buses. [subscription] is true for the answers of a
 * subscription query, whose `RESOURCE_EXHAUSTED` says why (a subscriber buffer overflow, an instance's subscription limit).
 */
object HubErrors {
    fun toException(
        type: String,
        code: ErrorCode,
        message: String,
        details: ByteArray,
        subscription: Boolean = false,
    ): MessagingException = when (code) {
        ErrorCode.NO_HANDLER -> NoHandlerException(type)
        ErrorCode.TIMEOUT -> MessageTimeoutException(type)
        ErrorCode.HANDLER_DISCONNECTED -> HandlerDisconnectedException(type)
        ErrorCode.HANDLER_ERROR -> RemoteHandlerException(type, message, details)
        ErrorCode.UNAUTHENTICATED -> UnauthenticatedException(message)
        ErrorCode.INVALID_REQUEST -> InvalidMessageException(message)
        ErrorCode.PERMISSION_DENIED -> AccessDeniedException(message)
        // A subscription's RESOURCE_EXHAUSTED says why (a subscriber buffer overflow, an instance's subscription limit).
        ErrorCode.RESOURCE_EXHAUSTED -> HandlerBusyException(type, message.takeIf { subscription && it.isNotEmpty() })
        // A streaming query or subscription the hub ended for outliving its maximum duration (P4b, P4c).
        ErrorCode.CANCELLED -> StreamCancelledException(message.ifEmpty { "cancelled by the hub" })
        else -> RemoteHandlerException(type, message, details, code.name)
    }
}
