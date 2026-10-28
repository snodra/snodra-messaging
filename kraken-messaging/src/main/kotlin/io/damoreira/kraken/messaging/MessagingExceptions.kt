package io.damoreira.kraken.messaging

sealed class MessagingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** The hub has no connected handler for the message type (hub error code NO_HANDLER). */
class NoHandlerException(val type: String) : MessagingException("No handler registered for '$type'")

/** The reply did not arrive within the configured timeout (hub error code TIMEOUT). */
class ReplyTimeoutException(val type: String) : MessagingException("Timed out waiting for reply to '$type'")

/** The handling instance disconnected mid-flight (hub error code HANDLER_DISCONNECTED). */
class HandlerDisconnectedException(val type: String) : MessagingException("Handler for '$type' disconnected")
