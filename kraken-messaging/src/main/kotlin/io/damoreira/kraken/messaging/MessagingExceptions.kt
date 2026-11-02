package io.damoreira.kraken.messaging

/** Root of all exceptions raised by kraken-messaging. */
sealed class MessagingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)

/** The hub has no connected handler for the message type (hub error code NO_HANDLER). */
class NoHandlerException(val type: String) : MessagingException("No handler registered for '$type'")

/** The reply did not arrive within the configured timeout (hub error code TIMEOUT). */
class MessageTimeoutException(val type: String) : MessagingException("Timed out waiting for reply to '$type'")

/** The handling instance disconnected mid-flight (hub error code HANDLER_DISCONNECTED). */
class HandlerDisconnectedException(val type: String) : MessagingException("Handler for '$type' disconnected")

/**
 * The remote handler failed; [details] carries the serialized error payload. [code] is the hub's error code name:
 * `HANDLER_ERROR` for a handler exception, another code for a scatter-gather part that failed on the hub's side (for
 * example `HANDLER_DISCONNECTED`), or for a code this library does not map to its own exception.
 */
class RemoteHandlerException(
    val type: String,
    message: String,
    val details: ByteArray,
    val code: String = "HANDLER_ERROR",
) : MessagingException(message)

/** The hub rejected the caller's credentials. */
class UnauthenticatedException(message: String) : MessagingException(message)

/** A message could not be encoded, decoded or validated. */
class InvalidMessageException(message: String, cause: Throwable? = null) : MessagingException(message, cause)

/** The caller is authenticated but the handler's `@RequiresAuthentication` expression denied it (hub code PERMISSION_DENIED). */
class AccessDeniedException(message: String) : MessagingException(message)

/**
 * The request was never written to a live hub stream: no stream came up in time, or the stream closed before the
 * request could be queued on it. The handler did not run; retrying is safe.
 */
class HubUnavailableException(message: String) : MessagingException(message)

/**
 * The request was written to a hub stream (accepted by its outbound queue) and that stream broke before an answer
 * arrived. The outcome is unknown: the handler may or may not have run, as with a `TIMEOUT`.
 */
class HubConnectionLostException(message: String) : MessagingException(message)

/**
 * The handling instance's hub queue or its session buffer is full (hub error code RESOURCE_EXHAUSTED, flow control):
 * the request was rejected before any handler ran. Retrying later is safe. For a subscription query [detail] is the hub's
 * (or the handler instance's) own reason: the subscriber's buffer overflowed, or an instance's subscription limit was
 * reached.
 */
class HandlerBusyException(val type: String, detail: String? = null) :
    MessagingException("Handler for '$type' is busy: ${detail ?: "its hub queue or session buffer is full"}")

/** Two handlers (`Bean.method` strings) claim the same single-handler message type. */
class DuplicateHandlerException(val type: String, val existing: String, val duplicate: String) :
    MessagingException("Duplicate handler for '$type': $existing and $duplicate")

/**
 * The connected hub does not offer [feature] (for example `scatter-gather`): its `RegisterAck` does not list the
 * capability, or it is a legacy hub. Nothing was sent; retrying against the same hub fails the same way.
 */
class UnsupportedByHubException(val feature: String, message: String) : MessagingException(message)

/** The hub refused this instance's handler registration. */
class HubRegistrationRejectedException(message: String) : MessagingException(message)

/**
 * The hub cancelled a streaming query (hub error code CANCELLED): it outlived `KRAKEN_HUB_MAX_STREAM_DURATION`. The
 * parts emitted before stay emitted; never retried.
 */
class StreamCancelledException(message: String) : MessagingException(message)
