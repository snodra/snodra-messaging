package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.ReplyPart
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.MessageKind
import io.damoreira.kraken.messaging.MessagingException
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.RetryClassification
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.ScatterOptions
import io.damoreira.kraken.messaging.ScatterPart
import io.damoreira.kraken.messaging.ScatterResult
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.StreamingQuery
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.codec.ResponseTypes
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.security.CallerContext
import io.damoreira.kraken.messaging.security.LocalIdentity
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import io.damoreira.kraken.messaging.tracing.CorrelationContext
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.UUID
import kotlin.reflect.KClass

/**
 * [CommandBus] and [QueryBus] over the hub. A message whose handler lives in this instance is invoked directly (same
 * codec round trip and error mapping, no network); anything else becomes a hub `Request` carrying the caller's bearer
 * token (when [forwardJwt]), a correlation id and the trace context. A query on a type this service registered as
 * **shared** (`@QueryHandler(shared = true)`) is the exception: it always goes through the hub, which round-robins it
 * over every owner's instances, this one included (ADR 0017).
 *
 * Per call [SendOptions] choose the priority (default [priorities] per type, else 0), the timeout (default
 * [requestTimeout]) and the retry policy (default [retry], null = no retry). With retry, failures that
 * [RetryClassification] allows are re-sent as new requests (fresh id, same correlation id) after the policy's jittered
 * backoff ([RetryPolicy.jitteredBackoffBefore]), all attempts within one budget: the chosen timeout, else the hub's
 * deadline for the type ([HubTransport.hubTimeoutFor]). Each attempt sends the remaining budget as `timeout_ms` (at least 1 ms). A retry is
 * started only if, after its backoff, at least `max(initialBackoff, RetryPolicy.MIN_ATTEMPT_BUDGET)` of the budget
 * is left; otherwise the last error is thrown at once, without waiting. Local handlers never retry.
 *
 * Scatter-gather ([scatterGather], [scatterGatherFlow], ADR 0017) always goes through the hub, also when this instance
 * owns the type. Each part is decoded like a unary reply (the query's declared result type); a part's error becomes a
 * [RemoteHandlerException] carrying the hub's code; a part whose payload does not decode is a failed part too (code
 * `ERROR_CODE_UNSPECIFIED`, the decode error as message), never a failure of the whole gather. The hub's `Error` for the gather itself maps as for unary requests
 * (`NO_HANDLER` -> [io.damoreira.kraken.messaging.NoHandlerException], `INVALID_REQUEST` -> [InvalidMessageException],
 * `UNSUPPORTED` -> [io.damoreira.kraken.messaging.UnsupportedByHubException]). Retry applies to the whole gather with
 * the query rules: a retried gather is a new gather (fresh id) and its result holds only its own parts. A flow is
 * retried only while it has not emitted a part. [scatterGatherFlow] reads the caller's bearer token and trace context
 * when it is called, not when the flow is collected: a flow returned from a controller is collected on another thread.
 *
 * Streaming queries ([stream], P4b, ADR 0017) always go through the hub too. The flow is cold and unbuffered: each
 * collection sends one `STREAMING` request to a hub that lists `streaming`, and each part is decoded (the query's
 * element type) and emitted before the next one is read, so the transport's credit refills ([HubTransport.stream])
 * follow what the collector consumed. Unlike a unary attempt, every attempt of a stream sends the chosen timeout
 * (`StreamOptions.timeout`, else [requestTimeout]) whole as `timeout_ms`: it is the stream's idle timeout, and the
 * retry budget bounds only the attempts before the first part. A `ReplyEnd` with an error fails the flow exactly like a unary `Error` with the
 * same code ([HubErrors]: `TIMEOUT` -> [io.damoreira.kraken.messaging.MessageTimeoutException], `HANDLER_DISCONNECTED`
 * -> [io.damoreira.kraken.messaging.HandlerDisconnectedException], `HANDLER_ERROR` -> [RemoteHandlerException],
 * `CANCELLED` -> [io.damoreira.kraken.messaging.StreamCancelledException], ...), and so does an `Error` before any
 * part; a part that does not decode fails the flow with [InvalidMessageException] (the transport cancels the stream).
 * An exception thrown by the collector (it runs inside the transport's part callback) is rethrown as it is, also when
 * it is a `HubUnavailableException` or `HubConnectionLostException` of a nested send: the transport reports it as a
 * [StreamCollectorException], so it never counts as this stream's transport failing (no failover to another hub; the
 * stream is cancelled). Retry applies with the query rules only until the first part, to ends with an error as to
 * errors (a retried stream is a new request); after the first part nothing is retried: the flow throws, a broken hub
 * stream as [io.damoreira.kraken.messaging.HubConnectionLostException]. The caller's bearer token and trace context
 * are read when [stream] is called, as for [scatterGatherFlow].
 */
class HubBus(
    private val connection: HubTransport,
    private val registry: HandlerRegistry,
    private val invoker: HandlerInvoker,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val forwardJwt: Boolean,
    private val traceHeaders: TraceHeaders,
    /** Sent as `timeout_ms` when set; null sends 0 so the hub's default or per-type override applies (see [HubConnection.send]). */
    private val requestTimeout: Duration?,
    /** Default `Request.priority` per type name (`kraken.messaging.hub.priorities`); types not listed send 0. */
    private val priorities: Map<String, Int> = emptyMap(),
    /** Default retry policy (`kraken.messaging.hub.retry.*` when enabled); null = no retry. */
    private val retry: RetryPolicy? = null,
) : CommandBus, QueryBus {

    override suspend fun <R> send(command: Command<R>, routingKey: String?): R = send(command, SendOptions(routingKey = routingKey))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(command: Command<R>, options: SendOptions): R =
        dispatch(command, RequestKind.COMMAND, options) as R

    override suspend fun <R> send(query: Query<R>): R = send(query, SendOptions())

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(query: Query<R>, options: SendOptions): R =
        dispatch(query, RequestKind.QUERY, options) as R

    override suspend fun <R> scatterGather(query: Query<R>, options: ScatterOptions): ScatterResult<R> {
        val template = template(query, RequestKind.QUERY, routingKey = null, options.priority).toBuilder()
            .setMaxResults(options.maxResults)
            .build()
        return withRetry(template.type, MessageKind.QUERY, options.timeout, options.retry, retryIf = { true }) { timeout ->
            val parts = mutableListOf<ScatterPart<R>>()
            val end = gatherOnce(template, query::class, timeout) { parts += it }
            // Failed = parts whose outcome is an error, undecodable payloads included (may exceed ReplyEnd.failed).
            ScatterResult(parts.toList(), parts.count { it.error != null }, end.timedOut)
        }
    }

    override fun <R> scatterGatherFlow(query: Query<R>, options: ScatterOptions): Flow<ScatterPart<R>> {
        // Read here, on the caller's thread: a flow returned from a controller is collected later on another thread,
        // where the SecurityContextHolder (bearer token) and the current trace of the request are gone.
        val caller = callerHeaders()
        return scatterGatherFlow(query, options, caller)
    }

    private fun <R> scatterGatherFlow(query: Query<R>, options: ScatterOptions, caller: Map<String, String>): Flow<ScatterPart<R>> = channelFlow {
        val template = template(query, RequestKind.QUERY, routingKey = null, options.priority, caller).toBuilder()
            .setMaxResults(options.maxResults)
            .build()
        var emitted = false
        // Parts already emitted cannot be taken back: once one is out, a failure ends the flow.
        withRetry(template.type, MessageKind.QUERY, options.timeout, options.retry, retryIf = { !emitted }) { timeout ->
            gatherOnce<R>(template, query::class, timeout) { part ->
                emitted = true
                send(part)
            }
        }
    }

    override fun <R> stream(query: StreamingQuery<R>, options: StreamOptions): Flow<R> {
        val caller = callerHeaders() // on the caller's thread, see scatterGatherFlow
        return stream(query, options, caller)
    }

    private fun <R> stream(query: StreamingQuery<R>, options: StreamOptions, caller: Map<String, String>): Flow<R> = flow {
        val template = template(query, RequestKind.QUERY, routingKey = null, options.priority, caller).toBuilder()
            .setMaxResults(options.maxResults)
            .setInitialCredit(options.initialCredit)
            .build()
        var emitted = false
        // The idle timeout is the stream's own, sent whole on every attempt: the retry budget bounds only the attempts
        // (and, in the multi-hub transport, the wait for a hub), never how long the handler may take between parts.
        val idleTimeout = chosenTimeout(options.timeout)
        // Parts already emitted cannot be taken back: once one is out, a failure ends the flow.
        withRetry(template.type, MessageKind.QUERY, options.timeout, options.retry, retryIf = { !emitted }) { _ ->
            streamOnce<R>(template, query::class, idleTimeout) { part ->
                emitted = true
                emit(part)
            }
        }
    }

    /**
     * One streaming query built from [template] with a fresh id and [timeout] as `timeout_ms` (the idle timeout, whole,
     * not what is left of a retry budget); hands each decoded part to [onPart] and returns when the `ReplyEnd` arrived
     * without an error.
     */
    private suspend fun <R> streamOnce(
        template: Request,
        messageClass: KClass<*>,
        timeout: Duration?,
        onPart: suspend (R) -> Unit,
    ) {
        val type = template.type
        val request = withFreshId(template, timeout)
        val terminal = try {
            @Suppress("UNCHECKED_CAST")
            connection.stream(request) { part ->
                // The hub ends a stream instead of relaying an error part; one would be a hub that does it differently.
                if (part.hasError()) throw RemoteHandlerException(type, part.error.message, part.error.details.toByteArray(), part.error.code.name)
                onPart(decodeReply(type, messageClass, part.payload.toByteArray()) as R)
            }
        } catch (e: StreamCollectorException) {
            // The collector's (or the decoder's) own exception, as thrown: the transport wrapped it so that a transport
            // failure inside the collector (a nested send) is never taken for this stream's hub failing.
            throw e.cause!!
        }
        when {
            // The end's error is the same failure a unary send would see as an Error with that code.
            terminal.hasEnd() && terminal.end.hasError() -> throw terminal.end.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray(), HubProtocol.STREAMING)
            }
            terminal.hasEnd() -> return
            terminal.hasError() -> throw terminal.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray(), HubProtocol.STREAMING)
            }
            else -> throw InvalidMessageException("Unexpected answer to streaming query $type")
        }
    }

    /**
     * One scatter-gather built from [template] with a fresh id and [timeout] as `timeout_ms`; hands each decoded part to
     * [onPart] and returns the `ReplyEnd`.
     */
    private suspend fun <R> gatherOnce(
        template: Request,
        messageClass: KClass<*>,
        timeout: Duration?,
        onPart: suspend (ScatterPart<R>) -> Unit,
    ): io.damoreira.kraken.hub.v1.ReplyEnd {
        val type = template.type
        val request = withFreshId(template, timeout).toBuilder()
            .setMode(io.damoreira.kraken.hub.v1.RequestMode.SCATTER_GATHER)
            .build()
        val terminal = connection.gather(request) { part -> onPart(decodePart(type, messageClass, part)) }
        return when {
            terminal.hasEnd() && terminal.end.hasError() -> throw terminal.end.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
            }
            terminal.hasEnd() -> terminal.end
            terminal.hasError() -> throw terminal.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
            }
            else -> throw InvalidMessageException("Unexpected answer to scatter-gather of $type")
        }
    }

    @Suppress("UNCHECKED_CAST")
    private fun <R> decodePart(type: String, messageClass: KClass<*>, part: ReplyPart): ScatterPart<R> {
        if (part.hasError()) {
            val error = part.error
            val exception = RemoteHandlerException(type, error.message, error.details.toByteArray(), error.code.name)
            return ScatterPart(part.responderService, part.responderInstance, null, exception)
        }
        return try {
            val result = decodeReply(type, messageClass, part.payload.toByteArray()) as R
            ScatterPart(part.responderService, part.responderInstance, result, null)
        } catch (e: InvalidMessageException) {
            // One owner answering in another shape fails its own part, not the gather (same rule as the Rust library).
            val message = listOfNotNull(e.message, e.cause?.message).joinToString(": ")
            val exception = RemoteHandlerException(type, message, ByteArray(0), io.damoreira.kraken.hub.v1.ErrorCode.ERROR_CODE_UNSPECIFIED.name)
            ScatterPart(part.responderService, part.responderInstance, null, exception)
        }
    }

    private suspend fun dispatch(message: Any, kind: RequestKind, options: SendOptions): Any {
        val type = typeResolver.typeNameOf(message::class)
        // A shared query type is never answered locally: the hub round-robins a point query over every owner's
        // instances (ADR 0017), this one included, so an owner is not favoured over the others (as in the Rust library).
        val local = when (kind) {
            RequestKind.COMMAND -> registry.commandHandler(type)
            else -> registry.queryHandler(type)?.takeUnless { it.shared }
        }
        if (local != null) {
            val correlationId = CorrelationContext.resolve(currentCoroutineContext())
            val token = CallerContext.currentBearerToken()
            return invokeLocally(type, kind, message::class, codec.encode(message), correlationId, token)
        }
        val template = template(message, kind, options.routingKey, options.priority)
        val messageKind = if (kind == RequestKind.COMMAND) MessageKind.COMMAND else MessageKind.QUERY
        return withRetry(type, messageKind, options.timeout, options.retry, retryIf = { true }) { timeout ->
            attempt(template, message::class, timeout)
        }
    }

    /**
     * The headers bound to the calling thread: the caller's bearer token (when [forwardJwt]) and the trace context.
     * Not suspending on purpose: read them before any suspension, on the thread that holds the security context.
     */
    private fun callerHeaders(): Map<String, String> {
        val headers = mutableMapOf<String, String>()
        val token = CallerContext.currentBearerToken()
        if (forwardJwt && token != null) headers["authorization"] = "Bearer $token"
        traceHeaders.inject(headers)
        return headers
    }

    /**
     * The hub request for [message] without id and timeout: payload, correlation id, [caller] headers (bearer token and
     * trace, see [callerHeaders]), priority. The correlation id comes from the coroutine context, so it is resolved here.
     */
    private suspend fun template(
        message: Any,
        kind: RequestKind,
        routingKey: String?,
        priority: Int?,
        caller: Map<String, String> = callerHeaders(),
    ): Request {
        val type = typeResolver.typeNameOf(message::class)
        val payload = codec.encode(message)
        val correlationId = CorrelationContext.resolve(currentCoroutineContext())
        val headers = mutableMapOf("correlationId" to correlationId)
        headers += caller
        return Request.newBuilder()
            .setKind(kind)
            .setType(type)
            .setRoutingKey(routingKey.orEmpty())
            .setPayload(ByteString.copyFrom(payload))
            .putAllHeaders(headers)
            .setPriority(priority ?: priorities[type] ?: 0)
            .build()
    }

    /**
     * Runs [attempt] with the timeout and retry rules of the class comment: [timeout] (else [requestTimeout]) as the
     * budget, [policy] (else [retry]) for the attempts. A failure is retried only when [RetryClassification] allows it
     * for [kind] and [retryIf] still holds. [attempt] gets the `timeout_ms` to send (null = unset, the hub decides).
     */
    private suspend fun <T> withRetry(
        type: String,
        kind: MessageKind,
        timeout: Duration?,
        policy: RetryPolicy?,
        retryIf: () -> Boolean,
        attempt: suspend (Duration?) -> T,
    ): T {
        val chosen = chosenTimeout(timeout)
        val effective = (policy ?: retry)?.takeIf { it.maxAttempts > 1 }
            ?: return attempt(chosen) // unset: 0 on the wire, the hub decides

        val budget = chosen ?: connection.hubTimeoutFor(type)
        val deadline = System.nanoTime() + budget.toNanos()
        var attemptNo = 1
        while (true) {
            val remaining = Duration.ofNanos(deadline - System.nanoTime())
            try {
                return attempt(remaining.coerceAtLeast(Duration.ofMillis(1)))
            } catch (e: MessagingException) {
                if (attemptNo >= effective.maxAttempts || !RetryClassification.isRetryable(kind, e) || !retryIf()) throw e
                val wait = effective.jitteredBackoffBefore(attemptNo + 1)
                // Too little of the budget left after the wait for a useful attempt: give up with this error now.
                val leftAfterWait = deadline - System.nanoTime() - wait.toNanos()
                if (leftAfterWait < maxOf(effective.initialBackoff, RetryPolicy.MIN_ATTEMPT_BUDGET).toNanos()) throw e
                delay(wait.toMillis())
                attemptNo++
            }
        }
    }

    /** The caller's [timeout], else [requestTimeout]; null (unset: the hub decides) when neither is positive. */
    private fun chosenTimeout(timeout: Duration?): Duration? = (timeout ?: requestTimeout)?.takeIf { !it.isZero && !it.isNegative }

    /** [template] with a fresh id and [timeout] as `timeout_ms` (null sends 0). */
    private fun withFreshId(template: Request, timeout: Duration?): Request = template.toBuilder()
        .setId(UUID.randomUUID().toString())
        // Unset: 0 on the wire, the hub decides (a legacy hub gets the 5 s default instead).
        .setTimeoutMs(timeout?.toMillis()?.coerceIn(0, Int.MAX_VALUE.toLong())?.toInt() ?: 0)
        .build()

    /** One hub request built from [template] with a fresh id and [timeout] as `timeout_ms` (null sends 0). */
    private suspend fun attempt(template: Request, messageClass: KClass<*>, timeout: Duration?): Any {
        val type = template.type
        val answer = connection.send(withFreshId(template, timeout))
        return when {
            answer.hasReply() && answer.reply.hasError() -> throw answer.reply.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
            }
            answer.hasReply() -> decodeReply(type, messageClass, answer.reply.payload.toByteArray())
            answer.hasError() -> throw answer.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
            }
            else -> throw InvalidMessageException("Unexpected answer to $type")
        }
    }

    private suspend fun invokeLocally(
        type: String,
        kind: RequestKind,
        messageClass: KClass<*>,
        payload: ByteArray,
        correlationId: String,
        token: String?,
    ): Any {
        val identity = LocalIdentity.current()
        val ctx = MessageContext(
            messageId = UUID.randomUUID().toString(),
            type = type,
            correlationId = correlationId,
            userId = identity?.userId,
            roles = identity?.roles ?: emptySet(),
            authorization = token?.let { "Bearer $it" },
            headers = mapOf("correlationId" to correlationId),
        )
        // Same thread and coroutine as the caller (so a thread-bound transaction of the caller is joined).
        val result = withContext(CorrelationContext(correlationId)) {
            if (kind == RequestKind.COMMAND) invoker.invokeCommand(type, payload, ctx, identity) else invoker.invokeQuery(type, payload, ctx, identity)
        }
        return when (result) {
            is InvocationResult.Success -> decodeReply(type, messageClass, result.payload)
            is InvocationResult.Failure -> throw HubErrors.toException(type, result.code, result.message, result.details)
        }
    }

    private fun decodeReply(type: String, messageClass: KClass<*>, payload: ByteArray): Any {
        val responseType = ResponseTypes.responseTypeOf(messageClass)
        return try {
            codec.decode(payload, responseType)
        } catch (e: InvalidMessageException) {
            throw InvalidMessageException("Reply to $type did not match ${responseType.qualifiedName}", e)
        }
    }
}
