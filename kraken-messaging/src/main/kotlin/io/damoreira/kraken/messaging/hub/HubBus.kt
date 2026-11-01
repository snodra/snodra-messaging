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
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Query
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
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.UUID
import kotlin.reflect.KClass

/**
 * [CommandBus] and [QueryBus] over the hub. A message whose handler lives in this instance is invoked directly (same
 * codec round trip and error mapping, no network); anything else becomes a hub `Request` carrying the caller's bearer
 * token (when [forwardJwt]), a correlation id and the trace context.
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
 * retried only while it has not emitted a part.
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

    override fun <R> scatterGatherFlow(query: Query<R>, options: ScatterOptions): Flow<ScatterPart<R>> = channelFlow {
        val template = template(query, RequestKind.QUERY, routingKey = null, options.priority).toBuilder()
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
        val local = if (kind == RequestKind.COMMAND) registry.commandHandler(type) else registry.queryHandler(type)
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

    /** The hub request for [message] without id and timeout: payload, correlation id, bearer token, trace, priority. */
    private suspend fun template(message: Any, kind: RequestKind, routingKey: String?, priority: Int?): Request {
        val type = typeResolver.typeNameOf(message::class)
        val payload = codec.encode(message)
        val correlationId = CorrelationContext.resolve(currentCoroutineContext())
        val token = CallerContext.currentBearerToken()
        val headers = mutableMapOf("correlationId" to correlationId)
        if (forwardJwt && token != null) headers["authorization"] = "Bearer $token"
        traceHeaders.inject(headers)
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
        val chosen = (timeout ?: requestTimeout)?.takeIf { !it.isZero && !it.isNegative }
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
