package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.MessageKind
import io.damoreira.kraken.messaging.MessagingException
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.RetryClassification
import io.damoreira.kraken.messaging.RetryPolicy
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
 * [RetryClassification] allows are re-sent as new requests (fresh id, same correlation id) after the policy's backoff,
 * all attempts within one budget: the chosen timeout, else the hub's deadline for the type
 * ([HubConnection.hubTimeoutFor]). Each attempt sends the remaining budget as `timeout_ms` (at least 1 ms). A retry is
 * started only if, after its backoff, at least `max(initialBackoff, RetryPolicy.MIN_ATTEMPT_BUDGET)` of the budget
 * is left; otherwise the last error is thrown at once, without waiting. Local handlers never retry.
 */
class HubBus(
    private val connection: HubConnection,
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

    private suspend fun dispatch(message: Any, kind: RequestKind, options: SendOptions): Any {
        val type = typeResolver.typeNameOf(message::class)
        val local = if (kind == RequestKind.COMMAND) registry.commandHandler(type) else registry.queryHandler(type)
        val payload = codec.encode(message)
        val correlationId = CorrelationContext.resolve(currentCoroutineContext())
        val token = CallerContext.currentBearerToken()
        if (local != null) return invokeLocally(type, kind, message::class, payload, correlationId, token)

        val headers = mutableMapOf("correlationId" to correlationId)
        if (forwardJwt && token != null) headers["authorization"] = "Bearer $token"
        traceHeaders.inject(headers)
        val template = Request.newBuilder()
            .setKind(kind)
            .setType(type)
            .setRoutingKey(options.routingKey.orEmpty())
            .setPayload(ByteString.copyFrom(payload))
            .putAllHeaders(headers)
            .setPriority(options.priority ?: priorities[type] ?: 0)
            .build()
        val chosen = (options.timeout ?: requestTimeout)?.takeIf { !it.isZero && !it.isNegative }
        val policy = (options.retry ?: retry)?.takeIf { it.maxAttempts > 1 }
            ?: return attempt(template, message::class, chosen) // unset: 0 on the wire, the hub decides

        val messageKind = if (kind == RequestKind.COMMAND) MessageKind.COMMAND else MessageKind.QUERY
        val budget = chosen ?: connection.hubTimeoutFor(type)
        val deadline = System.nanoTime() + budget.toNanos()
        var attemptNo = 1
        while (true) {
            val remaining = Duration.ofNanos(deadline - System.nanoTime())
            try {
                return attempt(template, message::class, remaining.coerceAtLeast(Duration.ofMillis(1)))
            } catch (e: MessagingException) {
                if (attemptNo >= policy.maxAttempts || !RetryClassification.isRetryable(messageKind, e)) throw e
                val wait = policy.backoffBefore(attemptNo + 1)
                // Too little of the budget left after the wait for a useful attempt: give up with this error now.
                val leftAfterWait = deadline - System.nanoTime() - wait.toNanos()
                if (leftAfterWait < maxOf(policy.initialBackoff, RetryPolicy.MIN_ATTEMPT_BUDGET).toNanos()) throw e
                delay(wait.toMillis())
                attemptNo++
            }
        }
    }

    /** One hub request built from [template] with a fresh id and [timeout] as `timeout_ms` (null sends 0). */
    private suspend fun attempt(template: Request, messageClass: KClass<*>, timeout: Duration?): Any {
        val type = template.type
        val request = template.toBuilder()
            .setId(UUID.randomUUID().toString())
            // Unset: 0 on the wire, the hub decides (a legacy hub gets the 5 s default instead).
            .setTimeoutMs(timeout?.toMillis()?.coerceIn(0, Int.MAX_VALUE.toLong())?.toInt() ?: 0)
            .build()
        val answer = connection.send(request)
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
