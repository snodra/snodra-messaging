package io.damoreira.kraken.messaging.hub

import com.google.protobuf.ByteString
import io.damoreira.kraken.hub.v1.Request
import io.damoreira.kraken.hub.v1.RequestKind
import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.codec.JsonCodec
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.codec.ResponseTypes
import io.damoreira.kraken.messaging.handler.HandlerInvoker
import io.damoreira.kraken.messaging.handler.HandlerRegistry
import io.damoreira.kraken.messaging.handler.InvocationResult
import io.damoreira.kraken.messaging.security.CallerContext
import io.damoreira.kraken.messaging.tracing.TraceHeaders
import org.slf4j.MDC
import java.time.Duration
import java.util.UUID
import kotlin.reflect.KClass

/**
 * [CommandBus] and [QueryBus] over the hub. A message whose handler lives in this instance is invoked directly (same
 * codec round trip and error mapping, no network); anything else becomes a hub `Request` carrying the caller's bearer
 * token (when [forwardJwt]), a correlation id and the trace context.
 */
class HubBus(
    private val connection: HubConnection,
    private val registry: HandlerRegistry,
    private val invoker: HandlerInvoker,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    private val forwardJwt: Boolean,
    private val traceHeaders: TraceHeaders,
    private val requestTimeout: Duration,
) : CommandBus, QueryBus {

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(command: Command<R>, routingKey: String?): R =
        dispatch(command, RequestKind.COMMAND, routingKey) as R

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(query: Query<R>): R =
        dispatch(query, RequestKind.QUERY, null) as R

    private suspend fun dispatch(message: Any, kind: RequestKind, routingKey: String?): Any {
        val type = typeResolver.typeNameOf(message::class)
        val local = if (kind == RequestKind.COMMAND) registry.commandHandler(type) else registry.queryHandler(type)
        val payload = codec.encode(message)
        val correlationId = MDC.get("correlationId") ?: UUID.randomUUID().toString()
        val token = CallerContext.currentBearerToken()
        if (local != null) return invokeLocally(type, kind, message::class, payload, correlationId, token)

        val headers = mutableMapOf("correlationId" to correlationId)
        if (forwardJwt && token != null) headers["authorization"] = "Bearer $token"
        traceHeaders.inject(headers)
        val request = Request.newBuilder()
            .setId(UUID.randomUUID().toString())
            .setKind(kind)
            .setType(type)
            .setRoutingKey(routingKey.orEmpty())
            .setPayload(ByteString.copyFrom(payload))
            .putAllHeaders(headers)
            .setTimeoutMs(requestTimeout.toMillis().coerceIn(0, Int.MAX_VALUE.toLong()).toInt())
            .build()

        val answer = connection.send(request)
        return when {
            answer.hasReply() && answer.reply.hasError() -> throw answer.reply.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
            }
            answer.hasReply() -> decodeReply(type, message::class, answer.reply.payload.toByteArray())
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
        val ctx = MessageContext(
            messageId = UUID.randomUUID().toString(),
            type = type,
            correlationId = correlationId,
            userId = CallerContext.currentUserId(),
            roles = CallerContext.currentRoles(),
            authorization = token?.let { "Bearer $it" },
            headers = mapOf("correlationId" to correlationId),
        )
        val result = if (kind == RequestKind.COMMAND) invoker.invokeCommand(type, payload, ctx) else invoker.invokeQuery(type, payload, ctx)
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
