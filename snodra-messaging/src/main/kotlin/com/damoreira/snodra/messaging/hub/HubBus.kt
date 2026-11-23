package com.damoreira.snodra.messaging.hub

import com.google.protobuf.ByteString
import com.damoreira.snodra.server.v1.ReplyPart
import com.damoreira.snodra.server.v1.Request
import com.damoreira.snodra.server.v1.RequestKind
import com.damoreira.snodra.messaging.CommandBus
import com.damoreira.snodra.messaging.InvalidMessageException
import com.damoreira.snodra.messaging.MessageContext
import com.damoreira.snodra.messaging.MessageKind
import com.damoreira.snodra.messaging.NoHandlerException
import com.damoreira.snodra.messaging.MessagingException
import com.damoreira.snodra.messaging.QueryBus
import com.damoreira.snodra.messaging.RemoteHandlerException
import com.damoreira.snodra.messaging.RetryClassification
import com.damoreira.snodra.messaging.RetryPolicy
import com.damoreira.snodra.messaging.ScatterOptions
import com.damoreira.snodra.messaging.ScatterPart
import com.damoreira.snodra.messaging.ScatterResult
import com.damoreira.snodra.messaging.SendOptions
import com.damoreira.snodra.messaging.StreamOptions
import com.damoreira.snodra.messaging.SubscriptionOptions
import com.damoreira.snodra.messaging.SubscriptionResult
import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Query
import com.damoreira.snodra.messaging.api.StreamingQuery
import com.damoreira.snodra.messaging.api.SubscriptionQuery
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import com.damoreira.snodra.messaging.codec.ResponseTypes
import com.damoreira.snodra.messaging.handler.HandlerInvoker
import com.damoreira.snodra.messaging.handler.HandlerRegistry
import com.damoreira.snodra.messaging.handler.InvocationResult
import com.damoreira.snodra.messaging.metrics.HubOutcome
import com.damoreira.snodra.messaging.metrics.MessagingMetrics
import com.damoreira.snodra.messaging.metrics.NoopMessagingMetrics
import com.damoreira.snodra.messaging.saga.LocalRawCommandSender
import com.damoreira.snodra.messaging.saga.RawCommandSender
import com.damoreira.snodra.messaging.saga.RawFailures
import com.damoreira.snodra.messaging.saga.RawOutcome
import com.damoreira.snodra.messaging.security.CallerContext
import com.damoreira.snodra.messaging.security.LocalIdentity
import com.damoreira.snodra.messaging.tracing.TraceHeaders
import com.damoreira.snodra.messaging.tracing.CorrelationContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import java.time.Duration
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.reflect.KClass

/**
 * [CommandBus] and [QueryBus] over the hub. By default a command or query whose handler lives in this instance is invoked
 * directly (same codec round trip and error mapping, no network; its routing key is ignored); anything else becomes a hub
 * `Request` carrying the caller's bearer token (when [forwardJwt]), a correlation id and the trace context. A query on a
 * type this service registered as **shared** (`@QueryHandler(shared = true)`) is the exception: it always goes through the
 * hub, which round-robins it over every owner's instances, this one included. With [alwaysThroughHub]
 * (`snodra.messaging.hub.always-route-through-hub`) nothing is invoked directly: every request goes through the hub, so a
 * keyed request reaches the instance its routing key chooses whichever instance sent it.
 *
 * Per call [SendOptions] choose the priority (default [priorities] per type, else 0), the timeout (default
 * [requestTimeout]) and the retry policy (default [retry], null = no retry). With retry, failures that
 * [RetryClassification] allows are re-sent as new requests (fresh id, same correlation id) after the policy's jittered
 * backoff ([RetryPolicy.jitteredBackoffBefore]), all attempts within one budget: the chosen timeout, else the hub's
 * deadline for the type ([HubTransport.hubTimeoutFor]). Each attempt sends the remaining budget as `timeout_ms` (at least 1 ms). A retry is
 * started only if, after its backoff, at least `max(initialBackoff, RetryPolicy.MIN_ATTEMPT_BUDGET)` of the budget
 * is left; otherwise the last error is thrown at once, without waiting. Local handlers never retry.
 *
 * [sendRaw] is the saga relay's own way in ([RawCommandSender], kept out of [CommandBus]): a command known only by wire type
 * name and JSON bytes, sent with no caller and answered with a classified [RawOutcome] instead of a decoded reply.
 *
 * Scatter-gather ([scatterGather], [scatterGatherFlow]) always goes through the hub, also when this instance
 * owns the type. Each part is decoded like a unary reply (the query's declared result type); a part's error becomes a
 * [RemoteHandlerException] carrying the hub's code; a part whose payload does not decode is a failed part too (code
 * `ERROR_CODE_UNSPECIFIED`, the decode error as message), never a failure of the whole gather. The hub's `Error` for the gather itself maps as for unary requests
 * (`NO_HANDLER` -> [com.damoreira.snodra.messaging.NoHandlerException], `INVALID_REQUEST` -> [InvalidMessageException]). Retry applies to the whole gather with
 * the query rules: a retried gather is a new gather (fresh id) and its result holds only its own parts. A flow is
 * retried only while it has not emitted a part. [scatterGatherFlow] reads the caller's bearer token and trace context
 * when it is called, not when the flow is collected: a flow returned from a controller is collected on another thread.
 *
 * Streaming queries ([stream]) always go through the hub too. The flow is cold and unbuffered: each
 * collection sends one `STREAMING` request through the hub, and each part is decoded (the query's
 * element type) and emitted before the next one is read, so the transport's credit refills ([HubTransport.stream])
 * follow what the collector consumed. Unlike a unary attempt, every attempt of a stream sends the chosen timeout
 * (`StreamOptions.timeout`, else [requestTimeout]) whole as `timeout_ms`: it is the stream's idle timeout, and the
 * retry budget bounds only the attempts before the first part. A `ReplyEnd` with an error fails the flow exactly like a unary `Error` with the
 * same code ([HubErrors]: `TIMEOUT` -> [com.damoreira.snodra.messaging.MessageTimeoutException], `HANDLER_DISCONNECTED`
 * -> [com.damoreira.snodra.messaging.HandlerDisconnectedException], `HANDLER_ERROR` -> [RemoteHandlerException],
 * `CANCELLED` -> [com.damoreira.snodra.messaging.StreamCancelledException], ...), and so does an `Error` before any
 * part; a part that does not decode fails the flow with [InvalidMessageException] (the transport cancels the stream).
 * An exception thrown by the collector (it runs inside the transport's part callback) is rethrown as it is, also when
 * it is a `HubUnavailableException` or `HubConnectionLostException` of a nested send: the transport reports it as a
 * [StreamCollectorException], so it never counts as this stream's transport failing (no failover to another hub; the
 * stream is cancelled). Retry applies with the query rules only until the first part, to ends with an error as to
 * errors (a retried stream is a new request); after the first part nothing is retried: the flow throws, a broken hub
 * stream as [com.damoreira.snodra.messaging.HubConnectionLostException]. The caller's bearer token and trace context
 * are read when [stream] is called, as for [scatterGatherFlow].
 *
 * A query type served by a subscription handler (`SubscriptionQuery<I, U>`) is declared to the hub as a subscription type, and
 * the hub serves it in mode `SUBSCRIPTION` only: a [send] (unary), [stream] or [scatterGather] of such a type, and a [subscribe] of a
 * query type nobody declared, fail with the hub's `INVALID_REQUEST` ([InvalidMessageException]) before any handler is reached, and
 * are not retried.
 *
 * Subscription queries ([subscribe]) always go through the hub, to one live hub.
 * [subscribe] returns once the initial result arrived (decoded as the query's `I`); `timeout_ms` bounds only that
 * request, so the call retries like a unary query (the retry budget, the remaining time per attempt) and never after the
 * initial result. A `ReplyEnd` with an error, or an `Error`, before the initial result fails the call exactly like a unary
 * `Error` with the same code (`HubErrors` for a subscription: `RESOURCE_EXHAUSTED` keeps the hub's own message as
 * [com.damoreira.snodra.messaging.HandlerBusyException]'s detail). The updates are a hot, collect-once flow over the
 * transport's [HubSubscription]: each is decoded as `U`, the credit refills as the collector consumes ([HubSubscription.next]),
 * `Cancel` goes to the hub when the collector is cancelled or throws (or [SubscriptionResult.cancel] is called), a clean
 * `ReplyEnd` completes the flow, an `ReplyEnd` with an error (or a broken hub stream) fails it with the unary mapping, and a
 * part the hub dropped fails it as `InvalidMessageException("subscription <type> lost N part(s)")`. The caller's bearer token
 * and trace context are read when [subscribe] is called.
 *
 * Each unary command or query call that goes through the hub ([send], [sendRaw]) is timed into [metrics] once, from the
 * call to its result with every retry and back-off, labelled with its final [HubOutcome] (a request that cannot be encoded
 * is `error`); a call cancelled by its caller is not. Local invocations, scatter-gather, streaming and subscription queries are not observed.
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
    /** Default `Request.priority` per type name (`snodra.messaging.hub.priorities`); types not listed send 0. */
    private val priorities: Map<String, Int> = emptyMap(),
    /** Default retry policy (`snodra.messaging.hub.retry.*` when enabled); null = no retry. */
    private val retry: RetryPolicy? = null,
    private val metrics: MessagingMetrics = NoopMessagingMetrics,
    /**
     * `snodra.messaging.hub.always-route-through-hub`: when true, every command and query goes through the hub, also one of a
     * type this instance handles itself (and [sendRaw]'s commands), so the hub routes it like any other: by its routing key to
     * the same instance whichever instance sent it, with flow control, priorities, the hub's deadline and metrics. False (the
     * default) answers such a request in-process, without the hub, ignoring its routing key.
     */
    private val alwaysThroughHub: Boolean = false,
) : CommandBus, QueryBus, RawCommandSender {

    override suspend fun <R> send(command: Command<R>, routingKey: String?): R = send(command, SendOptions(routingKey = routingKey))

    @Suppress("UNCHECKED_CAST")
    override suspend fun <R> send(command: Command<R>, options: SendOptions): R =
        dispatch(command, RequestKind.COMMAND, options) as R

    /**
     * The saga relay's send path ([com.damoreira.snodra.messaging.saga.RawCommandSender]): a command known by wire [type]
     * and JSON [payload]. A handler in this instance is invoked directly (unless [alwaysThroughHub]), otherwise the request goes to the hub, with
     * [options] (routing key, headers, timeout, retry) and the correlation id of the calling coroutine. It carries **no
     * caller**: no bearer token and no identity, whoever is signed in on the thread (a saga's triggering event is not a
     * caller). The reply is not decoded; a failure is returned classified, see [com.damoreira.snodra.messaging.saga.RawFailures], except that the hub's `NO_HANDLER` is transient here.
     */
    override suspend fun sendRaw(type: String, payload: ByteArray, options: SendOptions): RawOutcome {
        if (handledHere(type)) return localRaw.sendRaw(type, payload, options)
        return try {
            timed(MessageKind.COMMAND, type) {
                val template = rawTemplate(type, payload, options)
                withRetry(type, MessageKind.COMMAND, options.timeout, options.retry, retryIf = { true }) { timeout ->
                    attemptReply(template, timeout)
                }
            }
            RawOutcome.Success
        } catch (e: MessagingException) {
            // Through the hub "no handler" is a rolling redeploy of the target service as likely as a mistake: transient
            // (backoff up to EXHAUSTED). Through the local sender, which has no hub to wait for, it stays permanent.
            if (e is NoHandlerException) RawOutcome.Transient(e.message ?: "no handler registered for '$type'") else RawFailures.classify(e)
        }
    }

    private val localRaw = LocalRawCommandSender(invoker)

    /** [sendRaw] answers a command of [type] in-process: this instance handles it and [alwaysThroughHub] is off. */
    private fun handledHere(type: String): Boolean = !alwaysThroughHub && registry.commandHandler(type) != null

    /**
     * A local handler: [sendTimeout]. Through the hub at least the hub's deadline for the request [sendRaw] sends (its
     * `timeout_ms` is [requestTimeout] or 0; every live hub's per-type override or clamp) plus
     * [RawCommandSender.HUB_CALL_GUARD], so the relay never abandons a call the hub may still be running.
     */
    override fun sendTimeoutFor(type: String, sendTimeout: Duration): Duration {
        if (handledHere(type)) return sendTimeout
        val timeoutMs = chosenTimeout(null)?.toMillis()?.coerceIn(0, Int.MAX_VALUE.toLong())?.toInt() ?: 0
        return maxOf(sendTimeout, connection.hubDeadlineBound(type, timeoutMs).plus(RawCommandSender.HUB_CALL_GUARD))
    }

    /** The request of [sendRaw]: correlation id and trace context, [SendOptions.headers], never the caller's token. */
    private suspend fun rawTemplate(type: String, payload: ByteArray, options: SendOptions): Request {
        val headers = options.headers.toMutableMap()
        headers["correlationId"] = CorrelationContext.resolve(currentCoroutineContext())
        traceHeaders.inject(headers, currentCoroutineContext())
        return Request.newBuilder()
            .setKind(RequestKind.COMMAND)
            .setType(type)
            .setRoutingKey(options.routingKey.orEmpty())
            .setPayload(ByteString.copyFrom(payload))
            .putAllHeaders(headers)
            .setPriority(options.priority ?: priorities[type] ?: 0)
            .build()
    }

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

    override suspend fun <I, U> subscribe(query: SubscriptionQuery<I, U>, options: SubscriptionOptions): SubscriptionResult<I, U> {
        val caller = callerHeaders() // first, on the caller's thread: nothing has suspended yet
        val initialType = ResponseTypes.responseTypeOf(query::class)
        val updateType = ResponseTypes.updateTypeOf(query::class)
        val template = template(query, RequestKind.QUERY, routingKey = null, options.priority, caller).toBuilder()
            .setMaxResults(options.maxResults)
            .setInitialCredit(options.initialCredit)
            .build()
        val type = template.type
        // timeout_ms bounds the initial request only: attempts before the initial result are retried like a unary query.
        val subscription = withRetry(type, MessageKind.QUERY, options.timeout, options.retry, retryIf = { true }) { timeout ->
            subscribeOnce(template, timeout)
        }
        val initial = try {
            val part = subscription.initial
            if (part.hasError()) throw RemoteHandlerException(type, part.error.message, part.error.details.toByteArray(), part.error.code.name)
            decodeReply(type, query::class, part.payload.toByteArray())
        } catch (e: Throwable) {
            subscription.cancel("initial result unusable: ${e::class.simpleName}") // never leave a half-open subscription behind
            throw e
        }
        val collected = AtomicBoolean(false)
        val updates: Flow<Any> = flow {
            check(!collected.getAndSet(true)) { "the updates of a subscription to $type can be collected once" }
            var reason = "collector cancelled"
            try {
                while (true) {
                    val message = subscription.next() ?: return@flow // cancelled by SubscriptionResult.cancel()
                    when {
                        message.hasPart() -> emit(decodeUpdate(type, updateType, message.part))
                        message.hasEnd() && message.end.hasError() -> throw message.end.error.let {
                            HubErrors.toException(type, it.code, it.message, it.details.toByteArray(), subscription = true)
                        }
                        message.hasEnd() -> return@flow
                        message.hasError() -> throw message.error.let {
                            HubErrors.toException(type, it.code, it.message, it.details.toByteArray(), subscription = true)
                        }
                        else -> throw InvalidMessageException("Unexpected message in subscription $type")
                    }
                }
            } catch (e: Throwable) {
                // An exception of the collector (or of decoding) ends the subscription; the transport tells the hub.
                if (e !is CancellationException) reason = "collector failed: ${e::class.simpleName}: ${e.message}"
                throw e
            } finally {
                subscription.cancel(reason) // a no-op when the subscription has ended or was cancelled
            }
        }
        @Suppress("UNCHECKED_CAST")
        return SubscriptionResult(initial as I, updates as Flow<U>) { subscription.cancel("subscriber cancelled") }
    }

    /** One subscription built from [template] with a fresh id and [timeout] as `timeout_ms` (the initial request's); returns it open. */
    private suspend fun subscribeOnce(template: Request, timeout: Duration?): HubSubscription {
        val type = template.type
        return when (val opened = connection.subscribe(withFreshId(template, timeout))) {
            is SubscribeResult.Open -> opened.subscription
            is SubscribeResult.Closed -> {
                val terminal = opened.terminal
                when {
                    // The failed initial result (or a refused target) ends the subscription: the same failure a unary Error with that code is.
                    terminal.hasEnd() && terminal.end.hasError() -> throw terminal.end.error.let {
                        HubErrors.toException(type, it.code, it.message, it.details.toByteArray(), subscription = true)
                    }
                    terminal.hasError() -> throw terminal.error.let {
                        HubErrors.toException(type, it.code, it.message, it.details.toByteArray(), subscription = true)
                    }
                    else -> throw InvalidMessageException("Unexpected answer to subscription query $type")
                }
            }
        }
    }

    private fun decodeUpdate(type: String, updateType: KClass<*>, part: ReplyPart): Any {
        // The hub relays an error as the end of the subscription, never as a part; one would be a hub that does it differently.
        if (part.hasError()) throw RemoteHandlerException(type, part.error.message, part.error.details.toByteArray(), part.error.code.name)
        return try {
            codec.decode(part.payload.toByteArray(), updateType)
        } catch (e: InvalidMessageException) {
            throw InvalidMessageException("Update of $type did not match ${updateType.qualifiedName}", e)
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
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
            }
            terminal.hasEnd() -> return
            terminal.hasError() -> throw terminal.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
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
    ): com.damoreira.snodra.server.v1.ReplyEnd {
        val type = template.type
        val request = withFreshId(template, timeout).toBuilder()
            .setMode(com.damoreira.snodra.server.v1.RequestMode.SCATTER_GATHER)
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
            val exception = RemoteHandlerException(type, message, ByteArray(0), com.damoreira.snodra.server.v1.ErrorCode.ERROR_CODE_UNSPECIFIED.name)
            ScatterPart(part.responderService, part.responderInstance, null, exception)
        }
    }

    private suspend fun dispatch(message: Any, kind: RequestKind, options: SendOptions): Any {
        val type = typeResolver.typeNameOf(message::class)
        // A shared query type is never answered locally: the hub round-robins a point query over every owner's
        // instances, this one included, so an owner is not favoured over the others (as in the Rust library).
        // With alwaysThroughHub nothing is: the hub routes it (by its routing key, too) like a request of another service.
        val local = when {
            alwaysThroughHub -> null
            kind == RequestKind.COMMAND -> registry.commandHandler(type)
            else -> registry.queryHandler(type)?.takeUnless { it.shared }
        }
        if (local != null) {
            val correlationId = CorrelationContext.resolve(currentCoroutineContext())
            val token = CallerContext.currentBearerToken()
            return invokeLocally(type, kind, message::class, codec.encode(message), correlationId, token, options.headers)
        }
        val messageKind = if (kind == RequestKind.COMMAND) MessageKind.COMMAND else MessageKind.QUERY
        return timed(messageKind, type) {
            // Built inside the timing: a request that cannot be encoded is a call that failed (`error`), as in Rust.
            val template = template(message, kind, options.routingKey, options.priority, extra = options.headers)
            withRetry(type, messageKind, options.timeout, options.retry, retryIf = { true }) { timeout ->
                attempt(template, message::class, timeout)
            }
        }
    }

    /** Runs [call] (a unary call through the hub, all its attempts) and reports its time and outcome to [metrics]. */
    private inline fun <T> timed(kind: MessageKind, type: String, call: () -> T): T {
        val started = System.nanoTime()
        try {
            return call().also { metrics.hubRequest(kind, type, HubOutcome.OK, System.nanoTime() - started) }
        } catch (e: CancellationException) {
            throw e // the caller gave up: not an outcome of the call
        } catch (e: Exception) {
            metrics.hubRequest(kind, type, HubOutcome.of(e), System.nanoTime() - started)
            throw e
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
     * trace, see [callerHeaders]), priority, and the caller's [extra] headers ([SendOptions.headers]; the library's own
     * headers win, though `SendOptions` already refuses those names). The correlation id comes from the coroutine
     * context, so it is resolved here.
     */
    private suspend fun template(
        message: Any,
        kind: RequestKind,
        routingKey: String?,
        priority: Int?,
        caller: Map<String, String> = callerHeaders(),
        extra: Map<String, String> = emptyMap(),
    ): Request {
        val type = typeResolver.typeNameOf(message::class)
        val payload = codec.encode(message)
        val correlationId = CorrelationContext.resolve(currentCoroutineContext())
        val headers = extra.toMutableMap()
        headers["correlationId"] = correlationId
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
        // Unset: 0 on the wire, the hub decides.
        .setTimeoutMs(timeout?.toMillis()?.coerceIn(0, Int.MAX_VALUE.toLong())?.toInt() ?: 0)
        .build()

    /** One hub request built from [template] with a fresh id and [timeout] as `timeout_ms` (null sends 0). */
    private suspend fun attempt(template: Request, messageClass: KClass<*>, timeout: Duration?): Any =
        decodeReply(template.type, messageClass, attemptReply(template, timeout))

    /** [attempt] up to the reply: its payload bytes, or the failure the hub's answer maps to. */
    private suspend fun attemptReply(template: Request, timeout: Duration?): ByteArray {
        val type = template.type
        val answer = connection.send(withFreshId(template, timeout))
        return when {
            answer.hasReply() && answer.reply.hasError() -> throw answer.reply.error.let {
                HubErrors.toException(type, it.code, it.message, it.details.toByteArray())
            }
            answer.hasReply() -> answer.reply.payload.toByteArray()
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
        extra: Map<String, String>,
    ): Any {
        val identity = LocalIdentity.current()
        val ctx = MessageContext(
            messageId = UUID.randomUUID().toString(),
            type = type,
            correlationId = correlationId,
            userId = identity?.userId,
            roles = identity?.roles ?: emptySet(),
            authorization = token?.let { "Bearer $it" },
            headers = extra + ("correlationId" to correlationId),
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
