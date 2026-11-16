package io.damoreira.kraken.conformance

import io.damoreira.kraken.messaging.AccessDeniedException
import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.HandlerBusyException
import io.damoreira.kraken.messaging.HandlerDisconnectedException
import io.damoreira.kraken.messaging.HubConnectionLostException
import io.damoreira.kraken.messaging.HubUnavailableException
import io.damoreira.kraken.messaging.InvalidMessageException
import io.damoreira.kraken.messaging.MessageTimeoutException
import io.damoreira.kraken.messaging.NoHandlerException
import io.damoreira.kraken.messaging.QueryBus
import io.damoreira.kraken.messaging.RemoteHandlerException
import io.damoreira.kraken.messaging.RetryPolicy
import io.damoreira.kraken.messaging.ScatterOptions
import io.damoreira.kraken.messaging.SendOptions
import io.damoreira.kraken.messaging.StreamCancelledException
import io.damoreira.kraken.messaging.StreamOptions
import io.damoreira.kraken.messaging.SubscriptionOptions
import io.damoreira.kraken.messaging.UnauthenticatedException
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.StreamingQuery
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import org.springframework.stereotype.Component
import tools.jackson.databind.JsonNode
import tools.jackson.databind.node.ObjectNode
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * `conformance.Send` (conformance.md): sends a request through the library's buses, as an application would, with the options
 * the payload asks for, and reports the outcome in the contract's shape; asynchronously when asked (`conformance.Outcome`).
 */
@Component
class Sender(private val commands: CommandBus, private val queries: QueryBus) {
    private val scope = CoroutineScope(SupervisorJob() + Dispatchers.Default)
    private val ops = ConcurrentHashMap<String, CompletableDeferred<JsonNode>>()

    suspend fun send(spec: JsonNode): JsonNode {
        if (!spec.path("async").asBoolean(false)) return run(spec)
        val id = UUID.randomUUID().toString()
        val done = CompletableDeferred<JsonNode>()
        ops[id] = done
        scope.launch { done.complete(run(spec)) }
        return json.objectNode().put("opId", id)
    }

    suspend fun outcome(id: String, waitMs: Long): JsonNode {
        val op = ops[id] ?: return json.objectNode().put("unknownOp", id)
        return withTimeoutOrNull(waitMs) { op.await() } ?: json.objectNode().put("pending", true)
    }

    private suspend fun run(spec: JsonNode): JsonNode {
        val started = System.nanoTime()
        val outcome: ObjectNode = try {
            when (val op = spec.path("op").asString("unary")) {
                "unary" -> unary(spec)
                "scatter" -> scatter(spec)
                "stream" -> stream(spec)
                "subscribe" -> subscribe(spec)
                else -> unsupported("op $op")
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Refused) {
            failed(e.cause!!, "refused")
        } catch (e: Throwable) {
            failed(e)
        }
        return outcome.put("elapsedMs", (System.nanoTime() - started) / 1_000_000)
    }

    /** The library refused the options before anything was sent (a header it owns). */
    private class Refused(cause: Throwable) : RuntimeException(cause)

    private fun timeout(spec: JsonNode): Duration? = spec.path("timeoutMs").takeIf { it.isNumber }?.let { Duration.ofMillis(it.asLong()) }

    private fun retry(spec: JsonNode): RetryPolicy? = spec.path("retry").takeIf { it.isObject }?.let {
        RetryPolicy(maxAttempts = it.path("maxAttempts").asInt(1), initialBackoff = Duration.ofMillis(it.path("initialBackoffMs").asLong(100)))
    }

    private fun options(spec: JsonNode): SendOptions {
        val headers = buildMap { spec.path("headers").properties().forEach { (k, v) -> put(k, v.asString("")) } }
        return try {
            SendOptions(timeout = timeout(spec), routingKey = spec.path("routingKey").takeIf { it.isString }?.asString(), retry = retry(spec), headers = headers)
        } catch (e: IllegalArgumentException) {
            // SendOptions refuses a header the library sets itself, on construction: nothing was sent.
            throw Refused(e)
        }
    }

    private fun credit(spec: JsonNode): Int = spec.path("initialCredit").asInt(0).takeIf { it > 0 } ?: 64

    private suspend fun unary(spec: JsonNode): ObjectNode {
        val options = options(spec)
        val payload = spec.path("payload")
        val reply: Any = when (val target = spec.path("target").asString("probe")) {
            "probe" -> commands.send(Probe(payload), options)
            "missing" -> commands.send(Missing(payload), options)
            "echo" -> commands.send(Echo(payload), options)
            "probe-query" -> queries.send(ProbeQuery(payload), options)
            "echo-query" -> queries.send(EchoQuery(payload), options)
            "shared-query" -> queries.send(SharedQuery(payload), options)
            else -> return unsupported("target $target")
        }
        return json.objectNode().put("result", "ok").set("payload", mapper.valueToTree<JsonNode>(reply))
    }

    private suspend fun scatter(spec: JsonNode): ObjectNode {
        val options = ScatterOptions(timeout = timeout(spec), maxResults = spec.path("maxResults").asInt(0), retry = retry(spec))
        val payload = spec.path("payload")
        val query: Query<JsonNode> = when (val target = spec.path("target").asString("probe-query")) {
            "probe-query" -> ProbeQuery(payload)
            "echo-query" -> EchoQuery(payload)
            "shared-query" -> SharedQuery(payload)
            else -> return unsupported("target $target")
        }
        val result = queries.scatterGather(query, options)
        val parts = json.arrayNode()
        for (p in result.parts) {
            parts.add(
                json.objectNode().apply {
                    put("service", p.service)
                    put("instance", p.instance)
                    put("ok", p.error == null)
                    if (p.error == null) set("payload", p.result) else set("error", error(p.error!!, "code"))
                },
            )
        }
        return json.objectNode().put("result", "ok").set("parts", parts).put("failed", result.failed).put("timedOut", result.timedOut)
    }

    private suspend fun stream(spec: JsonNode): ObjectNode {
        val options = StreamOptions(timeout = timeout(spec), maxResults = spec.path("maxResults").asInt(0), initialCredit = credit(spec), retry = retry(spec))
        val payload = spec.path("payload")
        val query: StreamingQuery<JsonNode> = when (val target = spec.path("target").asString("probe-stream")) {
            "probe-stream" -> ProbeStream(payload)
            "stream" -> Stream(payload)
            else -> return unsupported("target $target")
        }
        return read(queries.stream(query, options), spec, mutableListOf())
    }

    private suspend fun subscribe(spec: JsonNode): ObjectNode {
        val options = SubscriptionOptions(timeout = timeout(spec), maxResults = spec.path("maxResults").asInt(0), initialCredit = credit(spec), retry = retry(spec))
        val payload = spec.path("payload")
        val result = when (val target = spec.path("target").asString("probe-watch")) {
            "probe-watch" -> queries.subscribe(ProbeWatch(payload), options)
            "watch" -> queries.subscribe(Watch(payload.path("topic").asString("")), options)
            else -> return unsupported("target $target")
        }
        val read = spec.path("read").takeIf { it.isNumber }?.asInt()
        if (read != null && read <= 1) {
            // The initial result counts: nothing more is read, and dropping the subscription cancels it.
            result.cancel()
            return json.objectNode().put("result", "ok").set("items", json.arrayNode().add(result.initial)).put("end", "stopped")
        }
        return read(result.updates, spec, mutableListOf(result.initial))
    }

    /**
     * Reads [items] (after [got], a subscription's initial result) until it ends, or until `read` items were read: the flow is
     * then left, which cancels the stream or subscription; `readDelayMs` between reads.
     */
    private suspend fun read(items: Flow<JsonNode>, spec: JsonNode, got: MutableList<JsonNode>): ObjectNode {
        val read = spec.path("read").takeIf { it.isNumber }?.asInt()
        val delayMs = spec.path("readDelayMs").asLong(0)
        val wanted = read?.let { it - got.size }
        try {
            val flow = when {
                wanted == null -> items
                wanted <= 0 -> null
                else -> items.take(wanted)
            }
            flow?.collect { item ->
                got.add(item)
                if (delayMs > 0) delay(delayMs)
            }
        } catch (e: CancellationException) {
            throw e
        } catch (e: Throwable) {
            return failed(e).set("items", json.arrayNode().addAll(got))
        }
        val stopped = read != null && got.size >= read
        return json.objectNode().put("result", "ok").set("items", json.arrayNode().addAll(got)).put("end", if (stopped) "stopped" else "completed")
    }

    private fun unsupported(reason: String): ObjectNode = json.objectNode().put("result", "unsupported").put("reason", reason)

    private fun failed(e: Throwable, kind: String? = null): ObjectNode = json.objectNode().put("result", "error").set("error", error(e, kind))

    /**
     * A failed call as the contract's error, by the library's typed exceptions. kraken-messaging has no typed exception for lost
     * parts: a gap or a short end is an [InvalidMessageException] "... lost N part(s)", told apart by its text.
     */
    private fun error(e: Throwable, kind: String? = null): ObjectNode {
        val (k, code) = when {
            kind != null -> kind to null
            e is HubUnavailableException -> "never-sent" to null
            e is HubConnectionLostException -> "outcome-unknown" to null
            e is NoHandlerException -> "code" to "NO_HANDLER"
            e is MessageTimeoutException -> "code" to "TIMEOUT"
            e is HandlerDisconnectedException -> "code" to "HANDLER_DISCONNECTED"
            e is RemoteHandlerException -> "code" to (e.code.takeIf { it in CODES } ?: "HANDLER_ERROR")
            e is UnauthenticatedException -> "code" to "UNAUTHENTICATED"
            e is AccessDeniedException -> "code" to "PERMISSION_DENIED"
            e is HandlerBusyException -> "code" to "RESOURCE_EXHAUSTED"
            e is StreamCancelledException -> "code" to "CANCELLED"
            e is InvalidMessageException && LOST.containsMatchIn(e.message.orEmpty()) -> "lost-parts" to null
            e is InvalidMessageException -> "code" to "INVALID_REQUEST"
            else -> "local" to null
        }
        return json.objectNode().apply {
            put("kind", k)
            put("code", code)
            put("message", e.message ?: e::class.simpleName)
            put("details", (e as? RemoteHandlerException)?.details?.takeIf { it.isNotEmpty() }?.toString(Charsets.UTF_8))
        }
    }

    private companion object {
        val CODES = setOf(
            "NO_HANDLER", "TIMEOUT", "HANDLER_DISCONNECTED", "HANDLER_ERROR", "UNAUTHENTICATED", "INVALID_REQUEST",
            "PERMISSION_DENIED", "RESOURCE_EXHAUSTED", "CANCELLED",
        )
        val LOST = Regex("""\blost \d+ part\(s\)""")
    }
}
