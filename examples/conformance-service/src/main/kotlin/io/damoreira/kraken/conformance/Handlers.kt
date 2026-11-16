package io.damoreira.kraken.conformance

import io.damoreira.kraken.messaging.CommandBus
import io.damoreira.kraken.messaging.MessageContext
import io.damoreira.kraken.messaging.QueryUpdateEmitter
import io.damoreira.kraken.messaging.complete
import io.damoreira.kraken.messaging.completeExceptionally
import io.damoreira.kraken.messaging.emit
import io.damoreira.kraken.messaging.handler.CommandHandler
import io.damoreira.kraken.messaging.handler.QueryHandler
import io.damoreira.kraken.messaging.handler.RequiresAuthentication
import io.damoreira.kraken.messaging.hub.HubConnections
import io.damoreira.kraken.messaging.hub.SubscriptionRegistry
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.springframework.stereotype.Component
import tools.jackson.databind.JsonNode
import tools.jackson.databind.json.JsonMapper
import tools.jackson.databind.node.JsonNodeFactory

/** An application failure: what a handler throws when it fails (the library answers `HANDLER_ERROR`). */
class ConformanceFailure(message: String) : RuntimeException(message)

internal val json: JsonNodeFactory = JsonNodeFactory.instance
internal val mapper: JsonMapper = JsonMapper.builder().build()

/** The service's types (conformance.md, "The service's types"), written as an application writes its handlers. */
@Component
class Handlers(
    private val env: ConformanceEnv,
    private val commands: CommandBus,
    private val emitter: QueryUpdateEmitter,
    private val sender: Sender,
    private val connections: HubConnections,
    private val subscriptions: SubscriptionRegistry,
) {
    private fun echo(payload: JsonNode, ctx: MessageContext): JsonNode = json.objectNode().apply {
        set("payload", payload)
        set("headers", json.objectNode().apply { ctx.headers.forEach { (k, v) -> put(k, v) } })
        put("instanceId", env.instance)
        put("requestId", ctx.messageId)
    }

    @CommandHandler
    fun handle(m: Echo, ctx: MessageContext): JsonNode = echo(m.json, ctx)

    @QueryHandler
    fun handle(m: EchoQuery, ctx: MessageContext): JsonNode = echo(m.json, ctx)

    @CommandHandler
    fun handle(m: Done) = Unit

    private suspend fun sleep(payload: JsonNode): JsonNode {
        val ms = payload.path("ms").asLong(0)
        delay(ms)
        return json.objectNode().put("instanceId", env.instance).put("sleptMs", ms)
    }

    @CommandHandler
    suspend fun handle(m: Sleep): JsonNode = sleep(m.json)

    @QueryHandler
    suspend fun handle(m: SleepQuery): JsonNode = sleep(m.json)

    /**
     * An application failure. kraken-messaging maps a handler's exception to `HANDLER_ERROR` with `"<SimpleName>: <message>"`
     * and no details (only validation failures carry details, a JSON list of violations): the payload's `details` cannot be
     * handed to the caller, so they are not.
     */
    @CommandHandler
    fun handle(m: Fail): JsonNode = throw ConformanceFailure(m.json.path("message").asString("failed"))

    @CommandHandler
    fun handle(m: Strict): JsonNode = json.objectNode().put("count", m.count)

    /** An authenticated caller (anonymous ones are refused before decoding), and not the subject the contract refuses. */
    @CommandHandler
    @RequiresAuthentication(expression = "isAuthenticated() and principal.userId != 'conformance-refused'")
    fun handle(m: Guarded, ctx: MessageContext): JsonNode = json.objectNode().put("count", m.count).put("user", ctx.userId)

    /** A nested call with default options; its failure is rethrown as an application lets it propagate. */
    @CommandHandler
    suspend fun handle(m: Relay): JsonNode {
        val payload = m.json.path("payload")
        val reply = when (m.json.path("target").asString("probe")) {
            "missing" -> commands.send(Missing(payload))
            else -> commands.send(Probe(payload))
        }
        return json.objectNode().set("reply", reply)
    }

    @QueryHandler(shared = true)
    fun handle(m: SharedQuery): JsonNode = json.objectNode().put("service", env.service).put("instanceId", env.instance)

    @QueryHandler
    fun handle(m: Stream): Flow<JsonNode> {
        val count = m.json.path("count").asLong(0)
        val delayMs = m.json.path("delayMs").asLong(0)
        return flow {
            for (n in 1..count) {
                if (delayMs > 0) delay(delayMs)
                emit(json.objectNode().put("n", n))
            }
        }
    }

    @QueryHandler
    fun handle(m: Watch): JsonNode = json.objectNode().put("topic", m.topic).put("value", 0).put("instanceId", env.instance)

    @CommandHandler
    fun handle(m: Emit): JsonNode {
        val topic = m.json.path("topic").asString("")
        val matches: (Watch) -> Boolean = { it.topic == topic }
        for (value in m.json.path("values")) {
            emitter.emit<Watch, JsonNode>(matches, json.objectNode().put("topic", topic).set("value", value))
        }
        val fail = m.json.path("fail")
        when {
            fail.isString -> emitter.completeExceptionally<Watch>(matches, ConformanceFailure(fail.asString()))
            m.json.path("complete").asBoolean(false) -> emitter.complete<Watch>(matches)
        }
        return json.objectNode()
    }

    @CommandHandler
    suspend fun handle(m: Send): JsonNode = sender.send(m.json)

    @QueryHandler
    suspend fun handle(m: Outcome): JsonNode = sender.outcome(m.json.path("opId").asString(""), m.json.path("waitMs").asLong(0))

    @QueryHandler
    fun handle(m: View): JsonNode = json.objectNode().apply {
        put("service", env.service)
        put("instanceId", env.instance)
        set("hubIds", json.arrayNode().apply { connections.liveHubIds().forEach { add(it) } })
        // The newest refusal by any hub, kept after a later acceptance.
        put("lastRejection", connections.latestRejection.value?.let { r -> r.code + (r.description?.let { ": $it" } ?: "") })
        put("subscriptionsServed", subscriptions.size)
        // CONFORMANCE_RETRY_* configures the library's policy for every send (it also takes one per call).
        put("retry", env.retry != null)
    }
}
