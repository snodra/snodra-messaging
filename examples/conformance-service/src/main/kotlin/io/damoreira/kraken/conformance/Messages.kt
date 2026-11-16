package io.damoreira.kraken.conformance

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonValue
import io.damoreira.kraken.messaging.api.Command
import io.damoreira.kraken.messaging.api.MessageType
import io.damoreira.kraken.messaging.api.Query
import io.damoreira.kraken.messaging.api.StreamingQuery
import io.damoreira.kraken.messaging.api.SubscriptionQuery
import tools.jackson.databind.JsonNode

// The fixed types of the conformance contract (kraken protocol/conformance.md, version 1; their JSON Schemas in
// protocol/conformance/). Most payloads are any JSON value: such a message is the JSON document itself.

/** A message whose payload is any JSON document, written and read as that document. */
abstract class AnyJson(@get:JsonValue val json: JsonNode) {
    override fun toString() = "${this::class.simpleName}($json)"
}

// ------------------------------------------------------------------------------------------------- the service's types

@MessageType("conformance.Echo")
class Echo @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>

@MessageType("conformance.EchoQuery")
class EchoQuery @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Query<JsonNode>

@MessageType("conformance.Done")
class Done @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<Unit>

@MessageType("conformance.Sleep")
class Sleep @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>

@MessageType("conformance.SleepQuery")
class SleepQuery @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Query<JsonNode>

@MessageType("conformance.Fail")
class Fail @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>

/** A typed payload: anything that does not decode to an integer count is the library's `INVALID_REQUEST`. */
@MessageType("conformance.Strict")
data class Strict(val count: Long) : Command<JsonNode>

/** As [Strict], for an authenticated caller only. */
@MessageType("conformance.Guarded")
data class Guarded(val count: Long) : Command<JsonNode>

@MessageType("conformance.Relay")
class Relay @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>

@MessageType("conformance.SharedQuery")
class SharedQuery @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Query<JsonNode>

@MessageType("conformance.Stream")
class Stream @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), StreamingQuery<JsonNode>

/** A subscription by topic. */
@MessageType("conformance.Watch")
data class Watch(val topic: String) : SubscriptionQuery<JsonNode, JsonNode>

@MessageType("conformance.Emit")
class Emit @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>

@MessageType("conformance.Send")
class Send @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>

@MessageType("conformance.Outcome")
class Outcome @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Query<JsonNode>

@MessageType("conformance.View")
class View @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Query<JsonNode>

// ------------------------------------------------------------------------- the runner's types (sent, never handled here)

@MessageType("conformance.runner.Probe")
class Probe @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>

@MessageType("conformance.runner.ProbeQuery")
class ProbeQuery @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Query<JsonNode>

@MessageType("conformance.runner.ProbeStream")
class ProbeStream @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), StreamingQuery<JsonNode>

@MessageType("conformance.runner.ProbeWatch")
class ProbeWatch @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) :
    AnyJson(json), SubscriptionQuery<JsonNode, JsonNode>

@MessageType("conformance.runner.Missing")
class Missing @JsonCreator(mode = JsonCreator.Mode.DELEGATING) constructor(json: JsonNode) : AnyJson(json), Command<JsonNode>
