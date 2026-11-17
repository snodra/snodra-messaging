package com.damoreira.snodra.messaging.saga

import com.damoreira.snodra.messaging.api.Command
import com.damoreira.snodra.messaging.api.Event
import com.damoreira.snodra.messaging.api.MessageType
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import java.time.Duration
import java.time.Instant
import java.util.UUID
import kotlin.reflect.KClass

/*
 * What a saga handler is handed: objects that only record. The runtime applies the recorded effects, in one
 * transaction, after every handler of the attempt returned, so an exception leaves nothing behind (not even in an
 * in-memory store without transactions).
 */

/** Header of a `saga_dispatch` entry that carries the correlation id of the message that caused it. */
internal const val DISPATCH_CORRELATION_HEADER = "correlation-id"

/**
 * The header of a `saga_dispatch` entry that keeps the handled event's W3C `traceparent` (the trace floor): the relay sends the
 * command, or publishes the event, in that trace. Absent when the event had none (or an invalid one).
 */
internal const val DISPATCH_TRACEPARENT_HEADER = "traceparent"

/** Entries of one saga handling take `seq = version * SEQ_STRIDE + index`, so the index must stay below it. */
internal const val SEQ_STRIDE = 1000L

internal class RecordingSagaLifecycle(override val sagaId: String, associations: List<Association>) : SagaLifecycle {
    val associations: MutableList<Association> = associations.toMutableList()
    var endRequested = false
        private set

    override fun associateWith(key: String, value: String) {
        val association = Association(key, value)
        if (association !in associations) associations += association
    }

    override fun removeAssociationWith(key: String, value: String) {
        associations.remove(Association(key, value))
    }

    override fun end() {
        endRequested = true
    }
}

/** Records commands and events as `saga_dispatch` entries `sagaId:seq`, `seq = newVersion * 1000 + index`. */
internal class RecordingSagaCommands(
    private val sagaId: String,
    private val version: Long,
    private val correlationId: String,
    private val now: Instant,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
    /** The handled event's valid `traceparent`, kept with every entry ([DISPATCH_TRACEPARENT_HEADER]); null: none. */
    private val traceparent: String? = null,
) : SagaCommands {
    val entries = mutableListOf<SagaDispatchDocument>()

    override fun send(command: Command<*>, routingKey: String?) = record(DispatchKind.COMMAND, command, routingKey)

    override fun publish(event: Event, key: String?) = record(DispatchKind.EVENT, event, key)

    private fun record(kind: DispatchKind, message: Any, routingKey: String?) {
        val index = entries.size
        check(index < SEQ_STRIDE) { "A saga handling may send and publish at most ${SEQ_STRIDE - 1} messages, was $index" }
        val seq = version * SEQ_STRIDE + index
        entries += SagaDispatchDocument(
            id = "$sagaId:$seq",
            sagaId = sagaId,
            seq = seq,
            kind = kind,
            type = typeResolver.typeNameOf(message::class),
            payload = String(codec.encode(message), Charsets.UTF_8),
            headers = mapOf(DISPATCH_CORRELATION_HEADER to correlationId) + (traceparent?.let { mapOf(DISPATCH_TRACEPARENT_HEADER to it) } ?: emptyMap()),
            routingKey = routingKey,
            createdAt = now,
        )
    }
}

/** A deadline change recorded by a handler, applied in call order. */
internal sealed interface DeadlineOp {
    class Schedule(val deadline: DeadlineDocument) : DeadlineOp
    class Cancel(val deadlineId: String) : DeadlineOp
    class CancelAll(val name: String) : DeadlineOp
}

/** Records deadline changes of one saga instance; times are relative to [now] (the injectable clock). */
internal class RecordingDeadlineManager(
    private val sagaType: String,
    private val sagaId: String,
    private val now: Instant,
    private val codec: JsonCodec,
    private val correlationId: String,
) : DeadlineManager {
    val ops = mutableListOf<DeadlineOp>()

    override fun schedule(after: Duration, name: String, payload: Any?): String = schedule(now.plus(after), name, payload)

    override fun schedule(at: Instant, name: String, payload: Any?): String {
        val id = UUID.randomUUID().toString()
        ops += DeadlineOp.Schedule(
            DeadlineDocument(
                id = id,
                sagaType = sagaType,
                sagaId = sagaId,
                name = name,
                payload = payload?.let { String(codec.encode(it), Charsets.UTF_8) },
                payloadType = payload?.let { deadlinePayloadType(it::class) },
                dueAt = at,
                correlationId = correlationId,
            ),
        )
        return id
    }

    override fun cancel(deadlineId: String) {
        ops += DeadlineOp.Cancel(deadlineId)
    }

    override fun cancelAll(name: String) {
        ops += DeadlineOp.CancelAll(name)
    }
}

/** R7: the payload class's `@MessageType` name when it has one, else its fully qualified class name. */
internal fun deadlinePayloadType(klass: KClass<*>): String = klass.java.getAnnotation(MessageType::class.java)?.value ?: klass.java.name
