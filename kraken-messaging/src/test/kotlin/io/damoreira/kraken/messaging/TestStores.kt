package io.damoreira.kraken.messaging

import io.damoreira.kraken.messaging.mongo.MongoMessageStore
import io.damoreira.kraken.messaging.saga.DeadlineDocument
import io.damoreira.kraken.messaging.saga.SagaDispatchDocument
import io.damoreira.kraken.messaging.saga.SagaDocument
import io.damoreira.kraken.messaging.saga.SagaFailureDocument
import io.damoreira.kraken.messaging.store.ExternalCallRecord
import io.damoreira.kraken.messaging.store.ExternalCallStore
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.SagaDispatchStore
import io.damoreira.kraken.messaging.store.SagaStores
import org.springframework.dao.DataAccessResourceFailureException
import org.springframework.data.mongodb.core.MongoTemplate
import java.time.Instant
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/** The MongoDB message store over [mongo] (its transaction manager over the same factory), with its indexes, as at startup. */
fun mongoStore(mongo: MongoTemplate): MongoMessageStore = MongoMessageStore(mongo).also { it.ensureIndexes() }

/*
 * The collections of the saga documents, for the tests that read or write them through the template: the core's saga types
 * are plain classes, so the template maps them by their properties (`id` as `_id`) into the collection named here.
 */
const val SAGAS = "sagas"
const val SAGA_DISPATCH = "saga_dispatch"
const val DEADLINES = "deadlines"
const val SAGA_FAILURES = "saga_failures"
const val EXTERNAL_CALLS = "external_calls"

fun MongoTemplate.sagas(): List<SagaDocument> = findAll(SagaDocument::class.java, SAGAS)
fun MongoTemplate.dispatchEntries(): List<SagaDispatchDocument> = findAll(SagaDispatchDocument::class.java, SAGA_DISPATCH)
fun MongoTemplate.deadlines(): List<DeadlineDocument> = findAll(DeadlineDocument::class.java, DEADLINES)
fun MongoTemplate.sagaFailures(): List<SagaFailureDocument> = findAll(SagaFailureDocument::class.java, SAGA_FAILURES)

/**
 * The MongoDB message store over [mongo] with faults a test switches on, each for the next matching call only: the crash
 * between sending a dispatch entry and marking it ([failNextMark]), the store going away right after an external call
 * returned ([failNextSucceededWrite]) or after a failure notice was delivered ([failNextNoticeClear]), and another call
 * writing right after a read of an external call record ([afterNextExternalCallRead]).
 */
class FaultyMessageStore(val mongo: MongoTemplate, private val delegate: MessageStore = mongoStore(mongo)) : MessageStore by delegate {
    val failNextMark = AtomicBoolean()
    val failNextSucceededWrite = AtomicBoolean()
    val failNextNoticeClear = AtomicBoolean()
    val afterNextExternalCallRead = AtomicReference<(() -> Unit)?>()

    override val sagas: SagaStores = delegate.sagas.let {
        SagaStores(it.sagas, Dispatch(it.dispatch), it.deadlines, it.failures, it.markers)
    }

    override val externalCalls: ExternalCallStore = Calls(delegate.externalCalls)

    private inner class Dispatch(private val d: SagaDispatchStore) : SagaDispatchStore by d {
        override fun markDispatched(id: String, now: Instant) {
            if (failNextMark.getAndSet(false)) throw IllegalStateException("crashed before marking the entry dispatched")
            d.markDispatched(id, now)
        }

        override fun clearNotice(id: String) {
            if (failNextNoticeClear.getAndSet(false)) throw DataAccessResourceFailureException("the store went away after the notice was delivered")
            d.clearNotice(id)
        }
    }

    private inner class Calls(private val c: ExternalCallStore) : ExternalCallStore by c {
        override suspend fun find(id: String): ExternalCallRecord? = c.find(id).also { afterNextExternalCallRead.getAndSet(null)?.invoke() }

        override suspend fun succeed(id: String, owner: String, reply: ByteArray, now: Instant): Boolean {
            if (failNextSucceededWrite.getAndSet(false)) throw DataAccessResourceFailureException("the store went away after the external call returned")
            return c.succeed(id, owner, reply, now)
        }
    }
}
const val DEAD_LETTERS = "dead_letters"
const val PROCESSOR_CONTROL = "processor_control"
