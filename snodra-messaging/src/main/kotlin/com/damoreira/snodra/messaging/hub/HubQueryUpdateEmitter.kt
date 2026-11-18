package com.damoreira.snodra.messaging.hub

import com.damoreira.snodra.server.v1.Error
import com.damoreira.snodra.server.v1.ErrorCode
import com.damoreira.snodra.messaging.QueryUpdateEmitter
import com.damoreira.snodra.messaging.api.SubscriptionQuery
import com.damoreira.snodra.messaging.codec.JsonCodec
import com.damoreira.snodra.messaging.codec.MessageTypeResolver
import org.slf4j.LoggerFactory
import org.springframework.transaction.support.TransactionSynchronization
import org.springframework.transaction.support.TransactionSynchronizationManager
import kotlin.reflect.KClass
import kotlin.reflect.full.safeCast

/**
 * The [QueryUpdateEmitter] over a [SubscriptionRegistry]: encodes the update with the library codec once and writes it to
 * every matching subscription's hub stream. In an active Spring transaction the whole delivery (which subscriptions
 * match, the encoding, the writes) runs in `afterCommit`; a failure there is logged, never thrown at the committer. Outside
 * a transaction it is immediate and an encoding failure reaches the caller. A filter that throws skips its subscription
 * (logged), never the others.
 *
 * A subscription whose admission is still running (it is registered when its request is read) matches too: its updates are
 * held in order and written once it is admitted, or dropped when it is refused or cancelled.
 */
class HubQueryUpdateEmitter(
    private val registry: SubscriptionRegistry,
    private val codec: JsonCodec,
    private val typeResolver: MessageTypeResolver,
) : QueryUpdateEmitter {
    private val log = LoggerFactory.getLogger(HubQueryUpdateEmitter::class.java)

    override fun <Q : SubscriptionQuery<*, U>, U : Any> emit(queryType: KClass<Q>, filter: (Q) -> Boolean, update: U) = whenCommitted {
        val matching = matching(queryType, filter)
        if (matching.isEmpty()) return@whenCommitted
        val payload = codec.encode(update)
        for (subscription in matching) {
            if (!subscription.update(payload)) registry.remove(subscription)
        }
    }

    override fun <Q : SubscriptionQuery<*, *>> complete(queryType: KClass<Q>, filter: (Q) -> Boolean) = whenCommitted {
        for (subscription in matching(queryType, filter)) {
            registry.remove(subscription)
            subscription.end(null)
        }
    }

    override fun <Q : SubscriptionQuery<*, *>> completeExceptionally(queryType: KClass<Q>, filter: (Q) -> Boolean, cause: Throwable) = whenCommitted {
        for (subscription in matching(queryType, filter)) {
            registry.remove(subscription)
            val error = Error.newBuilder().setRequestId(subscription.requestId).setCode(ErrorCode.HANDLER_ERROR)
                .setMessage("${cause::class.simpleName}: ${cause.message}").build()
            subscription.end(error)
        }
    }

    private fun <Q : Any> matching(queryType: KClass<Q>, filter: (Q) -> Boolean): List<ServedSubscription> =
        registry.of(typeResolver.typeNameOf(queryType)).filter { subscription ->
            val query = queryType.safeCast(queryOf(subscription, queryType)) ?: return@filter false
            try {
                filter(query)
            } catch (e: Exception) {
                log.warn("The filter of an update for {} threw {}; subscription {} skipped", subscription.type, e.toString(), subscription.requestId)
                false
            }
        }

    /**
     * The query [subscription] watches: decoded when it is admitted, else (admission still running, the subscription is
     * registered pending) decoded here from its request's payload; null when that does not decode (admission will refuse it).
     */
    private fun <Q : Any> queryOf(subscription: ServedSubscription, queryType: KClass<Q>): Any? =
        subscription.query ?: subscription.payload?.let { payload ->
            try {
                codec.decode(payload, queryType)
            } catch (e: Exception) {
                null
            }
        }

    /** Runs [delivery] now, or after the commit of the active thread-bound transaction (never, when it rolls back). */
    private fun whenCommitted(delivery: () -> Unit) {
        if (TransactionSynchronizationManager.isActualTransactionActive() && TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(object : TransactionSynchronization {
                override fun afterCommit() {
                    try {
                        delivery()
                    } catch (e: Exception) {
                        // An exception here would reach the caller of commit() although the transaction is committed.
                        log.error("Delivering a subscription update after the commit failed", e)
                    }
                }
            })
        } else {
            delivery()
        }
    }
}
