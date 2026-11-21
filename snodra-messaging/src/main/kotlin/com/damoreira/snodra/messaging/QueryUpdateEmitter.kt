package com.damoreira.snodra.messaging

import com.damoreira.snodra.messaging.api.SubscriptionQuery
import kotlin.reflect.KClass

/**
 * Pushes the updates of subscription queries from the instance that learns of them. Every subscription of
 * a query type that this instance serves (a `@QueryHandler` taking the [SubscriptionQuery]) is a target; [emit],
 * [complete] and [completeExceptionally] pick the ones whose decoded query passes `filter` and deliver to them in call
 * order. Emitting when no subscription matches is a no-op, so a service may emit unconditionally.
 *
 * **Transactions.** Inside an active Spring transaction (`TransactionSynchronizationManager.isActualTransactionActive()`,
 * a thread-bound one: `@Transactional`, `TransactionTemplate`) every call is deferred to `afterCommit` and dropped on
 * rollback, so a subscriber never sees an update of data that was not committed. The subscriptions open and the filter
 * are evaluated at delivery, after the commit. Outside a transaction delivery is immediate. A reactive (Reactor-context)
 * transaction is not seen: its calls are immediate.
 *
 * Updates are delivered at most once and only to this instance's subscriptions: an instance that is not subscribed
 * (the hub fixes the targets when the subscription opens) never hears of them, and a subscription lost with its hub
 * stream is gone for good.
 */
interface QueryUpdateEmitter {
    /** Sends [update] to every open subscription of [queryType] on this instance whose query passes [filter]. */
    fun <Q : SubscriptionQuery<*, U>, U : Any> emit(queryType: KClass<Q>, filter: (Q) -> Boolean, update: U)

    /** Ends, cleanly, every open subscription of [queryType] on this instance whose query passes [filter]; they are removed. */
    fun <Q : SubscriptionQuery<*, *>> complete(queryType: KClass<Q>, filter: (Q) -> Boolean)

    /**
     * Ends with an error every open subscription of [queryType] on this instance whose query passes [filter]
     * (`HANDLER_ERROR`, `"<SimpleName>: <message>"`, so the subscriber's flow fails with a `RemoteHandlerException`);
     * they are removed.
     */
    fun <Q : SubscriptionQuery<*, *>> completeExceptionally(queryType: KClass<Q>, filter: (Q) -> Boolean, cause: Throwable)
}

/** [QueryUpdateEmitter.emit] with the query type as a type argument: `emitter.emit<WatchAccount, BalanceChanged>({ it.account == 7 }, update)`. */
inline fun <reified Q : SubscriptionQuery<*, U>, U : Any> QueryUpdateEmitter.emit(noinline filter: (Q) -> Boolean, update: U) =
    emit(Q::class, filter, update)

/** [QueryUpdateEmitter.complete] with the query type as a type argument: `emitter.complete<WatchAccount> { it.account == 7 }`. */
inline fun <reified Q : SubscriptionQuery<*, *>> QueryUpdateEmitter.complete(noinline filter: (Q) -> Boolean) =
    complete(Q::class, filter)

/** [QueryUpdateEmitter.completeExceptionally] with the query type as a type argument. */
inline fun <reified Q : SubscriptionQuery<*, *>> QueryUpdateEmitter.completeExceptionally(noinline filter: (Q) -> Boolean, cause: Throwable) =
    completeExceptionally(Q::class, filter, cause)
