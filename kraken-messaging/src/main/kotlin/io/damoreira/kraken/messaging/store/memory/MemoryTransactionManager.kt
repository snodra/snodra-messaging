package io.damoreira.kraken.messaging.store.memory

import org.springframework.transaction.TransactionDefinition
import org.springframework.transaction.support.AbstractPlatformTransactionManager
import org.springframework.transaction.support.DefaultTransactionStatus
import org.springframework.transaction.support.SmartTransactionObject
import org.springframework.transaction.support.TransactionSynchronizationManager

/**
 * The transactions of a [MemoryMessageStore] (its [MemoryMessageStore.transactions]): a transaction is a journal of the store's
 * writes bound to the calling thread, applied at once at the commit (or failing it on a write conflict) and dropped at a
 * rollback. Joins an existing transaction (`REQUIRED`), suspends it (`REQUIRES_NEW`, `NOT_SUPPORTED`), as any Spring
 * transaction manager. Isolation and timeouts are not applied.
 */
class MemoryTransactionManager internal constructor(private val store: MemoryMessageStore) : AbstractPlatformTransactionManager() {
    private class TransactionObject(var tx: MemoryTransaction?) : SmartTransactionObject {
        override fun isRollbackOnly(): Boolean = tx?.rollbackOnly == true

        override fun flush() = Unit
    }

    override fun doGetTransaction(): Any = TransactionObject(store.bound())

    override fun isExistingTransaction(transaction: Any): Boolean = (transaction as TransactionObject).tx != null

    override fun doBegin(transaction: Any, definition: TransactionDefinition) {
        val tx = MemoryTransaction()
        (transaction as TransactionObject).tx = tx
        // Still bound here: a transaction that ended, begun again from its after-completion callbacks (its own cleanup unbinds
        // nothing then). Replaced for the new one, whose cleanup unbinds it.
        if (TransactionSynchronizationManager.hasResource(store.transactionKey)) TransactionSynchronizationManager.unbindResource(store.transactionKey)
        TransactionSynchronizationManager.bindResource(store.transactionKey, tx)
    }

    override fun doSuspend(transaction: Any): Any {
        (transaction as TransactionObject).tx = null
        return TransactionSynchronizationManager.unbindResource(store.transactionKey)
    }

    override fun doResume(transaction: Any?, suspendedResources: Any) {
        TransactionSynchronizationManager.bindResource(store.transactionKey, suspendedResources)
    }

    override fun doCommit(status: DefaultTransactionStatus) = store.commit(tx(status))

    override fun doRollback(status: DefaultTransactionStatus) = store.rollback(tx(status))

    override fun doSetRollbackOnly(status: DefaultTransactionStatus) {
        tx(status).rollbackOnly = true
    }

    override fun doCleanupAfterCompletion(transaction: Any) {
        val tx = (transaction as TransactionObject).tx ?: return
        store.rollback(tx) // no-op after a commit or a rollback; frees a transaction that ended otherwise
        TransactionSynchronizationManager.unbindResourceIfPossible(store.transactionKey)
        transaction.tx = null
    }

    private fun tx(status: DefaultTransactionStatus): MemoryTransaction =
        (status.transaction as TransactionObject).tx ?: error("no in-memory transaction")
}
