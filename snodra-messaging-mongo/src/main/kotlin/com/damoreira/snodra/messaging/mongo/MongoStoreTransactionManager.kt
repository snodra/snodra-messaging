package com.damoreira.snodra.messaging.mongo

import com.mongodb.MongoException
import org.springframework.data.mongodb.MongoDatabaseFactory
import org.springframework.data.mongodb.MongoTransactionManager
import org.springframework.data.mongodb.TransientMongoDbException

/**
 * The [MongoTransactionManager] of the MongoDB store (the auto-configured bean and [MongoMessageStore]'s default). A commit
 * that fails with a transient error (a `WriteConflict`, or any error labelled `TransientTransactionError`) is reported as a
 * [TransientMongoDbException], the cause of the `TransactionSystemException` Spring throws, so a caller that retries
 * transient failures (the saga runtime counts them as conflicts) retries a commit-time conflict as it retries a write-time
 * one. Any other commit failure is reported as before.
 *
 * An application that declares its own [MongoTransactionManager] bean keeps it, without this mapping: a transient error at
 * commit then reaches the saga runtime as an ordinary failure (two attempts, then the event is parked).
 */
open class MongoStoreTransactionManager(databaseFactory: MongoDatabaseFactory) : MongoTransactionManager(databaseFactory) {
    override fun doCommit(transactionObject: MongoTransactionObject) {
        try {
            super.doCommit(transactionObject)
        } catch (e: MongoException) {
            throw transient(e) ?: e
        }
    }
}

private const val WRITE_CONFLICT = 112

/** [e] as a [TransientMongoDbException] when it is a write conflict or labelled transient, else null. */
internal fun transient(e: MongoException): TransientMongoDbException? =
    if (e.hasErrorLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL) || e.code == WRITE_CONFLICT) {
        TransientMongoDbException(e.message ?: "transient transaction error", e)
    } else {
        null
    }
