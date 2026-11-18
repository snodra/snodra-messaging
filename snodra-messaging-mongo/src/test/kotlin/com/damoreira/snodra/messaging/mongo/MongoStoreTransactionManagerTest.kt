package com.damoreira.snodra.messaging.mongo

import com.mongodb.MongoException
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.dao.TransientDataAccessException
import org.springframework.data.mongodb.MongoDatabaseFactory

/** A commit's transient failure is reported as one (no database: the commit is the transaction object's). */
class MongoStoreTransactionManagerTest {
    /** Commits a transaction whose commit fails with the given exception (the transaction object is the manager's own type). */
    private class Probe : MongoStoreTransactionManager(mockk<MongoDatabaseFactory>(relaxed = true)) {
        fun commitFailingWith(e: MongoException) = doCommit(mockk<MongoTransactionObject>(relaxed = true) { every { commitTransaction() } throws e })
    }

    @Test
    fun aLabelledCommitFailureIsTransient() {
        val labelled = MongoException(251, "NoSuchTransaction").apply { addLabel(MongoException.TRANSIENT_TRANSACTION_ERROR_LABEL) }
        val e = assertThrows<TransientDataAccessException> { Probe().commitFailingWith(labelled) }
        assertSame(labelled, e.cause)
    }

    @Test
    fun aWriteConflictAtCommitIsTransient() {
        val conflict = MongoException(112, "WriteConflict")
        assertSame(conflict, assertThrows<TransientDataAccessException> { Probe().commitFailingWith(conflict) }.cause)
    }

    @Test
    fun anyOtherCommitFailureIsReportedAsItIs() {
        val other = MongoException(50, "MaxTimeMSExpired")
        assertSame(other, assertThrows<MongoException> { Probe().commitFailingWith(other) })
    }
}
