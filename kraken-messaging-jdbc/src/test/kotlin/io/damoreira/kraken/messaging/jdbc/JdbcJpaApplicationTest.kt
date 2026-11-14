package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.OutboxEntry
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.hibernate.autoconfigure.HibernateJpaAutoConfiguration
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.orm.jpa.JpaTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import java.time.Instant

/**
 * A JPA application (Boot's DataSource, Hibernate and transaction auto-configurations) with `store.type=jdbc`: the store's
 * transactions are the application's JpaTransactionManager, and the outbox rolls back with it.
 */
class JdbcJpaApplicationTest {
    @Test
    fun aJpaApplicationsStoreJoinsItsJpaTransactions() {
        val db = TestDatabase.POSTGRES.create()
        try {
            ApplicationContextRunner()
                .withConfiguration(
                    AutoConfigurations.of(
                        DataSourceAutoConfiguration::class.java, DataSourceTransactionManagerAutoConfiguration::class.java,
                        HibernateJpaAutoConfiguration::class.java, TransactionAutoConfiguration::class.java,
                        KrakenMessagingAutoConfiguration::class.java, JdbcMessageStoreAutoConfiguration::class.java,
                    ),
                )
                .withPropertyValues(
                    "spring.datasource.url=${db.url}", "spring.datasource.username=${db.user}", "spring.datasource.password=${db.password}",
                    "kraken.messaging.service-name=orders", "kraken.messaging.type-prefix=test", "kraken.messaging.store.type=jdbc",
                )
                .run { context ->
                    assertNull(context.startupFailure)
                    val store = context.getBean(MessageStore::class.java) as JdbcMessageStore
                    val manager = context.getBean(PlatformTransactionManager::class.java)
                    assertTrue(manager is JpaTransactionManager, "$manager")
                    assertSame(manager, store.transactions)
                    TransactionTemplate(manager).executeWithoutResult { status ->
                        runBlocking { store.outbox.append(OutboxEntry("e-1", "t.Happened", "k", emptyMap(), "{}".toByteArray(), Instant.now(), 1)) }
                        status.setRollbackOnly()
                    }
                    TransactionTemplate(manager).executeWithoutResult {
                        runBlocking { store.outbox.append(OutboxEntry("e-2", "t.Happened", "k", emptyMap(), "{}".toByteArray(), Instant.now(), 2)) }
                    }
                    assertEquals(1L, runBlocking { store.outbox.pending() }, "the rolled-back append left nothing")
                }
        } finally {
            db.drop()
        }
    }
}
