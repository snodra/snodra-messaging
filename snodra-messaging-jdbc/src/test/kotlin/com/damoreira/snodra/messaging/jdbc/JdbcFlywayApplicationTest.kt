package com.damoreira.snodra.messaging.jdbc

import com.damoreira.snodra.messaging.config.SnodraMessagingAutoConfiguration
import com.damoreira.snodra.messaging.store.MessageStore
import com.damoreira.snodra.messaging.store.OutboxEntry
import kotlinx.coroutines.runBlocking
import org.flywaydb.core.Flyway
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.InitializingBean
import org.springframework.beans.factory.ObjectProvider
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.flyway.autoconfigure.FlywayAutoConfiguration
import org.springframework.boot.flyway.autoconfigure.FlywayMigrationInitializer
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import org.springframework.boot.transaction.autoconfigure.TransactionAutoConfiguration
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.simple.JdbcClient
import java.time.Instant
import javax.sql.DataSource

/** The README's snodra migration: a bean of its own type (never a `Flyway` bean, which would turn Boot's Flyway off). */
class SnodraSchemaMigration(
    private val dataSource: DataSource,
    private val applicationMigrations: ObjectProvider<FlywayMigrationInitializer>,
) : InitializingBean {
    override fun afterPropertiesSet() {
        applicationMigrations.ifAvailable { } // the application's own migrations first: they need the schema without snodra's tables
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/snodra/postgresql")
            .table("snodra_schema_history")
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load()
            .migrate()
    }
}

@Configuration(proxyBeanMethods = false)
class SnodraSchemaMigrationConfiguration {
    @Bean
    fun snodraSchemaMigration(dataSource: DataSource, applicationMigrations: ObjectProvider<FlywayMigrationInitializer>) =
        SnodraSchemaMigration(dataSource, applicationMigrations)
}

/** An application bean that needs the store, created before the migration bean (it is declared first). */
@Configuration(proxyBeanMethods = false)
class EarlyStoreUser {
    @Bean
    fun ordersService(store: MessageStore) = store.name
}

/**
 * A Spring Boot application with its own Flyway migrations (its own V1) and snodra's files under a separate history, as the
 * README says, with `init-schema=false`, on a fresh database: both run, and the store's startup check passes after them.
 */
class JdbcFlywayApplicationTest {
    private fun runner(db: TestDatabase.Connection) = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                DataSourceAutoConfiguration::class.java, DataSourceTransactionManagerAutoConfiguration::class.java, FlywayAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java, SnodraMessagingAutoConfiguration::class.java, JdbcMessageStoreAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues(
            "spring.datasource.url=${db.url}", "spring.datasource.username=${db.user}", "spring.datasource.password=${db.password}",
            "spring.flyway.locations=classpath:db/app",
            "snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=test", "snodra.messaging.store.init-schema=false",
        )

    @Test
    fun theApplicationsMigrationsAndSnodrasBothRun() {
        val db = TestDatabase.POSTGRES.create()
        try {
            // A bean that needs the store is created first: the schema check still runs after the migration (after every bean).
            runner(db).withUserConfiguration(EarlyStoreUser::class.java, SnodraSchemaMigrationConfiguration::class.java).run { context ->
                assertNull(context.startupFailure)
                val jdbc = JdbcClient.create(context.getBean(DataSource::class.java))
                assertEquals(listOf("1"), jdbc.sql("SELECT version FROM flyway_schema_history WHERE version IS NOT NULL").query(String::class.java).list())
                assertEquals(listOf("0", "1"), jdbc.sql("SELECT version FROM snodra_schema_history WHERE version IS NOT NULL ORDER BY installed_rank").query(String::class.java).list())
                assertEquals(0L, jdbc.sql("SELECT COUNT(*) FROM app_things").query(Long::class.javaObjectType).single())
                val store = context.getBean(MessageStore::class.java)
                runBlocking { store.outbox.append(OutboxEntry("e-1", "t.Happened", "k", emptyMap(), "{}".toByteArray(), Instant.now(), 1)) }
            }
        } finally {
            db.drop()
        }
    }

    @Test
    fun withoutTheMigrationTheStartupSaysWhatIsMissing() {
        val db = TestDatabase.POSTGRES.create()
        try {
            runner(db).run { context ->
                val failure = context.startupFailure!!.stackTraceToString()
                assertTrue(failure.contains("schema is at version 0, it needs 1 or later"), failure)
            }
        } finally {
            db.drop()
        }
    }
}
