package io.damoreira.kraken.messaging.jdbc

import io.damoreira.kraken.messaging.config.KrakenMessagingAutoConfiguration
import io.damoreira.kraken.messaging.store.MessageStore
import io.damoreira.kraken.messaging.store.OutboxEntry
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

/** The README's kraken migration: a bean of its own type (never a `Flyway` bean, which would turn Boot's Flyway off). */
class KrakenSchemaMigration(
    private val dataSource: DataSource,
    private val applicationMigrations: ObjectProvider<FlywayMigrationInitializer>,
) : InitializingBean {
    override fun afterPropertiesSet() {
        applicationMigrations.ifAvailable { } // the application's own migrations first: they need the schema without kraken's tables
        Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/kraken/postgresql")
            .table("kraken_schema_history")
            .baselineOnMigrate(true)
            .baselineVersion("0")
            .load()
            .migrate()
    }
}

@Configuration(proxyBeanMethods = false)
class KrakenSchemaMigrationConfiguration {
    @Bean
    fun krakenSchemaMigration(dataSource: DataSource, applicationMigrations: ObjectProvider<FlywayMigrationInitializer>) =
        KrakenSchemaMigration(dataSource, applicationMigrations)
}

/** An application bean that needs the store, created before the migration bean (it is declared first). */
@Configuration(proxyBeanMethods = false)
class EarlyStoreUser {
    @Bean
    fun ordersService(store: MessageStore) = store.name
}

/**
 * A Spring Boot application with its own Flyway migrations (its own V1) and kraken's files under a separate history, as the
 * README says, with `init-schema=false`, on a fresh database: both run, and the store's startup check passes after them.
 */
class JdbcFlywayApplicationTest {
    private fun runner(db: TestDatabase.Connection) = ApplicationContextRunner()
        .withConfiguration(
            AutoConfigurations.of(
                DataSourceAutoConfiguration::class.java, DataSourceTransactionManagerAutoConfiguration::class.java, FlywayAutoConfiguration::class.java,
                TransactionAutoConfiguration::class.java, KrakenMessagingAutoConfiguration::class.java, JdbcMessageStoreAutoConfiguration::class.java,
            ),
        )
        .withPropertyValues(
            "spring.datasource.url=${db.url}", "spring.datasource.username=${db.user}", "spring.datasource.password=${db.password}",
            "spring.flyway.locations=classpath:db/app",
            "kraken.messaging.service-name=orders", "kraken.messaging.type-prefix=test", "kraken.messaging.store.init-schema=false",
        )

    @Test
    fun theApplicationsMigrationsAndKrakensBothRun() {
        val db = TestDatabase.POSTGRES.create()
        try {
            // A bean that needs the store is created first: the schema check still runs after the migration (after every bean).
            runner(db).withUserConfiguration(EarlyStoreUser::class.java, KrakenSchemaMigrationConfiguration::class.java).run { context ->
                assertNull(context.startupFailure)
                val jdbc = JdbcClient.create(context.getBean(DataSource::class.java))
                assertEquals(listOf("1"), jdbc.sql("SELECT version FROM flyway_schema_history WHERE version IS NOT NULL").query(String::class.java).list())
                assertEquals(listOf("0", "1"), jdbc.sql("SELECT version FROM kraken_schema_history WHERE version IS NOT NULL ORDER BY installed_rank").query(String::class.java).list())
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
