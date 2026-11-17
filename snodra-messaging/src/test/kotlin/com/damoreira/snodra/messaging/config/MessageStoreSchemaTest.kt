package com.damoreira.snodra.messaging.config

import com.damoreira.snodra.messaging.store.MessageStore
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.support.StaticListableBeanFactory
import org.springframework.boot.autoconfigure.AutoConfigurations
import org.springframework.boot.test.context.runner.ApplicationContextRunner
import kotlin.test.assertNull

/** The core applies the store's schema at startup (`snodra.messaging.store.init-schema`), whoever declared the store. */
class MessageStoreSchemaTest {
    private val runner = ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(SnodraMessagingAutoConfiguration::class.java))
        .withPropertyValues("snodra.messaging.service-name=orders", "snodra.messaging.type-prefix=test")

    @Test
    fun anApplicationDeclaredStoreGetsItsSchemaOnce() {
        val store: MessageStore = mockk(relaxed = true)
        runner.withBean(MessageStore::class.java, { store }).run { context ->
            assertNull(context.startupFailure)
            verify(exactly = 1) { store.initSchema() }
        }
    }

    @Test
    fun initSchemaFalseLeavesTheSchemaToTheService() {
        val store: MessageStore = mockk(relaxed = true)
        runner.withBean(MessageStore::class.java, { store }).withPropertyValues("snodra.messaging.store.init-schema=false").run { context ->
            assertNull(context.startupFailure)
            verify(exactly = 0) { store.initSchema() }
        }
    }

    @Test
    fun theSchemaFollowsTheBoundSetting() {
        val store: MessageStore = mockk(relaxed = true)
        val provider = StaticListableBeanFactory(mapOf("store" to store)).getBeanProvider(MessageStore::class.java)
        fun properties(initSchema: Boolean) = SnodraMessagingProperties("orders", "test", store = SnodraMessagingProperties.Store(initSchema = initSchema))
        MessageStoreSchema(provider, properties(initSchema = false)).afterSingletonsInstantiated()
        verify(exactly = 0) { store.initSchema() }
        MessageStoreSchema(provider, properties(initSchema = true)).afterSingletonsInstantiated()
        verify(exactly = 1) { store.initSchema() }
    }

    @Test
    fun noStoreNoSchema() {
        runner.run { context -> assertNull(context.startupFailure) }
    }
}
