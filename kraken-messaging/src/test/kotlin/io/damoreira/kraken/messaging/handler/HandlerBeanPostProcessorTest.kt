package io.damoreira.kraken.messaging.handler

import io.damoreira.kraken.messaging.DuplicateHandlerException
import io.damoreira.kraken.messaging.codec.MessageTypeResolver
import io.damoreira.kraken.messaging.fixtures.Pong
import io.damoreira.kraken.messaging.fixtures.Ping
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.aop.framework.ProxyFactory
import org.springframework.aop.support.AopUtils
import org.springframework.beans.factory.BeanCreationException
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.core.NestedExceptionUtils
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertSame
import kotlin.test.assertTrue

open class ProxiedHandler {
    @CommandHandler open fun handle(command: Ping): Pong = Pong("proxied", Instant.EPOCH)
}

open class OtherPingHandler {
    @CommandHandler open fun handle(command: Ping): Pong = Pong("other", Instant.EPOCH)
}

@Configuration
open class PostProcessorConfig {
    companion object {
        @JvmStatic @Bean
        fun registry() = HandlerRegistry(MessageTypeResolver("test"))

        @JvmStatic @Bean
        fun handlerBeanPostProcessor(registry: HandlerRegistry) = HandlerBeanPostProcessor(registry)
    }

    @Bean open fun proxied(): ProxiedHandler =
        ProxyFactory(ProxiedHandler()).apply { isProxyTargetClass = true }.proxy as ProxiedHandler
}

@Configuration
open class DuplicatingConfig {
    companion object {
        @JvmStatic @Bean
        fun registry() = HandlerRegistry(MessageTypeResolver("test"))

        @JvmStatic @Bean
        fun handlerBeanPostProcessor(registry: HandlerRegistry) = HandlerBeanPostProcessor(registry)
    }

    @Bean open fun one() = ProxiedHandler()
    @Bean open fun two() = OtherPingHandler()
}

class HandlerBeanPostProcessorTest {
    @Test fun `beans with handlers are discovered including proxied ones`() {
        AnnotationConfigApplicationContext(PostProcessorConfig::class.java).use { context ->
            val registry = context.getBean(HandlerRegistry::class.java)
            assertEquals(setOf("test.Ping"), registry.commandTypes())
            val handler = registry.commandHandler("test.Ping")!!
            assertTrue(AopUtils.isAopProxy(handler.bean))
            val pong = kotlinx.coroutines.runBlocking {
                handler.invoke(Ping("x"), io.damoreira.kraken.messaging.MessageContext("m", "test.Ping", "c", null, emptySet(), null, emptyMap()))
            } as Pong
            assertEquals("proxied", pong.echoed)
        }
    }

    @Test fun `duplicate handlers across beans fail context startup`() {
        val ex = assertThrows<BeanCreationException> { AnnotationConfigApplicationContext(DuplicatingConfig::class.java) }
        assertTrue(NestedExceptionUtils.getMostSpecificCause(ex) is DuplicateHandlerException)
    }

    @Test fun `registry is resolved only for beans that declare handlers`() {
        val processor = HandlerBeanPostProcessor { error("registry must not be resolved for a plain bean") }
        val plain = Any()
        assertSame(plain, processor.postProcessAfterInitialization(plain, "plain"))
        assertSame(this, processor.postProcessAfterInitialization(this, "test"))
    }

    @Test fun `introspection failures skip the bean but validation errors propagate`() {
        val handler = ProxiedHandler()
        val skipping = HandlerBeanPostProcessor { throw UnsupportedOperationException("synthetic class") }
        assertSame(handler, skipping.postProcessAfterInitialization(handler, "handler"))

        val failing = HandlerBeanPostProcessor { throw IllegalStateException("Handler X.y must take (message)") }
        assertThrows<IllegalStateException> { failing.postProcessAfterInitialization(handler, "handler") }
    }
}
