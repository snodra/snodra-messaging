package io.damoreira.kraken.messaging

import kotlin.test.Test
import kotlin.test.assertNotNull

class BuildSmokeTest {
    @Test fun `grpc kotlin stub is generated`() {
        assertNotNull(Class.forName("io.damoreira.kraken.hub.v1.HubGrpcKt"))
    }

    @Test fun `jackson 3 kotlin module is on the classpath`() {
        assertNotNull(Class.forName("tools.jackson.module.kotlin.KotlinModule"))
    }
}
