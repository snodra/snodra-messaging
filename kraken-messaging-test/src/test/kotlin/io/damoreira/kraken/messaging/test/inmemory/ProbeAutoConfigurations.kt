package io.damoreira.kraken.messaging.test.inmemory

import org.springframework.boot.autoconfigure.AutoConfiguration
import org.springframework.context.annotation.Bean

/** A bean an auto-configuration of the test classpath provides; the test application excludes it in its own properties. */
class ExcludedProbe

class KeptProbe

@AutoConfiguration
open class ExcludedProbeAutoConfiguration {
    @Bean open fun excludedProbe() = ExcludedProbe()
}

@AutoConfiguration
open class KeptProbeAutoConfiguration {
    @Bean open fun keptProbe() = KeptProbe()
}
