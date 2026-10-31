package io.damoreira.kraken.messaging.test

import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.kafka.KafkaContainer
import org.testcontainers.mongodb.MongoDBContainer
import org.testcontainers.utility.DockerImageName

/**
 * Shared Testcontainers definitions for kraken integration tests. Containers are returned unstarted;
 * callers start them. Kafka and MongoDB (single-node replica set) are marked reusable.
 */
object KrakenContainers {
    const val KAFKA_IMAGE = "apache/kafka:4.4.0"
    const val MONGO_IMAGE = "mongo:8.3.13"

    private val hubImage: String get() = System.getenv("KRAKEN_HUB_IMAGE") ?: "kraken-hub:dev"

    /** The previous hub release, for compatibility tests of new clients against an old hub (legacy mode). */
    private val hubBaselineImage: String get() = System.getenv("KRAKEN_HUB_BASELINE_IMAGE") ?: "kraken-hub:v1-baseline"

    /** The merged P0 hub (protocol v2 without flow control), for compatibility tests of P1 clients. */
    private val hubP0Image: String get() = System.getenv("KRAKEN_HUB_P0_IMAGE") ?: "kraken-hub:p0"

    fun kafka(): KafkaContainer = KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withReuse(true)

    fun mongo(): MongoDBContainer = MongoDBContainer(DockerImageName.parse(MONGO_IMAGE)).withReplicaSet().withReuse(true)

    /** The hub image under test: `KRAKEN_HUB_IMAGE`, default `kraken-hub:dev`. */
    fun hub(): GenericContainer<*> = hub(hubImage)

    /** The baseline (previous) hub image: `KRAKEN_HUB_BASELINE_IMAGE`, default `kraken-hub:v1-baseline`. */
    fun hubBaseline(): GenericContainer<*> = hub(hubBaselineImage)

    /** The P0 hub image: `KRAKEN_HUB_P0_IMAGE`, default `kraken-hub:p0`. */
    fun hubP0(): GenericContainer<*> = hub(hubP0Image)

    /**
     * The hub under test with extra `KRAKEN_HUB_*` settings (for example `KRAKEN_HUB_QUEUE_CAPACITY_PER_INSTANCE` or
     * `KRAKEN_HUB_TIMEOUT_OVERRIDES`); OTLP export is disabled.
     */
    fun hub(env: Map<String, String>): GenericContainer<*> = hub().withEnv("KRAKEN_HUB_OTLP_ENDPOINT", "").withEnv(env)

    fun hubImageAvailable(): Boolean = imageAvailable(hubImage)

    fun hubP0ImageAvailable(): Boolean = imageAvailable(hubP0Image)

    fun hubBaselineImageAvailable(): Boolean = imageAvailable(hubBaselineImage)

    private fun hub(image: String): GenericContainer<*> = GenericContainer(DockerImageName.parse(image))
        .withExposedPorts(7000)
        .waitingFor(Wait.forLogMessage(".*hub listening.*", 1))

    private fun imageAvailable(image: String): Boolean = try {
        DockerClientFactory.instance().client().inspectImageCmd(image).exec()
        true
    } catch (_: Exception) {
        false
    }
}
