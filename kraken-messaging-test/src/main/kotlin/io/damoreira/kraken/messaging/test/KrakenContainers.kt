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

    fun kafka(): KafkaContainer = KafkaContainer(DockerImageName.parse(KAFKA_IMAGE)).withReuse(true)

    fun mongo(): MongoDBContainer = MongoDBContainer(DockerImageName.parse(MONGO_IMAGE)).withReplicaSet().withReuse(true)

    fun hub(): GenericContainer<*> = GenericContainer(DockerImageName.parse(hubImage))
        .withExposedPorts(7000)
        .waitingFor(Wait.forLogMessage(".*hub listening.*", 1))

    fun hubImageAvailable(): Boolean = try {
        DockerClientFactory.instance().client().inspectImageCmd(hubImage).exec()
        true
    } catch (_: Exception) {
        false
    }
}
