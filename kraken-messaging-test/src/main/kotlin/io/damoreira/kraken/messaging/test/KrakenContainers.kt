package io.damoreira.kraken.messaging.test

/**
 * Shared Testcontainers definitions for kraken integration tests:
 * Kafka (apache/kafka:4.4.0), MongoDB (mongo:8.3.13 as single-node replica set),
 * the kraken-hub image and RustFS. See ../kraken/docs/09-observability-and-testing.md.
 */
object KrakenContainers {
    const val KAFKA_IMAGE = "apache/kafka:4.4.0"
    const val MONGO_IMAGE = "mongo:8.3.13"
    const val HUB_IMAGE = "registry.gitlab.damoreira.ml/kraken/kraken-hub:latest"
    const val RUSTFS_IMAGE = "rustfs/rustfs:1.0.4"

    // TODO: expose started containers (KafkaContainer, MongoDBContainer, GenericContainer for hub and RustFS)
    //       plus an InMemoryHub for unit tests that do not need Docker.
}
