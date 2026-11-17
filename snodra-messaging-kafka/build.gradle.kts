plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":snodra-messaging"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
    // KafkaEventStore speaks the plain Kafka clients (producer, consumer, admin); a service sets their properties with
    // KafkaEventStoreSettings.clientProperties, so the classes are on its compile classpath.
    api("org.apache.kafka:kafka-clients")
    implementation(libs.spring.boot.starter)
    compileOnly("org.springframework.boot:spring-boot-actuator") // optional: the SanitizingFunction that masks the add-on's secrets

    testImplementation(project(":snodra-messaging-test"))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation("org.springframework.boot:spring-boot-actuator")
    testImplementation("tools.jackson.core:jackson-databind")
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.mongodb)
    // The conformance kit's at-least-once check needs a message store: the MongoDB add-on's.
    testImplementation(project(":snodra-messaging-mongo"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            // Write the resolved versions so version-less (BOM-managed) dependencies carry a version in the POM and .module.
            versionMapping {
                usage("java-api") { fromResolutionOf("runtimeClasspath") }
                usage("java-runtime") { fromResolutionResult() }
            }
            artifactId = "snodra-messaging-kafka"
            pom {
                name.set("snodra-messaging-kafka")
                description.set("Kafka event store add-on for snodra-messaging: publish, subscribe, replay, lag and dead-letter topics over Apache Kafka")
                licenses { license { name.set("MIT") } }
            }
        }
    }
    repositories {
        maven {
            name = "nexus"
            url = uri(System.getenv("NEXUS_MAVEN_RELEASE_URL")
                ?: "https://nexus.damoreira.ml/repository/maven-releases/")
            isAllowInsecureProtocol = true
            credentials {
                username = System.getenv("NEXUS_USERNAME")
                password = System.getenv("NEXUS_PASSWORD")
            }
        }
    }
}
