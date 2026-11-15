plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":kraken-messaging"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
    // RabbitEventStore speaks the stream protocol through the RabbitMQ Stream Java client's Client (see the class docs for why
    // not its Environment).
    implementation(libs.rabbitmq.stream.client)
    implementation(libs.spring.boot.starter)
    // The management API's consumer list (capability replay) is JSON.
    implementation("tools.jackson.core:jackson-databind")

    testImplementation(project(":kraken-messaging-test"))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.testcontainers.junit)
    // Two event-store add-ons on one classpath need kraken.messaging.event-store.
    testImplementation(project(":kraken-messaging-kafka"))
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
            artifactId = "kraken-messaging-rabbitmq"
            pom {
                name.set("kraken-messaging-rabbitmq")
                description.set("RabbitMQ Streams event store add-on for kraken-messaging: publish, subscribe, replay, lag and a dead-letter stream over RabbitMQ 3.13+ with the stream plugin")
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
