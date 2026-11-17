plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":snodra-messaging"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
    // RabbitEventStore speaks the stream protocol through the RabbitMQ Stream Java client's Client (see the class docs for why
    // not its Environment).
    // Exactly this version (the add-on uses the client's internal Client): strictly, so Gradle consumers cannot resolve another;
    // a service with Spring's dependency management sets rabbit-stream-client.version (see the README), checked at startup.
    implementation(libs.rabbitmq.stream.client) { version { strictly(libs.versions.rabbitmq.stream.client.get()) } }
    implementation(libs.spring.boot.starter)
    compileOnly("org.springframework.boot:spring-boot-actuator") // optional: the SanitizingFunction that masks the add-on's secrets
    // The management API's consumer list (capability replay) is JSON.
    implementation("tools.jackson.core:jackson-databind")

    testImplementation(project(":snodra-messaging-test"))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation("org.springframework.boot:spring-boot-actuator")
    testImplementation(libs.testcontainers.junit)
    // Two event-store add-ons on one classpath need snodra.messaging.event-store.
    testImplementation(project(":snodra-messaging-kafka"))
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
            artifactId = "snodra-messaging-rabbitmq"
            pom {
                name.set("snodra-messaging-rabbitmq")
                description.set("RabbitMQ Streams event store add-on for snodra-messaging: publish, subscribe, replay, lag and a dead-letter stream over RabbitMQ 3.13+ with the stream plugin")
                licenses { license { name.set("The Apache License, Version 2.0"); url.set("https://www.apache.org/licenses/LICENSE-2.0.txt") } }
            }
        }
    }
}
