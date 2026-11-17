plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":snodra-messaging"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
    // MongoMessageStore takes a MongoTemplate and exposes a MongoTransactionManager.
    api(libs.spring.boot.starter.data.mongodb)
    implementation(libs.jackson.module.kotlin)

    testImplementation(project(":snodra-messaging-test"))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.mockk)
    testImplementation(libs.testcontainers.mongodb)
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
            artifactId = "snodra-messaging-mongo"
            pom {
                name.set("snodra-messaging-mongo")
                description.set("MongoDB message store add-on for snodra-messaging: outbox, processed events, dead letters, processor control, external calls and sagas")
                licenses { license { name.set("MIT") } }
            }
        }
    }
}
