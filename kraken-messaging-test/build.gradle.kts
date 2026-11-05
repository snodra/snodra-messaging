plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":kraken-messaging"))
    api(libs.testcontainers.junit)
    api(libs.testcontainers.kafka)
    api(libs.testcontainers.mongodb)
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
    api("org.apache.kafka:kafka-clients")
    // InMemoryBuses synthesises a KrakenForwardedAuthentication (a Spring Security Authentication) for annotated handlers.
    implementation("org.springframework.security:spring-security-core")
    // SagaTestFixture runs the library's SagaRuntime, whose ports speak the saga documents (bson) and TransactionOperations (spring-tx).
    implementation(libs.spring.boot.starter.data.mongodb)
    api(libs.kotlinx.coroutines.core)
    api(libs.grpc.inprocess)
    implementation(libs.grpc.stub)
    implementation(libs.grpc.protobuf)
    // UnverifiedJwt reads the claims of a jwt credential.
    implementation("tools.jackson.core:jackson-databind")
    implementation(libs.grpc.kotlin.stub)

    testImplementation(libs.kotlin.test.junit5)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    // The starter is a project dependency, so its BOM-managed (version-less) transitives need the BOM here too.
    testRuntimeOnly(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
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
            artifactId = "kraken-messaging-test"
            pom {
                name.set("kraken-messaging-test")
                description.set("Testcontainers helpers for kraken services (Kafka, MongoDB, hub, RustFS)")
                licenses { license { name.set("MIT") } }
            }
        }
    }
    repositories {
        maven {
            name = "nexus"
            url = uri(System.getenv("NEXUS_MAVEN_RELEASE_URL")
                ?: "https://nexus.damoreira.ml/repository/maven-releases/")
            credentials {
                username = System.getenv("NEXUS_USERNAME")
                password = System.getenv("NEXUS_PASSWORD")
            }
        }
    }
}
