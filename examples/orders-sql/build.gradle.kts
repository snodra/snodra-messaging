plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

// Same quirk as the starter: the Boot BOM would downgrade the Kotlin compiler classpath below the plugin version.
extra["kotlin.version"] = libs.versions.kotlin.get()

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}")
    }
}

dependencies {
    implementation(project(":kraken-messaging"))
    // The message store: the outbox, the processed events, the dead letters and the sagas in the service's PostgreSQL (ADR 0028).
    implementation(project(":kraken-messaging-jdbc"))
    implementation(libs.jackson.module.kotlin)
    implementation(libs.kotlin.reflect)
    runtimeOnly(libs.postgresql)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.awaitility)
    testImplementation(project(":kraken-messaging-test"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
