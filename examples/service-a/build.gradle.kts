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
    implementation(project(":snodra-messaging"))
    // The message store: the outbox, the processed events, the dead letters and the sagas in the service's MongoDB (ADR 0028).
    implementation(project(":snodra-messaging-mongo"))
    // The event store: the service's events on Kafka (ADR 0027).
    implementation(project(":snodra-messaging-kafka"))
    implementation(project(":examples:examples-api"))
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.actuator)
    // Metrics over OTLP/HTTP when SNODRA_OTLP_METRICS_ENABLED is set (application.yaml); OpenTelemetry's resource attributes.
    implementation("io.micrometer:micrometer-registry-otlp")
    implementation("org.springframework.boot:spring-boot-opentelemetry")
    implementation(libs.jackson.module.kotlin)
    implementation(libs.kotlin.reflect)
    // Spring MVC runs `suspend fun` controller methods through Reactor; the bridge brings reactor-core and reactive-streams.
    implementation(libs.kotlinx.coroutines.reactor)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation("org.springframework.boot:spring-boot-starter-webmvc-test") // @WebMvcTest + MockMvcTester (Boot 4 slice starter)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(project(":snodra-messaging-test"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
