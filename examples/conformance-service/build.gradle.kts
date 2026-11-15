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
    // Commands, queries, streams and subscriptions only: no event store, no message store (level 1).
    implementation(project(":kraken-messaging"))
    implementation(libs.spring.boot.starter)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.kotlin.reflect)
    // HubConnection.lastRejection is a gRPC Status.
    implementation(libs.grpc.stub)
    // Callers' tokens validated against CONFORMANCE_JWKS_URL (a JwtDecoder bean the library picks up).
    implementation(libs.spring.security.oauth2.jose)
    implementation(libs.spring.security.oauth2.resource.server)

    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.testcontainers.junit)
    testImplementation(project(":kraken-messaging-test"))
    testImplementation("tools.jackson.core:jackson-databind")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

// The runner starts the executable jar once or more per check; the level-1 test needs it built.
tasks.test {
    dependsOn(tasks.bootJar)
    systemProperty("conformance.jar", tasks.bootJar.get().archiveFile.get().asFile.absolutePath)
    systemProperty("conformance.work", layout.buildDirectory.dir("conformance").get().asFile.absolutePath)
    // The runner binary (kraken-hub, conformance/): absent, the level-1 test is skipped saying so.
    System.getenv("KRAKEN_CONFORMANCE_BIN")?.let { systemProperty("conformance.runner", it) }
    // The run is long (every check of level 1, then the two-hub part): its report is printed.
    testLogging { showStandardStreams = true }
}
