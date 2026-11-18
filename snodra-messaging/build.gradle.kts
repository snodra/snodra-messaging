import java.security.MessageDigest

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.dependency.management)
    `java-library`
    `maven-publish`
    alias(libs.plugins.protobuf)
}

// The Boot BOM manages kotlin.version (2.3.x) and, via the dependency-management plugin, also downgrades the
// Kotlin compiler classpath below the Gradle plugin's 2.4.21, which makes compileKotlin fail with a
// getPluginClasspaths() NullPointerException. Pin it to the catalog version.
extra["kotlin.version"] = libs.versions.kotlin.get()

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}")
    }
}

dependencies {
    api(project(":snodra-messaging-api"))
    api(libs.kotlinx.coroutines.core)
    implementation(libs.spring.boot.starter)
    // MessageStore.transactions is a PlatformTransactionManager: the store interfaces speak spring-tx.
    api("org.springframework:spring-tx")
    implementation(libs.jackson.module.kotlin)
    implementation(libs.kotlin.reflect)
    // gRPC stubs generated from src/main/proto/hub.proto (see proto/README.md).
    implementation(libs.grpc.stub)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.kotlin.stub)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.protobuf.kotlin)
    implementation(libs.spring.security.oauth2.resource.server)
    implementation(libs.spring.security.oauth2.jose)
    compileOnly(libs.micrometer.tracing)
    compileOnly("io.micrometer:micrometer-core") // optional: the messaging, saga and hub client meters (P9) exist only when a registry does
    compileOnly("org.springframework.boot:spring-boot-actuator") // optional: HubTokenSanitizingConfiguration masks the hub token in configprops/env
    compileOnly("jakarta.validation:jakarta.validation-api") // optional: violation messages go into HANDLER_ERROR details when present

    testImplementation(libs.spring.boot.starter.test)
    testImplementation("org.springframework.boot:spring-boot-actuator")
    testImplementation("io.micrometer:micrometer-core")
    testImplementation(libs.micrometer.tracing) // TraceFloorTest: with tracing present the propagator decides
    testImplementation("org.springframework.boot:spring-boot-micrometer-metrics") // Boot's registry in the end-to-end applications
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.mockk)
    testImplementation("jakarta.validation:jakarta.validation-api")
    testImplementation(libs.awaitility)
    testImplementation(libs.grpc.inprocess)
    testImplementation(project(":snodra-messaging-test"))
    // The tests that need a real store run on MongoDB through the add-on (which depends on this module's main code).
    testImplementation(project(":snodra-messaging-mongo"))
    // ... and on Kafka through the event-store add-on (the core has no event-store client of its own, ADR 0027).
    testImplementation(project(":snodra-messaging-kafka"))
    testImplementation(libs.spring.boot.testcontainers)
    testImplementation(libs.testcontainers.kafka)
    testImplementation(libs.testcontainers.mongodb)
    testImplementation(libs.nimbus.jose.jwt)
    testImplementation(libs.bouncycastle.pkix) // test certificates with a URI SAN and a short validity (mTLS, P5c)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val protobufVersion = libs.versions.protobuf.asProvider().get()
val grpcVersion = libs.versions.grpc.asProvider().get()
val grpcKotlinVersion = libs.versions.grpc.kotlin.get()

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:$protobufVersion" }
    plugins {
        create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:$grpcVersion" }
        create("grpckt") { artifact = "io.grpc:protoc-gen-grpc-kotlin:${grpcKotlinVersion}:jdk8@jar" }
    }
    generateProtoTasks {
        all().forEach {
            it.plugins { create("grpc"); create("grpckt") }
            it.builtins { create("kotlin") }
        }
    }
}

java {
    withSourcesJar()
}

// Register.client_version: HubProtocol reads the library version from this resource.
tasks.processResources {
    val version = project.version.toString()
    inputs.property("version", version)
    filesMatching("META-INF/snodra-messaging/version.properties") { expand("version" to version) }
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
            artifactId = "snodra-messaging"
            pom {
                name.set("snodra-messaging")
                description.set("Spring Boot starter: EventBus, CommandBus, QueryBus over an event-store add-on and Snodra Server")
                licenses { license { name.set("The Apache License, Version 2.0"); url.set("https://www.apache.org/licenses/LICENSE-2.0.txt") } }
            }
        }
    }
}

// src/main/proto/hub.proto must stay byte-identical to the umbrella's snodra/protocol/hub.proto (same check as
// scripts/check-proto-sync.sh). Skipped with a log line when the sibling checkout is absent (e.g. the examples
// Docker build, which copies only this repo).
val checkProtoSync = tasks.register("checkProtoSync") {
    group = "verification"
    description = "Compares the SHA-256 of src/main/proto/hub.proto with ../snodra-server/proto/hub.proto"
    val copy = layout.projectDirectory.file("src/main/proto/hub.proto").asFile
    val source = rootProject.layout.projectDirectory.file(
        System.getenv("SNODRA_PROTO_SOURCE") ?: "../snodra-server/proto/hub.proto",
    ).asFile
    inputs.file(copy)
    doLast {
        if (!source.exists()) {
            logger.lifecycle("proto sync: snodra-server proto not found at $source, skipping")
            return@doLast
        }
        fun sha256(f: File) = MessageDigest.getInstance("SHA-256").digest(f.readBytes())
            .joinToString("") { "%02x".format(it) }
        val want = sha256(source)
        val have = sha256(copy)
        if (want != have) {
            throw GradleException("proto sync: $copy (sha256 $have) drifted from $source (sha256 $want); run: cp $source $copy")
        }
        logger.lifecycle("proto sync: hub.proto matches $source (sha256 $want)")
    }
}
tasks.named("check") { dependsOn(checkProtoSync) }
