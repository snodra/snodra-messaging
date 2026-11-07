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
    api(project(":kraken-messaging-api"))
    api(libs.kotlinx.coroutines.core)
    implementation(libs.spring.boot.starter)
    implementation(libs.spring.kafka)
    // ExternalRecordTranslator.translate takes a Kafka ConsumerRecord: a service writing one needs the client on its compile classpath.
    api("org.apache.kafka:kafka-clients")
    implementation(libs.spring.boot.starter.data.mongodb)
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
    testImplementation("org.springframework.boot:spring-boot-micrometer-metrics") // Boot's registry in the end-to-end applications
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.mockk)
    testImplementation("jakarta.validation:jakarta.validation-api")
    testImplementation(libs.awaitility)
    testImplementation(libs.grpc.inprocess)
    testImplementation(project(":kraken-messaging-test"))
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
    filesMatching("META-INF/kraken-messaging/version.properties") { expand("version" to version) }
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
            artifactId = "kraken-messaging"
            pom {
                name.set("kraken-messaging")
                description.set("Spring Boot starter: EventBus, CommandBus, QueryBus over Kafka and the kraken hub")
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

// src/main/proto/hub.proto must stay byte-identical to the umbrella's kraken/protocol/hub.proto (same check as
// scripts/check-proto-sync.sh). Skipped with a log line when the sibling checkout is absent (e.g. the examples
// Docker build, which copies only this repo).
val checkProtoSync = tasks.register("checkProtoSync") {
    group = "verification"
    description = "Compares the SHA-256 of src/main/proto/hub.proto with ../kraken/protocol/hub.proto"
    val copy = layout.projectDirectory.file("src/main/proto/hub.proto").asFile
    val source = rootProject.layout.projectDirectory.file(
        System.getenv("KRAKEN_PROTO_SOURCE") ?: "../kraken/protocol/hub.proto",
    ).asFile
    inputs.file(copy)
    doLast {
        if (!source.exists()) {
            logger.lifecycle("proto sync: umbrella proto not found at $source, skipping")
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
