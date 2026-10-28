plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.dependency.management)
    `java-library`
    `maven-publish`
}

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
    implementation(libs.spring.boot.starter.grpc.client)
    implementation(libs.spring.boot.starter.data.mongodb)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.kotlin.reflect)
    // gRPC stubs for protocol/hub.proto will be generated here once the proto is wired (see proto/README.md).
    implementation(libs.grpc.stub)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.kotlin.stub)
    implementation(libs.protobuf.kotlin)
    annotationProcessor(libs.spring.boot.configuration.processor)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.mockk)
}

java {
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "kraken-messaging"
            pom {
                name.set("kraken-messaging")
                description.set("Spring Boot starter: EventBus, CommandBus, QueryBus over Kafka and the kraken hub")
                licenses { license { name.set("Apache-2.0") } }
            }
        }
    }
    repositories {
        maven {
            name = "gitlab"
            url = uri(System.getenv("KRAKEN_GITLAB_MAVEN_URL")
                ?: "https://gitlab.damoreira.ml/api/v4/projects/0/packages/maven")
            credentials(HttpHeaderCredentials::class) {
                name = if (System.getenv("CI_JOB_TOKEN") != null) "Job-Token" else "Private-Token"
                value = System.getenv("CI_JOB_TOKEN") ?: System.getenv("KRAKEN_GITLAB_TOKEN") ?: ""
            }
            authentication { create<HttpHeaderAuthentication>("header") }
        }
    }
}
