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
    api(libs.kotlinx.coroutines.core)

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
            artifactId = "kraken-messaging-test"
            pom {
                name.set("kraken-messaging-test")
                description.set("Testcontainers helpers for kraken services (Kafka, MongoDB, hub, RustFS)")
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
