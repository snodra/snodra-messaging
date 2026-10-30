plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
    `maven-publish`
}

// Pure Kotlin: no Spring, Kafka or gRPC. Consumed by every *-api module.
dependencies {
    testImplementation(kotlin("test"))
}

java {
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "kraken-messaging-api"
            pom {
                name.set("kraken-messaging-api")
                description.set("Annotations and marker types shared by all kraken *-api modules")
                licenses { license { name.set("MIT") } }
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
