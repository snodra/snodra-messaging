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
