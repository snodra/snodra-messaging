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
            artifactId = "snodra-messaging-api"
            pom {
                name.set("snodra-messaging-api")
                description.set("Annotations and marker types shared by all snodra *-api modules")
                licenses { license { name.set("The Apache License, Version 2.0"); url.set("https://www.apache.org/licenses/LICENSE-2.0.txt") } }
            }
        }
    }
}
