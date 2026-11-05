rootProject.name = "kraken-messaging"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        // In CI the group's Nexus proxy/group is the single entry point for Maven Central and the
        // kraken artifacts. Locally, fall back to mavenLocal()/mavenCentral() when it is unset.
        val nexusUrl = System.getenv("NEXUS_MAVEN_PUBLIC_URL")
        if (nexusUrl != null) {
            maven {
                name = "nexus"
                url = uri(nexusUrl)
                credentials {
                    username = System.getenv("NEXUS_USERNAME")
                    password = System.getenv("NEXUS_PASSWORD")
                }
            }
        }
        mavenLocal()
        mavenCentral()
    }
}

include("kraken-messaging-api")
include("kraken-messaging")
include("kraken-messaging-test")


// Example services (never published): see examples/README.md
include("examples:examples-api")
include("examples:service-b")
include("examples:service-a")
include("examples:fake-printer")
