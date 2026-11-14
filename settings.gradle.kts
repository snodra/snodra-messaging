rootProject.name = "kraken-messaging"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenLocal()
        mavenCentral()
    }
}

include("kraken-messaging-api")
include("kraken-messaging")
include("kraken-messaging-kafka")
include("kraken-messaging-mongo")
include("kraken-messaging-jdbc")
include("kraken-messaging-test")


// Example services (never published): see examples/README.md
include("examples:examples-api")
include("examples:service-b")
include("examples:service-a")
include("examples:fake-printer")
