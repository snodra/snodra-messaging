rootProject.name = "snodra-messaging"

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

include("snodra-messaging-api")
include("snodra-messaging")
include("snodra-messaging-kafka")
include("snodra-messaging-rabbitmq")
include("snodra-messaging-mongo")
include("snodra-messaging-jdbc")
include("snodra-messaging-test")


// Example services (never published): see examples/README.md
include("examples:examples-api")
include("examples:service-b")
include("examples:service-a")
include("examples:fake-printer")
include("examples:orders-sql")
include("examples:conformance-service")
