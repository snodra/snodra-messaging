rootProject.name = "snodra-messaging-workspace"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

plugins {
    id("com.gradleup.nmcp.settings") version "1.6.2"
}

nmcpSettings {

    centralPortal {
        username = System.getenv("MAVEN_CENTRAL_USERNAME") ?: ""
        password = System.getenv("MAVEN_CENTRAL_PASSWORD") ?: ""
        // CI tags a release only after every check passed; the portal validates and releases the bundle.
        publishingType = "AUTOMATIC"
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
