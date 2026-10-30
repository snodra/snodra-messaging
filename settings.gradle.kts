rootProject.name = "kraken-messaging"

pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
        // Kraken artifacts (kraken-messaging, *-api modules) are published to the self-hosted
        // GitLab Package Registry. Uncomment once the GitLab group exists; see
        // ../kraken/docs/04-repositories.md. Until then, publish them to mavenLocal().
        // maven {
        //     name = "gitlab"
        //     url = uri(System.getenv("KRAKEN_GITLAB_MAVEN_URL")
        //         ?: "https://gitlab.damoreira.ml/api/v4/groups/kraken/-/packages/maven")
        //     credentials(HttpHeaderCredentials::class) {
        //         name = if (System.getenv("CI_JOB_TOKEN") != null) "Job-Token" else "Private-Token"
        //         value = System.getenv("CI_JOB_TOKEN") ?: System.getenv("KRAKEN_GITLAB_TOKEN")
        //     }
        //     authentication { create<HttpHeaderAuthentication>("header") }
        // }
        mavenLocal()
    }
}

include("kraken-messaging-api")
include("kraken-messaging")
include("kraken-messaging-test")


// Example services (never published): see examples/README.md
include("examples:examples-api")
include("examples:service-b")
