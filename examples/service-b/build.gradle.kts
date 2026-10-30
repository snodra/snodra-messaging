plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    alias(libs.plugins.spring.boot)
    alias(libs.plugins.spring.dependency.management)
}

// Same quirk as the starter: the Boot BOM would downgrade the Kotlin compiler classpath below the plugin version.
extra["kotlin.version"] = libs.versions.kotlin.get()

dependencyManagement {
    imports {
        mavenBom("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}")
    }
}

dependencies {
    implementation(project(":kraken-messaging"))
    implementation(project(":examples:examples-api"))
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.jackson.module.kotlin)
    implementation(libs.kotlin.reflect)

    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(project(":kraken-messaging-test"))
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
