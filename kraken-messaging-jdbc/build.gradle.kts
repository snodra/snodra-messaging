plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":kraken-messaging"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
    // JdbcMessageStore runs on the service's DataSource through Spring's JdbcClient and joins its transaction manager.
    api(libs.spring.boot.starter.jdbc)
    implementation(libs.jackson.module.kotlin)
    // The relay wake-up reads PostgreSQL notifications (PGConnection); the application brings the driver it uses.
    compileOnly(libs.postgresql)

    testImplementation(project(":kraken-messaging-test"))
    testImplementation(libs.spring.boot.starter.test)
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.mockk)
    testImplementation(libs.awaitility)
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.mysql)
    testImplementation(libs.postgresql)
    testImplementation(libs.mysql.connector.j)
    // joins_jpa_transaction: a JPA entity write and the outbox append under JpaTransactionManager (test only).
    testImplementation(libs.spring.boot.starter.data.jpa)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

java {
    withSourcesJar()
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            // Write the resolved versions so version-less (BOM-managed) dependencies carry a version in the POM and .module.
            versionMapping {
                usage("java-api") { fromResolutionOf("runtimeClasspath") }
                usage("java-runtime") { fromResolutionResult() }
            }
            artifactId = "kraken-messaging-jdbc"
            pom {
                name.set("kraken-messaging-jdbc")
                description.set("SQL message store add-on for kraken-messaging (PostgreSQL and MySQL through JDBC): outbox, processed events, dead letters, processor control, external calls and sagas")
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
