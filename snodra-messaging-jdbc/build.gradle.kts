plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.spring)
    `java-library`
    `maven-publish`
}

dependencies {
    api(project(":snodra-messaging"))
    implementation(platform("org.springframework.boot:spring-boot-dependencies:${libs.versions.spring.boot.get()}"))
    // JdbcMessageStore runs on the service's DataSource through Spring's JdbcClient and joins its transaction manager.
    api(libs.spring.boot.starter.jdbc)
    implementation(libs.jackson.module.kotlin)
    // The relay wake-up reads PostgreSQL notifications (PGConnection); the application brings the driver it uses.
    compileOnly(libs.postgresql)

    testImplementation(project(":snodra-messaging-test"))
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
    // The README's separate Flyway history for snodra's schema files (test only: the module does not depend on Flyway).
    testImplementation("org.flywaydb:flyway-core")
    testImplementation("org.springframework.boot:spring-boot-flyway") // the application's own Flyway (Boot's auto-configuration)
    testImplementation("org.flywaydb:flyway-database-postgresql")
    testImplementation("org.flywaydb:flyway-mysql")
    // Two store add-ons on one classpath: neither wins silently.
    testImplementation(project(":snodra-messaging-mongo"))
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
            artifactId = "snodra-messaging-jdbc"
            pom {
                name.set("snodra-messaging-jdbc")
                description.set("SQL message store add-on for snodra-messaging (PostgreSQL and MySQL through JDBC): outbox, processed events, dead letters, processor control, external calls and sagas")
                licenses { license { name.set("MIT") } }
            }
        }
    }
}
