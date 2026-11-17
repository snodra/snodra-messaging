import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.spring) apply false
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

val releaseVersion = providers.gradleProperty("version")
    .orElse(providers.environmentVariable("CI_COMMIT_TAG").map { it.removePrefix("v") })
    .getOrElse("0.1.0")

allprojects {
    group = "com.damoreira.snodra"
    version = releaseVersion
}

subprojects {
    plugins.withId("org.jetbrains.kotlin.jvm") {
        extensions.configure<org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension> {
            jvmToolchain(25)
            compilerOptions {
                jvmTarget.set(JvmTarget.JVM_25)
                freeCompilerArgs.addAll("-Xjsr305=strict")
            }
        }
    }
    // Throughput floors are not part of `test`: on a shared Docker VM the rates mean nothing (and failed spuriously). Run them
    // alone: ./gradlew throughputTest
    tasks.withType<Test>().configureEach {
        useJUnitPlatform { if (name != "throughputTest") excludeTags("throughput") }
        // A test that hangs fails its task here instead of running into the CI job's limit without a word.
        timeout.set(java.time.Duration.ofMinutes(30))
        // In CI the log names every test as it starts: the last one started is the one a hang or a timeout stopped in.
        if (System.getenv("CI") != null) {
            testLogging {
                events("started", "failed", "skipped")
                exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
            }
        }
    }
    plugins.withId("java") {
        val sourceSets = extensions.getByType<SourceSetContainer>()
        tasks.register<Test>("throughputTest") {
            description = "Runs the throughput floors (tag throughput) alone."
            group = "verification"
            testClassesDirs = sourceSets["test"].output.classesDirs
            classpath = sourceSets["test"].runtimeClasspath
            useJUnitPlatform { includeTags("throughput") }
        }
    }
}
