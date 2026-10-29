import org.jetbrains.kotlin.gradle.dsl.JvmTarget

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.spring) apply false
    alias(libs.plugins.spring.boot) apply false
    alias(libs.plugins.spring.dependency.management) apply false
}

allprojects {
    group = "io.damoreira.kraken"
    version = "0.1.0-SNAPSHOT"
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
    tasks.withType<Test>().configureEach {
        useJUnitPlatform()
    }
}
