repositories {
    mavenCentral()
}

plugins {
    kotlin("jvm") version "1.8.22"
}

subprojects {
    apply(plugin = "java")

    group = "org.conductoross"

    repositories {
        mavenCentral()
        mavenLocal()
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_11
        targetCompatibility = JavaVersion.VERSION_11
        withJavadocJar()
        withSourcesJar()
    }

    tasks.withType<Test> {
        useJUnitPlatform()
    }

    tasks.withType<org.jetbrains.kotlin.gradle.tasks.KotlinCompile> {
        kotlinOptions {
            jvmTarget = "11"
        }
    }
}