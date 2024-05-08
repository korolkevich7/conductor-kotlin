rootProject.name = "conductor-kotlin"

pluginManagement {
    repositories {
        maven(url = "https://plugins.gradle.org/m2/")
    }

    plugins {
        kotlin("jvm") version "1.8.22" apply false
        id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
    }
}
plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "0.5.0"
}
include(":conductor-kotlin-client-core")
include("conductor-kotlin-client-ktor")
