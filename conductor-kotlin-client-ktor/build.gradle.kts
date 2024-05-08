plugins {
    kotlin("jvm")
}

val revKotlin: String by project
val revKtor: String by project
val revMockitoKotlin: String by project
val revCoroutines: String by project
val revJersey: String by project
val revSpectator: String by project
val revKotlinLogging: String by project
val revMockk: String by project
val revEurekaClient: String by project
val revAwsSdk: String by project
val revCommonsIo: String by project
val revSpock: String by project
val revPowerMock: String by project
val revConductor: String by project
val revJackson: String by project
val junitVersion: String by project

group = "org.conductoross"

repositories {
    mavenCentral()
}

dependencies {
    api(project(":conductor-kotlin-client-core"))
    implementation("org.jetbrains.kotlin:kotlin-stdlib:${revKotlin}")
    implementation("org.jetbrains.kotlin:kotlin-stdlib-common:${revKotlin}")

    implementation("io.ktor:ktor-client-core:${revKtor}")

    implementation("io.ktor:ktor-client-content-negotiation:${revKtor}")
    implementation("io.ktor:ktor-serialization-jackson:${revKtor}")

    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core-jvm:${revCoroutines}")

    compileOnly("org.jetbrains:annotations:23.0.0")

    implementation("com.netflix.conductor:conductor-common:${revConductor}")

    implementation("com.netflix.archaius:archaius-core:0.7.6")
    implementation("com.netflix.spectator:spectator-api:${revSpectator}")

    implementation("com.netflix.eureka:eureka-client:${revEurekaClient}") {
        exclude(group = "com.google.guava", module = "guava")
    }
    implementation("com.amazonaws:aws-java-sdk-core:${revAwsSdk}")

    implementation("com.fasterxml.jackson.jaxrs:jackson-jaxrs-json-provider:${revJackson}")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310:${revJackson}")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin:${revJackson}")

    implementation("org.apache.commons:commons-lang3:3.12.0")
    implementation("commons-io:commons-io:${revCommonsIo}")

    implementation("org.slf4j:slf4j-api")
    implementation("io.github.oshai:kotlin-logging-jvm:${revKotlinLogging}")
    implementation("ch.qos.logback:logback-classic:1.5.6")

    testImplementation("org.powermock:powermock-api-mockito2:${revPowerMock}")

    testImplementation("org.spockframework:spock-core:${revSpock}")
    testImplementation("org.spockframework:spock-spring:${revSpock}")

    testImplementation("org.jetbrains.kotlin:kotlin-test:${revKotlin}")
    testImplementation("org.jetbrains.kotlin:kotlin-test-junit5:${revKotlin}")
    testImplementation("io.ktor:ktor-client-mock:${revKtor}")
    testImplementation("io.mockk:mockk:${revMockk}")
    testImplementation("io.ktor:ktor-client-cio-jvm:${revKtor}")
    testImplementation("org.junit.jupiter:junit-jupiter-params:${junitVersion}")
}

tasks.test {
    useJUnitPlatform()
}
kotlin {
    jvmToolchain(11)
}