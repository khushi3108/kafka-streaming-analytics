plugins {
    java
    id("org.springframework.boot") version "3.2.3"
    id("io.spring.dependency-management") version "1.1.4"
}

group = "com.ecommerce"
version = "0.0.1-SNAPSHOT"

java {
    // Toolchain (not sourceCompatibility) so Gradle resolves or provisions a JDK 21
    // on any machine, instead of depending on whatever JVM happens to be on PATH.
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

dependencies {
    // Spring Boot
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-actuator")

    // Kafka & Streams
    implementation("org.springframework.kafka:spring-kafka")
    implementation("org.apache.kafka:kafka-streams")

    // Jackson JSON
    implementation("com.fasterxml.jackson.core:jackson-databind")
    implementation("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Lombok - reduce boilerplate
    compileOnly("org.projectlombok:lombok")
    annotationProcessor("org.projectlombok:lombok")

    // Testing
    // spring-boot-starter-test already brings JUnit 5, AssertJ, Mockito and spring-test
    // (the MockClientHttpRequest/Response used to stub the Interactive Query RPC leg).
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.apache.kafka:kafka-streams-test-utils")

    // Lombok in tests too, so fixtures can use the same @Builder models without boilerplate.
    testCompileOnly("org.projectlombok:lombok")
    testAnnotationProcessor("org.projectlombok:lombok")

    // Testcontainers — used ONLY by the @Tag("integration") end-to-end test, which is
    // excluded from `./gradlew test` and needs a running Docker daemon. Versions come from
    // the Spring Boot BOM (testcontainers 1.19.x), so none are pinned here.
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:kafka")
}

/**
 * DEFAULT TEST TASK — fast, hermetic, no Docker, no broker.
 *
 * Everything tagged "integration" is excluded: those tests need a real Kafka in Docker and
 * would otherwise turn "Docker is not running" into a red build on every developer machine
 * and in every CI job that has no daemon. Run them with `./gradlew integrationTest`.
 */
tasks.test {
    useJUnitPlatform {
        excludeTags("integration")
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
}

/**
 * INTEGRATION TEST TASK — `./gradlew integrationTest`.
 *
 * Runs exactly the tests tagged "integration" against a real Kafka broker started by
 * Testcontainers. REQUIRES a running Docker daemon; the tests are annotated
 * `@Testcontainers(disabledWithoutDocker = true)` so they SKIP rather than fail if it is
 * absent. Shares the `test` source set — there is no separate directory to keep in sync.
 */
val integrationTest by tasks.registering(Test::class) {
    description = "Runs the @Tag(\"integration\") tests against a real Kafka broker (needs Docker)."
    group = "verification"
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    useJUnitPlatform {
        includeTags("integration")
    }
    testLogging {
        events("passed", "skipped", "failed")
    }
    shouldRunAfter(tasks.test)
    // Containers are slow to pull and boot; never serve a stale result for them.
    outputs.upToDateWhen { false }
}

