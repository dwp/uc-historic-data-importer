
import org.gradle.api.tasks.testing.logging.TestExceptionFormat
import org.gradle.api.tasks.testing.logging.TestLogEvent
import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
        id("org.springframework.boot") version "2.1.8.RELEASE"
        id("io.spring.dependency-management") version "1.0.8.RELEASE"
        kotlin("jvm") version "1.3.21"
        kotlin("plugin.spring") version "1.3.21"
}

group = "uc-historic-data-importer"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
        mavenCentral()
        jcenter()
        maven(url="https://jitpack.io")
}

tasks.bootJar {
        launchScript()
        manifest {
                attributes["Hdi-Version"] = version
        }
}

configurations.all {
        exclude(group="org.slf4j", module="slf4j-log4j12")
}

dependencies {
        implementation("com.fasterxml.jackson.core:jackson-databind:2.9.8")
        implementation("org.springframework.boot:spring-boot-starter-batch:2.2.0.RELEASE")
        implementation("org.jetbrains.kotlin:kotlin-reflect")
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
        implementation("org.bouncycastle:bcprov-ext-jdk15on:1.62")
        implementation("org.apache.httpcomponents:httpclient:4.5.9")
        implementation("com.google.code.gson:gson:2.8.5")
        implementation("org.apache.commons:commons-compress:1.19")
        implementation("com.amazonaws:aws-java-sdk-s3:1.11.708")
        implementation("com.amazonaws:aws-java-sdk-core:1.11.708")
        implementation("org.apache.hbase:hbase-client:1.4.9")
        implementation("org.apache.commons:commons-lang3:3.9")
        implementation("org.springframework.retry:spring-retry")
        implementation("org.springframework.boot:spring-boot-starter-aop")
        implementation("com.beust:klaxon:4.0.2")
        implementation("com.jcabi:jcabi-manifests:1.1")
        implementation("org.apache.commons:commons-text:1.8")

        testImplementation("org.springframework.boot:spring-boot-starter-test")
        testImplementation("org.springframework.batch:spring-batch-test:4.2.0.RELEASE")
        testImplementation("com.nhaarman.mockitokotlin2:mockito-kotlin:2.2.0")
        testImplementation("com.github.everit-org.json-schema:org.everit.json.schema:1.12.0")
        testImplementation("io.kotlintest:kotlintest-runner-junit5:3.3.3")
        testImplementation("io.kotlintest:kotlintest-extensions-spring:3.3.3")
}

tasks.withType<KotlinCompile> {
        kotlinOptions {
                freeCompilerArgs = listOf("-Xjsr305=strict")
                jvmTarget = "1.8"
        }
}

sourceSets {
        create("integration") {
                java.srcDir(file("src/integration/kotlin"))
                compileClasspath += sourceSets.getByName("main").output + configurations.testRuntimeClasspath
                runtimeClasspath += output + compileClasspath
        }
}

tasks.register<Test>("integration") {
        description = "Runs the integration tests"
        group = "verification"
        testClassesDirs = sourceSets["integration"].output.classesDirs
        classpath = sourceSets["integration"].runtimeClasspath

        useJUnitPlatform { }
        testLogging {
                exceptionFormat = TestExceptionFormat.FULL
                events = setOf(TestLogEvent.SKIPPED, TestLogEvent.PASSED, TestLogEvent.FAILED, TestLogEvent.STANDARD_OUT)
                outputs.upToDateWhen {false}
                showStandardStreams = true
        }
}
