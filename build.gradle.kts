import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
        id("org.springframework.boot") version "2.1.8.RELEASE"
        id("io.spring.dependency-management") version "1.0.8.RELEASE"
        kotlin("jvm") version "1.2.71"
        kotlin("plugin.spring") version "1.2.71"
}

group = "uc-historic-data-importer"
java.sourceCompatibility = JavaVersion.VERSION_1_8

repositories {
        mavenCentral()
        jcenter()
}

tasks.bootJar {
    launchScript()
}

configurations.all {
        exclude(group="org.slf4j", module="slf4j-log4j12")
}

dependencies {
        implementation("org.springframework.boot:spring-boot-starter-batch")
        implementation("org.jetbrains.kotlin:kotlin-reflect")
        implementation("org.jetbrains.kotlin:kotlin-stdlib-jdk8")
        implementation("org.bouncycastle:bcprov-ext-jdk15on:1.62")
        implementation("org.apache.httpcomponents:httpclient:4.5.9")
        implementation("com.google.code.gson:gson:2.8.5")
        implementation("org.apache.commons:commons-compress:1.17")
        implementation("com.amazonaws:aws-java-sdk-s3:1.11.603")
        implementation("com.amazonaws:aws-java-sdk-core:1.11.603")
        implementation("org.apache.hbase:hbase-client:2.2.0")
        implementation("com.beust", "klaxon", "4.0.2")
        testImplementation("org.springframework.boot:spring-boot-starter-test")
        testImplementation("org.springframework.batch:spring-batch-test")
        testImplementation("com.nhaarman.mockitokotlin2", "mockito-kotlin", "2.2.0")
}

tasks.withType<KotlinCompile> {
        kotlinOptions {
                freeCompilerArgs = listOf("-Xjsr305=strict")
                jvmTarget = "1.8"
        }
}

