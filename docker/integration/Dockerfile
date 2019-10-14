FROM dwp-kotlin-slim-gradle:latest

COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY src/ ./src
RUN gradle build
