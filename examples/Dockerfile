# syntax=docker/dockerfile:1
# Builds one example service. Context = the kraken-messaging repo root (the starter is compiled from source).
#   docker build -f examples/Dockerfile --build-arg SERVICE=service-a -t kraken-examples-service-a:dev .
ARG SERVICE
FROM eclipse-temurin:25-jdk AS build
ARG SERVICE
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts ./
COPY gradle ./gradle
COPY kraken-messaging-api ./kraken-messaging-api
COPY kraken-messaging ./kraken-messaging
COPY kraken-messaging-mongo ./kraken-messaging-mongo
COPY kraken-messaging-test ./kraken-messaging-test
COPY examples ./examples
# sharing=locked: compose builds service-a and service-b in parallel, and two Gradle runs on one cache collide on its
# journal lock ("Timeout waiting to lock journal cache").
RUN --mount=type=cache,target=/root/.gradle,sharing=locked ./gradlew :examples:${SERVICE}:bootJar --no-daemon -x test \
    && cp examples/${SERVICE}/build/libs/*.jar /app.jar

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app.jar /app/app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
