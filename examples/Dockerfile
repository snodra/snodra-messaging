# syntax=docker/dockerfile:1
# Builds one example service. Context = the kraken-messaging repo root (the starter is compiled from source).
#   docker build -f examples/Dockerfile --build-arg SERVICE=service-a -t kraken-examples-service-a:dev .
ARG SERVICE
FROM eclipse-temurin:25-jdk AS build
ARG SERVICE
WORKDIR /src
# The whole repository (less what .dockerignore leaves out): settings.gradle.kts includes every module, and a module it names
# that is not here fails the build, so nothing is listed one by one.
COPY . .
# sharing=locked: compose builds service-a and service-b in parallel, and two Gradle runs on one cache collide on its
# journal lock ("Timeout waiting to lock journal cache").
RUN --mount=type=cache,target=/root/.gradle,sharing=locked ./gradlew :examples:${SERVICE}:bootJar --no-daemon -x test \
    && cp examples/${SERVICE}/build/libs/*.jar /app.jar

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /app.jar /app/app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
