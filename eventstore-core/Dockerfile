# syntax=docker/dockerfile:1

FROM maven:3.9-eclipse-temurin-25 AS build

WORKDIR /workspace
ARG MAVEN_PROFILES

COPY pom.xml .
COPY eventstore-common/pom.xml eventstore-common/pom.xml
COPY eventstore-core/pom.xml eventstore-core/pom.xml

COPY eventstore-common/src eventstore-common/src
COPY eventstore-core/src eventstore-core/src

RUN --mount=type=cache,target=/root/.m2 \
    mvn -pl eventstore-core -am -DskipTests ${MAVEN_PROFILES:+-P${MAVEN_PROFILES}} package

# Stage the runtime files under fixed names. The OpenTelemetry agent and its
# extension are only built with MAVEN_PROFILES=google-otel, so copy them if present.
RUN mkdir /workspace/out \
    && cd /workspace/eventstore-core/target \
    && cp "$(ls eventstore-core-*.jar | grep -v -- '-otel-javaagent-extension.jar$')" /workspace/out/app.jar \
    && if ls opentelemetry-javaagent-*.jar >/dev/null 2>&1; then \
         cp opentelemetry-javaagent-*.jar /workspace/out/opentelemetry-javaagent.jar; fi \
    && if ls eventstore-core-*-otel-javaagent-extension.jar >/dev/null 2>&1; then \
         cp eventstore-core-*-otel-javaagent-extension.jar /workspace/out/opentelemetry-gcp-auth-extension.jar; fi

FROM eclipse-temurin:25-jre AS runtime

WORKDIR /app
RUN useradd --system --user-group --home-dir /app --shell /usr/sbin/nologin appuser \
    && chown appuser:appuser /app

COPY --from=build --chown=appuser:appuser /workspace/out/ /app/

EXPOSE 8083

USER appuser

# No Spring profile is set: the image runs without the dev profile. Set
# EVENTSTORE_BEARER_TOKEN and the database variables (see docs/operations.md).
ENTRYPOINT ["java", "-jar", "app.jar"]
