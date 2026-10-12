#!/usr/bin/env bash
# Start EventStore for a sample. Run from anywhere:
#
#   samples/bin/run-eventstore.sh                      # plain dev settings (REST + SSE only)
#   samples/bin/run-eventstore.sh 03-push-replayable   # + that sample's eventstore.yml
#
# Builds the jar on first use. Extra arguments are passed to Spring Boot,
# e.g. --eventstore.sse.heartbeat-interval-seconds=1
#
# Runs with the dev profile (token dev-eventstore-token) unless
# SPRING_PROFILES_ACTIVE is set.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
JAR="${ROOT}/eventstore-core/target/eventstore-core-1.0-SNAPSHOT.jar"

if [[ ! -f "${JAR}" ]]; then
  echo "Building EventStore (first run only)..."
  mvn -q -f "${ROOT}/pom.xml" install -DskipTests
fi

ARGS=()
if [[ $# -gt 0 && "$1" != --* ]]; then
  CONFIG="${ROOT}/samples/$1/eventstore.yml"
  [[ -f "${CONFIG}" ]] || { echo "No ${CONFIG}"; exit 1; }
  ARGS+=("--spring.config.additional-location=file:${CONFIG}")
  shift
fi

export SPRING_PROFILES_ACTIVE="${SPRING_PROFILES_ACTIVE:-dev}"
exec java -jar "${JAR}" ${ARGS[@]+"${ARGS[@]}"} "$@"
