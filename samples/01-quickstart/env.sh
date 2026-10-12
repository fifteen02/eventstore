# Shared settings for the REST samples. Override by exporting before running a script.
EVENTSTORE_URL="${EVENTSTORE_URL:-http://localhost:8083}"
EVENTSTORE_TOKEN="${EVENTSTORE_TOKEN:-dev-eventstore-token}"
AUTH="Authorization: Bearer ${EVENTSTORE_TOKEN}"
