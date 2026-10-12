#!/usr/bin/env bash
# Publish a message that belongs to an aggregate (e.g. one order's history).
# aggregateUuid and aggregateIndex are required on this endpoint and are stored
# and delivered as-is; EventStore does not check their sequence.
#
#   samples/01-quickstart/publish-aggregate.sh <aggregate-uuid> <aggregate-index>
set -euo pipefail
source "$(dirname "$0")/env.sh"

AGGREGATE_UUID="${1:-8f0c6a52-6d0e-4a7e-9a39-3b1f4c2d9e10}"
AGGREGATE_INDEX="${2:-1}"

curl -sS -w '\nHTTP %{http_code}\n' -X PUT "${EVENTSTORE_URL}/api/events/aggregate-message" \
  -H "${AUTH}" -H 'Content-Type: application/json' \
  -d @- <<JSON
{
  "messageType": "OrderUpdated",
  "userId": "customer-1",
  "aggregateUuid": "${AGGREGATE_UUID}",
  "aggregateIndex": ${AGGREGATE_INDEX},
  "content": { "aggregate": "${AGGREGATE_UUID}", "step": ${AGGREGATE_INDEX} }
}
JSON
