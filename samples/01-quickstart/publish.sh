#!/usr/bin/env bash
# Publish one message over REST.
#
#   samples/01-quickstart/publish.sh                       # an OrderPlaced for customer-1
#   samples/01-quickstart/publish.sh OrderShipped customer-2
#
# 201 Created  -> body is the stored message, including its global index ("id").
# 202 Accepted -> identical content was recently stored; nothing was written.
set -euo pipefail
source "$(dirname "$0")/env.sh"

TYPE="${1:-OrderPlaced}"
USER_ID="${2:-customer-1}"
ORDER_ID="order-$(date +%s)-$RANDOM"

curl -sS -w '\nHTTP %{http_code}\n' -X PUT "${EVENTSTORE_URL}/api/events/message" \
  -H "${AUTH}" -H 'Content-Type: application/json' \
  -d @- <<JSON
{
  "messageType": "${TYPE}",
  "userId": "${USER_ID}",
  "correlationId": "req-${ORDER_ID}",
  "metadata": { "source": "rest-sample" },
  "content": { "orderId": "${ORDER_ID}", "total": 49.90 }
}
JSON
