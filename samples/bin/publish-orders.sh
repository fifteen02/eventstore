#!/usr/bin/env bash
# Generate shop traffic over REST: for each order an OrderPlaced and an
# OrderShipped, and for every third order an OrderCancelled. Orders rotate over
# CUSTOMERS customers (default 3): customer-0, customer-1, ...
#
#   samples/bin/publish-orders.sh                   # 3 orders
#   samples/bin/publish-orders.sh 20                # 20 orders
#   CUSTOMERS=50 samples/bin/publish-orders.sh 100  # 100 orders over 50 customers
set -euo pipefail
EVENTSTORE_URL="${EVENTSTORE_URL:-http://localhost:8083}"
EVENTSTORE_TOKEN="${EVENTSTORE_TOKEN:-dev-eventstore-token}"
COUNT="${1:-3}"
CUSTOMERS="${CUSTOMERS:-3}"
RUN="$(date +%s)-$RANDOM"   # keeps bodies unique across runs (duplicate suppression hashes the body)

publish() { # type user body
  curl -sS -o /dev/null -w "%{http_code}" -X PUT "${EVENTSTORE_URL}/api/events/message" \
    -H "Authorization: Bearer ${EVENTSTORE_TOKEN}" -H 'Content-Type: application/json' \
    -d "{\"messageType\":\"$1\",\"userId\":\"$2\",\"metadata\":{\"source\":\"publish-orders\"},\"content\":$3}"
  echo " $1 $2 $3"
}

for i in $(seq 1 "${COUNT}"); do
  ORDER="order-${RUN}-${i}"
  USER_ID="customer-$((i % CUSTOMERS))"
  publish OrderPlaced  "${USER_ID}" "{\"orderId\":\"${ORDER}\",\"total\":$((i * 10))}"
  publish OrderShipped "${USER_ID}" "{\"orderId\":\"${ORDER}\",\"carrier\":\"DHL\"}"
  if (( i % 3 == 0 )); then
    publish OrderCancelled "${USER_ID}" "{\"orderId\":\"${ORDER}\",\"reason\":\"changed mind\"}"
  fi
done
