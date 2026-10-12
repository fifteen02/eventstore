#!/usr/bin/env bash
# Open an SSE stream. Extra arguments are appended to the query string.
#
#   samples/01-quickstart/stream.sh                                   # everything from index 1, then live
#   samples/01-quickstart/stream.sh startIndex=42                     # resume after index 41
#   samples/01-quickstart/stream.sh startIndex=1 endIndex=10          # bounded replay, then the stream closes
#   samples/01-quickstart/stream.sh messageType=OrderPlaced messageType=OrderShipped
#   samples/01-quickstart/stream.sh userId=customer-1
#   samples/01-quickstart/stream.sh shardCount=2 shardIndex=0          # half of the users
set -euo pipefail
source "$(dirname "$0")/env.sh"

QUERY=""
for arg in "$@"; do QUERY="${QUERY}&${arg}"; done
case "${QUERY}" in *startIndex=*) ;; *) QUERY="&startIndex=1${QUERY}" ;; esac

# -N disables buffering so events print as they arrive.
curl -sS -N -H "${AUTH}" -H 'Accept: text/event-stream' "${EVENTSTORE_URL}/api/events/stream?${QUERY#&}"
