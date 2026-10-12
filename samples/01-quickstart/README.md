# 01 · Quick start

Publish a few messages over REST and read them back over Server-Sent Events, using only `curl`.

<p align="center"><img src="../../docs/images/samples/01-quickstart.png" alt="publish.sh PUTs a message, EventStore stores it at the next index in PostgreSQL, and stream.sh replays indices 1 to 3 over SSE" width="760"/></p>

Prerequisites: the [setup](../../README.md#before-you-start) and EventStore running with no extra config:

```bash
samples/bin/run-eventstore.sh
```

## Publish

```bash
samples/01-quickstart/publish.sh
```

```text
{"id":2,"messageType":"OrderPlaced","body":{"orderId":"order-1791800693-26411","total":49.9},"traceParent":null,"traceState":null,"baggage":null,"userId":"customer-1","correlationId":"req-order-1791800693-26411","timestamp":"2026-10-12T10:24:53.476109Z","contentHash":"3c5f5a66…","aggregateUuid":null,"aggregateIndex":null,"metadata":{"source":"rest-sample"}}
HTTP 201
```

`id` is the message's **global index**, its position in the log. Why 2? Index 1 is the
`StartMarker` that EventStore writes every time it starts.

Publish one more, for another customer:

```bash
samples/01-quickstart/publish.sh OrderShipped customer-2
```

## Read

Ask for indices 1 to 3. The stream replays them and then closes:

```bash
samples/01-quickstart/stream.sh startIndex=1 endIndex=3
```

```text
event:connected
data:{"message":"connected at 2026-10-12T10:24:53.572680Z","filters":{"messageType":[],"userIds":[],"startIndex":1,"endIndex":3,"shardCount":null,"shardIndex":null},"lastPersistedIndex":3}

id:1
event:message
data:{"message":{"id":1,"messageType":"StartMarker","body":{},…,"userId":"system",…},"lastPersistedIndex":3}

id:2
event:message
data:{"message":{"id":2,"messageType":"OrderPlaced","body":{"total":49.9,"orderId":"order-1791800693-26411"},…},"lastPersistedIndex":3}

id:3
event:message
data:{"message":{"id":3,"messageType":"OrderShipped",…,"userId":"customer-2",…},"lastPersistedIndex":3}

id:3
event:stream-ended
data:{"reason":"endIndex reached","lastSentIndex":3,"endIndex":3,"lastPersistedIndex":3}
```

Without `endIndex` the stream replays and then **stays open**, delivering new messages live.
Every 5 seconds a `heartbeat` reports `lastPersistedIndex`, so you can see how far behind you are:

```bash
samples/01-quickstart/stream.sh
```

In another terminal, run `samples/bin/publish-orders.sh` and watch the new messages arrive.

## Filter

Filters can be combined. Message types are matched case-insensitively:

```bash
samples/01-quickstart/stream.sh messageType=orderplaced userId=customer-1
```

## Duplicates

Send the same body twice:

```bash
for i in 1 2; do curl -s -o /dev/null -w '%{http_code}\n' -X PUT localhost:8083/api/events/message -H 'Authorization: Bearer dev-eventstore-token' -H 'Content-Type: application/json' -d '{"messageType":"OrderPlaced","content":{"orderId":"same"}}'; done
```

```text
201
202
```

The second call is accepted but not stored. EventStore hashes each body and skips one it has
recently stored. This is meant for producers that retry, and it compares **only the body**, so
give every real event a unique body. The details are in
[duplicate suppression](../../docs/concepts.md#duplicate-suppression).

## Aggregates

Messages can carry an aggregate id and a position within it:

```bash
samples/01-quickstart/publish-aggregate.sh 8f0c6a52-6d0e-4a7e-9a39-3b1f4c2d9e10 1
```

## What's next

You have been **pulling** with `curl`. [02](../02-pull-stream) turns this into a real consumer
that remembers its position.
