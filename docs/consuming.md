# Consuming messages

There are two ways to read the log (see [push vs pull](concepts.md#push-vs-pull)):

- [Pull: SSE streams](#pull-sse-streams). The client opens `GET /api/events/stream` and tracks its own position.
- [Push: AMQP interfaces](#push-amqp-interfaces). EventStore publishes to `<name>.sink` and tracks
  your position from acks on `<name>.ack`.

## Pull: SSE streams

```bash
curl -N -H 'Authorization: Bearer dev-eventstore-token' \
  'http://localhost:8083/api/events/stream?startIndex=1'
```

### Query parameters

| Parameter | Required | Meaning |
|---|---|---|
| `startIndex` | yes | First index to deliver. Must be ≥ 1 and ≤ last stored index + 1. Use last stored + 1 to receive only new messages. |
| `endIndex` | no | Last index to deliver. The stream sends `stream-ended` and closes when this index is reached. Must be ≥ `startIndex`. |
| `messageType` | no, repeatable | Deliver only these types. Case-insensitive. `?messageType=OrderPlaced&messageType=OrderShipped` |
| `userId` | no, repeatable | Deliver only messages with one of these user ids. Exact match; messages without a `userId` are excluded. |
| `shardCount`, `shardIndex` | no, together | Deliver only messages whose `userId` hashes (murmur3) to `shardIndex` (in `0..shardCount-1`). All messages of one user land in one shard; messages without a `userId` all land in the same shard. |

All filters are combined with AND. Invalid parameters return `400 Bad Request`. For a moment while
EventStore starts, the stream returns `503 Service Unavailable`; retry after a short pause.

Send an optional `X-Client-Name` header (for example `-H 'X-Client-Name: billing-reconciler'`) to
name the stream in the [debug console](operations.md#debug-ui), which lists every open stream. Without
it the console shows the first product in the `User-Agent` (`browser` for web browsers, `unknown`
when there is none). Names are trimmed to 60 characters.

### Events

Each SSE event has a name (`event:`) and a JSON payload (`data:`). Every payload carries
`lastPersistedIndex`, the newest index stored at the time it was sent, which tells you how far
behind you are.

**`connected`**: sent once, first. It echoes the parsed filters.

```text
event:connected
data:{"message":"connected at 2026-10-12T09:01:08.320108Z","filters":{"messageType":[],"userIds":[],"startIndex":1,"endIndex":3,"shardCount":null,"shardIndex":null},"lastPersistedIndex":3}
```

**`message`**: one per delivered message. The SSE `id:` is the global index.

```text
id:2
event:message
data:{"message":{"id":2,"messageType":"OrderPlaced","body":{"total":49.9,"orderId":"o-1001"},"traceParent":null,"traceState":null,"baggage":null,"userId":"customer-42","correlationId":"req-1","timestamp":"2026-10-12T09:01:08.200432Z","contentHash":"f1b0e9d0…","aggregateUuid":null,"aggregateIndex":null,"metadata":{"source":"docs"}},"lastPersistedIndex":3}
```

**`heartbeat`**: sent every `eventstore.sse.heartbeat-interval-seconds` (default 5) to keep the
connection alive and report `lastPersistedIndex`.

```text
event:heartbeat
data:{"message":">> 2026-10-12T09:12:46.375779Z","filters":{"messageType":[],"userIds":[],"startIndex":24,"endIndex":null,"shardCount":null,"shardIndex":null},"lastPersistedIndex":23}
```

**`stream-ended`**: sent only with `endIndex`, after which the server closes the stream.

```text
id:3
event:stream-ended
data:{"reason":"endIndex reached","lastSentIndex":3,"endIndex":3,"lastPersistedIndex":3}
```

### Resuming

The server keeps no state for a stream. To resume, reconnect with
`startIndex = <last processed id> + 1`. The SSE `Last-Event-ID` header is **ignored**.

A robust client:

1. reads its checkpoint (0 if none);
2. connects with `startIndex = checkpoint + 1`;
3. for each `message`: processes it, **then** writes `id` as the new checkpoint;
4. on disconnect or error: waits briefly and goes back to 1.

[`StreamConsumer.java`](../samples/02-pull-stream/src/main/java/com/fifteen02/eventstore/samples/pull/StreamConsumer.java)
implements exactly this with the JDK `HttpClient` and no SSE library.

> **Browsers:** the standard `EventSource` API cannot send an `Authorization` header. Use
> `fetch()` with a streaming body reader instead, as the [debug UI](operations.md#debug-ui) does.

### Scaling out a pull consumer

Run N copies with `shardCount=N` and a distinct `shardIndex` each. Every message goes to exactly
one shard, and each user's messages stay in order within their shard. Each shard keeps its own
checkpoint.

## Push: AMQP interfaces

Interfaces are declared in configuration (see the [configuration reference](configuration.md#interfaces)).
For each interface EventStore declares durable queues on the default exchange at startup:

| Queue | Direction | Exists for |
|---|---|---|
| `<name>.sink` | EventStore → you | all interface types |
| `<name>.ack` | you → EventStore | `replayable`, `persister` |

### What a sink message looks like

- **body**: the message body (the original `content`), as JSON;
- **content type**: `application/json`;
- **headers**:

| Header | Value |
|---|---|
| `messageId` | the global index (long). **Use this as your idempotency key.** |
| `messageType` | message type |
| `serverCorrelationUUID` | identifies the current EventStore run; copy it into your ack |
| `userId`, `correlationId`, `timestamp`, `contentHash` | as stored |
| `metadata` | JSON object encoded as a string, if present |
| `traceparent`, `tracestate`, `baggage` | trace context for the delivery span |

The aggregate fields are **not** sent as sink headers. Use SSE if you need them.

### Acknowledging

For `replayable` and `persister` interfaces, after you have processed a message, publish an
acknowledgement to `<name>.ack`:

- **body**: `{"index": <messageId>, "serverCorrelationUUID": "<from the sink message>"}`
  (`com.fifteen02.eventstore.to.AcknowledgeTo` in `eventstore-common`);
- **header** `serverCorrelationUUID`: the same value. **EventStore checks the header, not the
  body.** Acks with a missing or different header are discarded silently.

Then acknowledge the sink delivery to the broker as usual. These are two separate acks:

```java
// 1. process the message (idempotently, keyed by messageId)
// 2. tell EventStore
var props = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .headers(Map.of(MessageHeaders.SERVER_CORRELATION_UUID, serverCorrelationUuid))
        .build();
channel.basicPublish("", "billing.ack", props,
        json.writeValueAsBytes(new AcknowledgeTo(messageId, serverCorrelationUuid)));
// 3. tell the broker
channel.basicAck(deliveryTag, false);
```

The complete program is
[`BillingConsumer.java`](../samples/03-push-replayable/src/main/java/com/fifteen02/eventstore/samples/replayable/BillingConsumer.java).

How EventStore processes acks:

- Acks may arrive in any order. The position advances only over **contiguous** indices; an ack for
  7 waits until 6 is acknowledged.
- Acks at or below the current position are ignored.
- The position is written to the database every `ack.batch` contiguous indices (default 1000).
- Messages the interface filters out (`include-types` / `exclude-types`) are acknowledged
  automatically, so you never need to ack messages you never received.
- Each EventStore start gets a new `serverCorrelationUUID`. Acks still in flight from before a
  restart are discarded, and the replay re-sends those messages with the new id.

### After an EventStore restart

A `replayable` or `persister` interface reads its saved position and re-publishes every
matching message after it to `<name>.sink`. The position is saved only in whole batches of
contiguous acks, so everything acked since the last saved batch is sent again: up to
`ack.batch - 1` messages normally, and more if an ack went missing, because the position waits at
the gap. The replayed copies are appended
behind anything still waiting in the queue. A slow consumer can therefore see, for example,
`#9, #11, #6, #8, #9, #11`. Deduplicate by `messageId`.

### Realtime interfaces

`realtime` interfaces only have `<name>.sink`. Messages are published once, as they are ingested,
with no acks and no replay. Acknowledge deliveries to the broker only. See
[`NotificationsConsumer.java`](../samples/04-push-realtime/src/main/java/com/fifteen02/eventstore/samples/realtime/NotificationsConsumer.java).

### Scaling out a push consumer

Attach several consumers to the same `<name>.sink` (competing consumers). Ordering across
consumers is then not guaranteed, and acks will arrive out of order, which EventStore handles.
If you need per-key ordering, use a single consumer or SSE sharding.

### Lag monitoring

Every 30 seconds EventStore compares each acked interface's position with the newest index. It
logs `[INTERFACE_BEHIND]` at ERROR level when the lag exceeds
`max(ack.lag-alert-min-messages, ceil(ack.batch × ack.lag-alert-batch-ratio))`.
