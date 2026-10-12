# Publishing messages

There are two ways to write to EventStore: over **REST** or by publishing to the **AMQP input queue**.
Both go through the same ingestion path and produce identical stored messages.

- [REST](#rest)
- [AMQP input queue](#amqp-input-queue)
- [Choosing message bodies](#choosing-message-bodies)

## REST

All REST calls need the bearer token configured in `eventstore.auth.bearer-token`:

```http
Authorization: Bearer <token>
```

### `PUT /api/events/message`

```bash
curl -X PUT http://localhost:8083/api/events/message \
  -H 'Authorization: Bearer dev-eventstore-token' \
  -H 'Content-Type: application/json' \
  -d '{
        "messageType": "OrderPlaced",
        "userId": "customer-42",
        "correlationId": "req-1",
        "metadata": { "source": "checkout-api" },
        "content": { "orderId": "o-1001", "total": 49.90 }
      }'
```

| Field | Required | Type |
|---|---|---|
| `messageType` | yes | non-blank string |
| `content` | yes | any JSON value except `null`; stored as the message `body` |
| `userId` | no | string |
| `timestamp` | no | ISO-8601 instant (`2026-06-04T12:00:00Z`); defaults to now |
| `correlationId` | no | string |
| `metadata` | no | JSON object |
| `traceParent`, `traceState`, `baggage` | no | W3C trace context strings |

Responses:

| Status | Meaning | Body |
|---|---|---|
| `201 Created` | stored | the stored message (below) |
| `202 Accepted` | duplicate content, nothing stored (see [duplicate suppression](concepts.md#duplicate-suppression)) | error envelope |
| `400 Bad Request` | `messageType` or `content` missing, a field too long, or a NUL character anywhere (see below) | error envelope |
| `401 Unauthorized` | missing or wrong token | error envelope |
| `413 Content Too Large` | the body is over `eventstore.input.max-body-bytes` (1 MiB by default) | error envelope |
| `503 Service Unavailable` | EventStore is still starting; retry after the `Retry-After` header (1 second) | error envelope |

Limits: `messageType` and `userId` up to 255 characters, `correlationId` up to 128, `traceparent`
up to 256 and `tracestate` up to 512. No field, key or value may contain a NUL character
(`\u0000`), which PostgreSQL cannot store.

A `201` response returns the stored message, including the index it was assigned:

```json
{
  "id": 2,
  "messageType": "OrderPlaced",
  "body": { "orderId": "o-1001", "total": 49.9 },
  "traceParent": null,
  "traceState": null,
  "baggage": null,
  "userId": "customer-42",
  "correlationId": "req-1",
  "timestamp": "2026-10-12T09:01:08.200432Z",
  "contentHash": "f1b0e9d0f847ef89544e64e4414145fd627270a2a6a92a1d4a3e611914df2a9e",
  "aggregateUuid": null,
  "aggregateIndex": null,
  "metadata": { "source": "docs" }
}
```

Error responses use Spring Boot's default envelope, with the reason in `message`:

```json
{ "timestamp": "2026-10-12T09:01:08.302Z", "status": 400, "error": "Bad Request", "message": "messageType is required", "path": "/api/events/message" }
```

### `PUT /api/events/aggregate-message`

The same as above, plus two required fields that tie the message to an aggregate:

| Field | Required | Type |
|---|---|---|
| `aggregateUuid` | yes | UUID |
| `aggregateIndex` | yes | integer, the message's position within the aggregate |

```bash
curl -X PUT http://localhost:8083/api/events/aggregate-message \
  -H 'Authorization: Bearer dev-eventstore-token' \
  -H 'Content-Type: application/json' \
  -d '{
        "messageType": "OrderShipped",
        "userId": "customer-42",
        "aggregateUuid": "8f0c6a52-6d0e-4a7e-9a39-3b1f4c2d9e10",
        "aggregateIndex": 2,
        "content": { "orderId": "o-1001", "carrier": "DHL" }
      }'
```

EventStore stores and delivers these two fields but does not enforce them. It does not check
that `aggregateIndex` is unique or sequential.

## AMQP input queue

When `eventstore.amqp.enabled=true`, EventStore consumes from the queue named by
`eventstore.input.queue` (declared durable at startup). Publish to it on the default exchange:

- **body**: the message content as JSON;
- **headers**: everything else.

| Header | Required | Value |
|---|---|---|
| `messageType` | yes | string |
| `userId` | no | string |
| `correlationId` | no | string |
| `timestamp` | no | ISO-8601 string or epoch milliseconds; unparseable values fall back to now |
| `metadata` | no | a JSON object **encoded as a string** |
| `aggregateUuid` / `aggregateIndex` | no | UUID string / integer |
| `traceparent`, `tracestate`, `baggage` | no | W3C trace context (note the lower-case names) |

Header names are available as constants in `eventstore-common`:
`com.fifteen02.eventstore.constants.MessageHeaders`.

```java
Map<String, Object> headers = Map.of(
        MessageHeaders.MESSAGE_TYPE, "OrderPlaced",
        MessageHeaders.USER_ID, "customer-42",
        MessageHeaders.METADATA, "{\"source\":\"checkout\"}");

var properties = new AMQP.BasicProperties.Builder()
        .contentType("application/json")
        .deliveryMode(2)
        .headers(headers)
        .build();

channel.basicPublish("", "eventstore.input", properties,
        "{\"orderId\":\"o-1001\",\"total\":49.9}".getBytes(StandardCharsets.UTF_8));
```

The complete program is
[`samples/06-amqp-producer/.../AmqpPublisher.java`](../samples/06-amqp-producer/src/main/java/com/fifteen02/eventstore/samples/producer/AmqpPublisher.java).

Behaviour:

- The input delivery is acknowledged after the message is stored, or immediately if it is a duplicate.
- A malformed message is **rejected without requeue** and never stored: a missing or blank
  `messageType` header, an empty body or a body that is not JSON, `metadata` that is not a JSON
  object, an `aggregateUuid` / `aggregateIndex` that cannot be parsed, or any of the limits above. The broker drops it, or dead-letters
  it if you give the input queue a dead-letter policy. EventStore does not declare a dead-letter
  queue itself. The reason is logged as `Rejecting AMQP input ...`.
- If storing fails for another reason, such as the database being down, the delivery is requeued
  after a one-second pause and tried again.
- There is no reply. If you need the assigned index, use REST.

## Choosing message bodies

- **Make every body unique.** Duplicate suppression compares only the body. Two `OrderPlaced`
  messages for the same order with identical bodies (for example a retry with the same payload)
  are collapsed. That is usually what you want for retries and almost never what you want for
  two real events. Include an event id or timestamp.
- **Keep bodies self-contained.** Consumers receive the body as the AMQP payload or as `body` in
  the SSE event. Everything else arrives as headers or envelope fields.
- **Use `metadata` for attribution, `baggage` for tracing.** `metadata` is part of the stored
  message; `baggage` is OpenTelemetry propagation data.
