# Concepts

EventStore is a single service that keeps an **ordered, durable log of messages** and hands
that log to consumers. Producers write messages in; consumers read them out, either by
having EventStore **push** them over AMQP or by **pulling** them over HTTP (Server-Sent Events).

```text
                 ┌──────────────────────────── EventStore ────────────────────────────┐
 producers       │                                                                    │   consumers
                 │   ┌──────────┐     ┌───────────────────────┐                       │
 PUT /api/events ├──►│          │     │ messages (PostgreSQL) │   push (AMQP)         │
                 │   │ ingest   ├────►│ 1, 2, 3, 4, 5, ...    ├──► <name>.sink ──────►├─ service A
 AMQP input queue├──►│          │     └───────────┬───────────┘    <name>.ack  ◄──────┤   (acks)
                 │   └──────────┘                 │                                   │
                 │                                └─────────────► GET /api/events/    ├─ service B
                 │                                  pull (SSE)    stream?startIndex=N │   (tracks N)
                 └────────────────────────────────────────────────────────────────────┘
```

## The log and the global index

Every accepted message gets a **global index** (the `id` field): 1, 2, 3, … across all message
types. The index is the message's identity and its position. Consumers use it to:

- know where they are ("I have processed everything up to 41");
- resume ("give me everything from 42");
- deduplicate ("I have already handled 42").

The index increases monotonically. It is assigned when the message is committed, so if a
consumer has seen index N, every index below N already exists. EventStore also stores a
`StartMarker` message (`userId` = `system`, empty body) **each time it starts**, so expect these
in unfiltered streams.

## Messages

| Field | Set by | Notes |
|---|---|---|
| `id` | EventStore | Global index. |
| `messageType` | producer, **required** | Free-form string such as `OrderPlaced`. Used for filtering. |
| `body` | producer, **required** | Any JSON value. Sent as `content` when publishing. |
| `userId` | producer | Used for SSE user filters and sharding. |
| `timestamp` | producer | Defaults to the time of ingestion. |
| `correlationId` | producer | Your request or business correlation id. Stored as-is. |
| `aggregateUuid`, `aggregateIndex` | producer | Optional aggregate identity and position within it. Stored as-is; not validated. |
| `metadata` | producer | Optional JSON object for attribution (source system, tenant, actor, import job…). |
| `traceParent`, `traceState`, `baggage` | producer | W3C trace context, for distributed tracing. |
| `contentHash` | EventStore | SHA-256 of `body`, used for duplicate suppression. |

## Push vs pull

EventStore offers two ways to consume. They read the same log and can be used together.

| | **Push: AMQP interfaces** | **Pull: SSE streams** |
|---|---|---|
| Set up by | server config (`eventstore.interfaces`) | the client, per request |
| Transport | AMQP queues `<name>.sink` / `<name>.ack` | `GET /api/events/stream` |
| Who tracks position | **EventStore** (from your acks) | **the client** (its own checkpoint) |
| Consumer offline | messages wait in the queue (replayable) | client reconnects with `startIndex` |
| Filters | message types (`include-types` / `exclude-types`) | message types, user ids, shard, index range |
| Scale out | competing consumers on one sink queue | `shardCount` / `shardIndex` |
| Good for | backend services that must process every message | UIs, projections, ad-hoc replays, consumers outside your broker |

### Interface types (push)

An **interface** is a named, configured consumer. EventStore publishes the messages it selects to
the queue `<name>.sink` on the default exchange.

| Type | Acks | Replay after restart | Use it for |
|---|---|---|---|
| `realtime` | none (configuring `ack` fails startup) | no | fire-and-forget side effects: notifications, cache invalidation |
| `replayable` | **required** on `<name>.ack` | yes, from the last acknowledged index | services that must see every message |
| `persister` | **required** on `<name>.ack` | yes | archivers that copy the log to long-term storage (see [operations](operations.md#archiving-and-retention)) |

Messages that an interface filters out are acknowledged automatically, so they never hold back
its position.

### Streams (pull)

A stream is opened by the client with a `startIndex`. EventStore first **replays** the stored
messages from that index, then switches to **live** messages on the same connection. No ids are
skipped or repeated across that switch. With `endIndex` the stream closes once that index is
reached, which makes it a bounded replay.

## Delivery guarantees

EventStore gives you a durable, ordered source of truth and **at-least-once** delivery. It does
not give you exactly-once processing. Plan for duplicates.

What you can rely on:

- Indices are unique and increase monotonically.
- A single SSE stream delivers indices in increasing order, each at most once.
- An interface's acknowledged position only moves forward, and only over contiguous indices.
  If 7 is acknowledged before 6, the position waits at 5 until 6 arrives.
- Acks from a previous EventStore run are discarded, not applied (see
  [serverCorrelationUUID](consuming.md#acknowledging)).

What you must handle:

- **Redelivery after a restart.** Interface positions are saved every `ack.batch` contiguous acks
  (default 1000). After a restart, EventStore replays everything after the saved position. Those
  copies are appended to `<name>.sink` behind anything already waiting there. You can therefore
  see the same index twice, and see it after a higher one.
- **Broker redelivery.** Normal AMQP semantics apply to `<name>.sink`.
- **SSE reconnects.** If you checkpoint after processing, a crash between the two re-delivers
  the message.

**Make consumers idempotent by using the index (`id` / `messageId` header) as the key.**

### Duplicate suppression

When a message is ingested, EventStore compares the SHA-256 of its `body` with the most recent
`eventstore.lookback` stored messages (default 100, the dev profile uses 2000). If it matches, the
message is skipped: REST returns `202 Accepted` and AMQP input acknowledges it without storing it.

This is meant to absorb producers **re-sending after an EventStore restart**. It is not general
deduplication:

- Only `body` is hashed. Two different message types with identical bodies count as duplicates,
  so include something unique (an id, a timestamp) in every body.
- The check runs only for the first `lookback` messages after each startup and then switches off
  until the next restart.
