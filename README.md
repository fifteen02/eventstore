<div align="center">

<picture>
  <source media="(prefers-color-scheme: dark)" srcset="docs/images/eventstore-lockup-dark.svg">
  <img src="docs/images/eventstore-lockup-light.svg" alt="eventstore" width="420">
</picture>

**An ordered, durable message log you can push and pull.**

Write events once. Let consumers pull them over HTTP from any point in history,
or have them pushed over AMQP with acknowledgements and automatic replay.

![Java 25](https://img.shields.io/badge/Java-25-orange)
![Spring Boot 4](https://img.shields.io/badge/Spring%20Boot-4.0-6db33f)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791)
![AMQP 0-9-1](https://img.shields.io/badge/AMQP-0--9--1-ff6600)

[Quick start](#quick-start) · [Tutorial](#tutorial) · [Concepts](docs/concepts.md) · [Docs](#documentation)

</div>

---

## Why EventStore?

Sooner or later, many systems grow the same plumbing. One service emits events, several others need
them, and soon you have a queue per consumer, a table for replays, an index so replays come back
in order, and a script for when a consumer falls behind. EventStore is that plumbing as one small
service:

- **One log, one index.** Every message gets a global, monotonically increasing index. It is the
  message's identity, its position, and your idempotency key.
- **Pull.** A client asks for `startIndex=N` over Server-Sent Events and gets history followed by
  live messages on the same connection, with no gap and no seam to stitch together.
- **Push.** A service declared in configuration gets messages on its own AMQP queue. EventStore
  tracks its acknowledgements and, after a restart, replays from where the service left off.
- **Small to run.** One Spring Boot process and PostgreSQL. An AMQP broker is optional, and so is
  archiving to BigQuery.

<p align="center"><img src="docs/images/eventstore-overview.png" alt="Producers write into one ordered log; services get messages pushed over AMQP, others pull over HTTP from any index" width="820"/></p>

## Features

| Area | What you get |
|---|---|
| **Ingest** | REST (`PUT /api/events/message`, `/aggregate-message`) or an AMQP input queue; bearer-token auth |
| **Pull (SSE)** | start at any index, bounded replays (`endIndex`), filters by message type and user, sharding across readers, heartbeats with lag |
| **Push (AMQP)** | `replayable` interfaces with acks and catch-up, `realtime` fire-and-forget interfaces, per-interface type filters |
| **Guarantees** | durable, gap-free ordering; at-least-once delivery; duplicate suppression for producer retries after a restart |
| **Metadata** | user id, correlation id, aggregate id and index, free-form JSON metadata, W3C trace context |
| **Operations** | health and metrics via Actuator, OpenTelemetry tracing, lag alerts, optional BigQuery archive with PostgreSQL retention, a [debug console](#debug-console) in the browser |

## Quick start

You need Docker, Java 25 and Maven.

```bash
docker compose up -d
```

```bash
mvn -q -DskipTests package
```

```bash
java -jar eventstore-core/target/eventstore-core-1.0-SNAPSHOT.jar --spring.profiles.active=dev
```

The `dev` profile is for local use only. It accepts the public token `dev-eventstore-token`.

Publish a message:

```bash
curl -X PUT http://localhost:8083/api/events/message \
  -H 'Authorization: Bearer dev-eventstore-token' \
  -H 'Content-Type: application/json' \
  -d '{"messageType":"OrderPlaced","userId":"customer-42","content":{"orderId":"o-1001","total":49.90}}'
```

```text
{"id":2,"messageType":"OrderPlaced","body":{"orderId":"o-1001","total":49.9},"userId":"customer-42", ...}
```

Read the log from the beginning and stay connected for new messages:

```bash
curl -N -H 'Authorization: Bearer dev-eventstore-token' 'http://localhost:8083/api/events/stream?startIndex=1'
```

```text
event:connected
data:{"message":"connected at ...","filters":{...},"lastPersistedIndex":2}

id:1
event:message
data:{"message":{"id":1,"messageType":"StartMarker",...},"lastPersistedIndex":2}

id:2
event:message
data:{"message":{"id":2,"messageType":"OrderPlaced","body":{"orderId":"o-1001","total":49.9},...},"lastPersistedIndex":2}
```

Index 1 is the `StartMarker` that EventStore writes each time it starts. Publish again in
another terminal and the new message appears on the open stream.

There is also a browser console for watching the log, see [Debug console](#debug-console).

## Debug console

EventStore ships with a console at <http://localhost:8083/debug/events> for seeing what the log and
its consumers are doing. It is a single page served by EventStore itself, with nothing to install.
Paste the bearer token into the **Token** field at the top right (`dev-eventstore-token` under the
`dev` profile; the page remembers it).

![The debug console: the head of the log, every push interface and open stream on a ruler, and the log streaming in live with billing's saved position drawn across it](docs/images/debug-console.png)

### Who is behind

The top of the page shows the **head** (the newest index) and how many messages arrived in the last
minute. The ruler next to it runs from a little before the furthest-behind consumer to the head:

- a **filled point** is a push interface from your configuration, at the position it has saved.
  Green means on time: it is behind by no more than its `ack.batch`, because positions are only
  saved every `ack.batch` acks. Amber means it is further behind than that. A `realtime` interface
  keeps no position and shows as *live only*;
- a **hollow point** is an open HTTP stream (`/api/events/stream`) at the last index it has passed.
  It is labelled with its `X-Client-Name` header (or the first product in its `User-Agent`) and its
  filters. Hover it for the stream id, address and start index;
- the band from a point to the head is how far that consumer is behind. Rows are sorted with the
  furthest behind first and scroll when there are more than about five. Click a point to jump to
  its line in the log below.

### Stream

**Stream** reads the log over SSE, starting on the newest 50 messages and then live. Set **From**,
**To**, **Type**, **User** or **Shard** to narrow it, and **Name** to choose how the stream appears in
the ruler. Every `replayable` or `persister` interface draws a line across the log where its saved
position sits, so the messages above the line are the ones it still has to confirm. Click any row
to see the whole message.

The **curl** line under the filters is the same request as a command, built as you type. **Copy**
it to run the stream from a terminal; it names itself with `X-Client-Name`, so you can spot it at
the top.

### Publish

**Publish** sends test messages. Pick a preset (each one gets a fresh order id, so it is never a
duplicate) or **Custom** for any JSON, then **Publish**, or **Publish 10** for a burst. Every attempt
leaves a receipt: the index it was stored at, *Duplicate* when the body matches a recent message,
or why it was refused. The **curl** line shows the same `PUT` to copy.

![The Publish tab: presets and fields on the left, receipts on the right, including one duplicate](docs/images/debug-console-publish.png)

### Messages

**Messages** pages through everything stored, newest or oldest first, 25 at a time. Interface lines
show here too, on the page where the saved position falls.

![The Messages tab: a page of stored messages with the billing interface's line across it](docs/images/debug-console-messages.png)

### Message details

Click any message for its details: user, stored time, correlation id, aggregate, trace context,
content hash, body and metadata. **Stream from here** opens the Stream tab starting at that index.

![The message panel for #323, an OrderPlaced message, with its facts and JSON body](docs/images/debug-console-message.png)

### When something is wrong

- *token rejected*: the token at the top right does not match `eventstore.auth.bearer-token`.
- *EventStore unreachable*: the numbers at the top are the last ones received; they update by
  themselves when EventStore is back.
- *Streaming · filtered*: with a filter set the console cannot tell how far behind the stream is,
  so it does not guess.

More detail, including the `/api/debug/overview` endpoint behind the top of the page, is in
[docs/operations.md](docs/operations.md#debug-ui).

## Tutorial

This tutorial takes you from an empty machine to a set of services that publish to EventStore,
pull from it, get messages pushed to them, split the work between readers, and fold the log into
state that survives a crash. It has seven steps, and each one is a small program you run and
poke at.

Every step uses the same story: a small online shop. Its orders publish `OrderPlaced`,
`OrderShipped` and `OrderCancelled` events for customers `customer-0`, `customer-1`, and so on.
The log carries over from step to step, so the message indices you see keep growing. You can
also jump straight to any step: each one only needs the setup below.

| Step | What you build | EventStore runs with | Terminals |
|---|---|---|---|
| [1](#step-1--publish-and-read-with-curl) | publish and read with `curl` | plain config | 2 |
| [2](#step-2--a-consumer-that-remembers-where-it-was) | a pull consumer that resumes after any restart | plain config | 3 |
| [3](#step-3--let-eventstore-push-with-replay) | a billing service EventStore pushes to, with acknowledgements and replay | `03-push-replayable` | 3 |
| [4](#step-4--push-without-replay) | a notifications service with fire-and-forget push | `04-push-realtime` | 3 |
| [5](#step-5--split-the-log-across-readers) | three readers that split the log by customer | plain config | 2 |
| [6](#step-6--publish-over-amqp) | a producer that publishes through a queue instead of REST | `06-amqp-producer` | 2 |
| [7](#step-7--fold-the-log-into-state) | a projection with snapshots that survives `kill -9` | plain config | 3 |

New to the ideas? [Concepts](docs/concepts.md) explains the log, the global index, push vs pull
and the delivery guarantees in a few minutes. It is optional; the steps explain what they use.

### Before you start

#### What you need

- **Docker**, to run PostgreSQL and the AMQP broker.
- **Java 25** and **Maven 3.9+**, to build EventStore and run the Java samples.
- **curl**, for step 1.

Run every command in this tutorial from the **repository root**.

#### 1. Start the infrastructure

```bash
docker compose -f samples/docker-compose.yml up -d --wait
```

This starts two containers:

| Container | What it is for | Where |
|---|---|---|
| `eventstore-sample-postgres` | EventStore's storage: the log, interface positions, settings | `localhost:5433`, database / user `eventstore`, password `changeme` |
| `eventstore-sample-lavinmq` | an AMQP 0-9-1 broker (RabbitMQ-compatible), used from step 3 on | `localhost:5672`; management UI at <http://localhost:15672> (guest / guest) |

The repository's root `docker-compose.yml` also uses port 5433, so stop it first if it is running.
The LavinMQ UI is worth keeping open from step 3 on: you can watch queues fill and drain.

#### 2. Build EventStore

```bash
mvn -q install -DskipTests
```

This builds the EventStore server jar and installs `eventstore-common` (the shared header and type
constants) into your local Maven repository, where the Java samples pick it up.

#### 3. Know the two helper scripts

**`samples/bin/run-eventstore.sh [step]`** starts EventStore in the foreground on
<http://localhost:8083>:

- with no argument, it runs the plain development config: REST publishing and SSE streams, no AMQP;
- with a step name, such as `03-push-replayable`, it also loads that step's `eventstore.yml`, which
  switches AMQP on and declares what that step needs;
- stop it with **Ctrl+C**. The data lives in PostgreSQL, so it survives restarts and switching steps.

The development config accepts the bearer token `dev-eventstore-token`, and every sample uses it by
default. EventStore always writes a `StartMarker` message when it starts, so you will see one of those
in the log after every restart.

**`samples/bin/publish-orders.sh [count]`** publishes shop traffic over REST. Each order is an
`OrderPlaced` and an `OrderShipped`, and every third order is also an `OrderCancelled`. Orders rotate over
three customers; set `CUSTOMERS=50` to spread them wider.

#### Settings every sample reads

| Variable | Default | Used by |
|---|---|---|
| `EVENTSTORE_URL` | `http://localhost:8083` | every step |
| `EVENTSTORE_TOKEN` | `dev-eventstore-token` | every step |
| `AMQP_HOST`, `AMQP_PORT`, `AMQP_USERNAME`, `AMQP_PASSWORD`, `AMQP_VHOST` | `localhost`, `5672`, `guest`, `guest`, `/` | steps 3, 4 and 6 |

You only need to set these to point the samples at a different EventStore or broker.

---

### Step 1 · Publish and read with `curl`

**Goal:** see the log with nothing but HTTP. Every message gets a global index; you can read any
range of the log back, or keep a stream open and watch new messages arrive.

<p align="center"><img src="docs/images/samples/01-quickstart.png" alt="publish.sh PUTs a message, EventStore stores it at the next index in PostgreSQL, and stream.sh replays indices 1 to 3 over SSE" width="760"/></p>

**Setup for this step:** EventStore with the plain config. No AMQP, no Java.

**Terminal 1**, EventStore:

```bash
samples/bin/run-eventstore.sh
```

**Terminal 2**, publish one order and read the start of the log back:

```bash
samples/01-quickstart/publish.sh
samples/01-quickstart/stream.sh startIndex=1 endIndex=3
```

`publish.sh` answers `HTTP 201` with the stored message. Its `id` is the **global index**, the
message's position in the log. The stream replays indices 1 to 3 as Server-Sent Events (`event:message`
with `id:1`, `id:2`, …) and closes with `event:stream-ended`, because you gave it an `endIndex`.

Now drop the `endIndex`:

```bash
samples/01-quickstart/stream.sh
```

The stream replays everything and then **stays open**. Publish from another terminal with
`samples/bin/publish-orders.sh` and watch the new messages arrive live. Every five seconds a
`heartbeat` reports `lastPersistedIndex`, so a reader always knows how far behind it is.

**Try this:**

- Filters: `samples/01-quickstart/stream.sh messageType=orderplaced userId=customer-1`.
- Duplicate suppression: send the same body twice and get `201`, then `202` (accepted, not stored).

**What you learned:** one ordered log and one index; replay and live are the same stream; the reader
chooses where to start.
**Full walkthrough:** [01-quickstart](samples/01-quickstart).

---

### Step 2 · A consumer that remembers where it was

**Goal:** turn step 1's `curl` into a real consumer that survives its own restarts, EventStore
restarts and dropped connections, without a single special case.

<p align="center"><img src="docs/images/samples/02-pull-stream.png" alt="StreamConsumer asks EventStore for startIndex = checkpoint + 1, receives history then live messages, and saves each index to its checkpoint file after processing" width="760"/></p>

**Setup for this step:** EventStore with the plain config. The consumer is a plain Java program with
no SSE library. It keeps its **checkpoint** (the last index it processed) in
`samples/02-pull-stream/target/stream.checkpoint`.

**Terminal 1**, EventStore:

```bash
samples/bin/run-eventstore.sh
```

**Terminal 2**, the consumer:

```bash
mvn -q -f samples/02-pull-stream compile exec:java
```

**Terminal 3**, traffic:

```bash
samples/bin/publish-orders.sh 2
```

The consumer first replays the log, printing `(n behind)` as it catches up, then prints new
messages live on the same connection.

**Now break it:**

1. Stop the consumer (Ctrl+C). Run `samples/bin/publish-orders.sh 1`. Start the consumer again and it
   prints `connected from <checkpoint + 1>`. It picks up the missed messages, nothing more and nothing less.
2. Restart EventStore instead. The consumer prints `reconnecting in 2s` and carries on.

**Try this:** `CHECKPOINT_FILE=/tmp/placed.cp END_INDEX=4 STREAM_TYPES=OrderPlaced mvn -q -f samples/02-pull-stream exec:java`
runs a bounded, filtered replay with its own checkpoint.

**What you learned:** with **pull**, the client owns its position. Process first, then save the
checkpoint: a crash in between means a message is processed twice, never skipped
(at-least-once).
**Full walkthrough:** [02-pull-stream](samples/02-pull-stream).

---

### Step 3 · Let EventStore push, with replay

**Goal:** a billing service that does not track anything itself. EventStore pushes it orders over
AMQP, remembers how far billing has acknowledged, and replays from there after a restart.

<p align="center"><img src="docs/images/samples/03-push-replayable.png" alt="EventStore pushes to billing.sink in LavinMQ, Billing processes and acks on billing.ack, and EventStore saves the acknowledged position in interface_state" width="760"/></p>

**Setup for this step:** EventStore with [`03-push-replayable/eventstore.yml`](samples/03-push-replayable/eventstore.yml).
That file:

- switches AMQP on and points EventStore at the LavinMQ container;
- declares one **interface** named `billing`, of type `replayable`, that receives `OrderPlaced` and
  `OrderCancelled`;
- makes EventStore declare two queues: `billing.sink` (EventStore publishes there) and `billing.ack`
  (billing answers there);
- sets `batch: 5`, so billing's position is saved every 5 acknowledged messages. The default is 1000;
  it is small here so you can see restarts re-deliver.

The consumer keeps a ledger of processed indices in `samples/03-push-replayable/target/billing.ledger`,
so a re-delivered message is recognised and skipped.

**Terminal 1**, EventStore with this step's config:

```bash
samples/bin/run-eventstore.sh 03-push-replayable
```

Look for `Interface [replayable] 'billing' started` and a `catch-up completed` line. On its first
start a replayable interface pushes the **whole history** that matches its types.

**Terminal 2**, the billing consumer:

```bash
mvn -q -f samples/03-push-replayable compile exec:java
```

**Terminal 3**, traffic:

```bash
samples/bin/publish-orders.sh 3
```

For each message billing **processes** it, **acks to EventStore** on `billing.ack` (this moves
billing's position) and **acks to the broker** (this removes the message from `billing.sink`).
Check the saved position:

```bash
docker exec eventstore-sample-postgres psql -U eventstore -tAc 'select name, last_acknowledged_id from interface_state'
```

**Now break it:**

1. Stop the consumer and publish. The messages wait in `billing.sink` (watch it in the LavinMQ UI).
   Start the consumer and it drains them.
2. Publish two more orders, then restart EventStore while the consumer keeps running. EventStore
   replays everything after the last saved position, and billing prints `#21 duplicate, skipped`.
   That is at-least-once, made harmless by the ledger.

**What you learned:** with **push**, EventStore owns the position, but the consumer must still be
idempotent. Your real service needs the same protection, usually a unique constraint on the index.
**Full walkthrough:** [03-push-replayable](samples/03-push-replayable).

---

### Step 4 · Push without replay

**Goal:** see the cheaper kind of push, for consumers that only care about now: notifications,
cache invalidation, live dashboards.

<p align="center"><img src="docs/images/samples/04-push-realtime.png" alt="EventStore publishes only new OrderShipped messages to notifications.sink for the Notifications consumer; old messages are never sent" width="760"/></p>

**Setup for this step:** EventStore with [`04-push-realtime/eventstore.yml`](samples/04-push-realtime/eventstore.yml).
It declares one `realtime` interface named `notifications` that receives `OrderShipped`, with a
`notifications.sink` queue and **no** ack queue: no acknowledgements, no saved position, no replay.

Restart EventStore with this step's config. Stop step 3's EventStore first; billing's saved
position stays in PostgreSQL for whenever you come back.

**Terminal 1:**

```bash
samples/bin/run-eventstore.sh 04-push-realtime
```

**Terminal 2:**

```bash
mvn -q -f samples/04-push-realtime compile exec:java
```

Nothing arrives, although the log is full of `OrderShipped` messages from earlier steps.
Realtime never sees history.

**Terminal 3:**

```bash
samples/bin/publish-orders.sh 2
```

Now exactly the two new shipments arrive.

**What you learned:** realtime is published once, as messages are stored. If the consumer is down,
messages wait in its queue. If **EventStore** cannot publish at that moment (broker down, queue
deleted), they are gone. Use replayable whenever a message must not be missed.
**Full walkthrough:** [04-push-realtime](samples/04-push-realtime), including a replayable vs realtime table.

---

### Step 5 · Split the log across readers

**Goal:** scale a pull consumer out to N readers, where each customer always lands with the same
reader and no message is read twice.

<p align="center"><img src="docs/images/samples/05-sharded-readers.png" alt="EventStore hashes each userId into one of three shards and each reader streams only its own shard" width="760"/></p>

**Setup for this step:** EventStore with the plain config. Sharding is just two query parameters
on the stream, `shardCount` and `shardIndex`; EventStore hashes each message's `userId` to pick its
shard. Sharding is per customer, so you need many customers for even shards.

**Terminal 1:**

```bash
samples/bin/run-eventstore.sh
```

**Terminal 2**, traffic over 50 customers, then three readers over the whole log:

```bash
CUSTOMERS=50 samples/bin/publish-orders.sh 100
mvn -q -f samples/05-sharded-readers compile exec:java
```

It prints how many messages each shard received, and finishes with
`total N distinct of N stored, 0 delivered twice`.

**Try this:** `SHARDS=2` for two readers. The step's README also shows the skew you get with only
three customers: one shard receives nothing.

**What you learned:** every message goes to exactly one reader, and all of a customer's messages go
to the same reader, in order. Changing the shard count moves customers between readers, so treat it
as a redeployment.
**Full walkthrough:** [05-sharded-readers](samples/05-sharded-readers).

---

### Step 6 · Publish over AMQP

**Goal:** publish without HTTP. Producers that already talk to a broker drop messages on a queue, and
keep working while EventStore restarts.

<p align="center"><img src="docs/images/samples/06-amqp-producer.png" alt="AmqpPublisher publishes to eventstore.input with headers and a JSON body; the broker confirms, EventStore stores the message and then acks the delivery" width="760"/></p>

**Setup for this step:** EventStore with [`06-amqp-producer/eventstore.yml`](samples/06-amqp-producer/eventstore.yml),
which switches AMQP on and declares the durable input queue `eventstore.input`. The producer sends
the event content as the JSON **body**, and everything else as **headers**. `messageType` is required;
`userId`, `correlationId`, `timestamp` and `metadata` are optional.

**Terminal 1:**

```bash
samples/bin/run-eventstore.sh 06-amqp-producer
```

**Terminal 2:**

```bash
mvn -q -f samples/06-amqp-producer compile exec:java
samples/01-quickstart/stream.sh messageType=OrderCancelled
```

The producer uses publisher confirms and ends with `all messages confirmed by the broker`. The
stream shows the new messages in the log, with their headers mapped to fields.

**Try this:** stop EventStore, run the producer, and look at `eventstore.input` in the LavinMQ UI.
The messages wait there. Start EventStore and they are stored.

**What you learned:** AMQP input trades the REST reply (you don't learn the index) for decoupling.
EventStore acknowledges each input message only after storing it. A message without `messageType`
is rejected, so give the queue a dead-letter policy if you want to inspect those.
**Full walkthrough:** [06-amqp-producer](samples/06-amqp-producer).

---

### Step 7 · Fold the log into state

**Goal:** build what most consumers really are: a **projection** that turns events into state
(orders and spend per customer), restarts in seconds from a snapshot instead of re-reading the whole
log, holds back side effects while catching up, and serves its state to others.

<p align="center"><img src="docs/images/samples/07-projection.png" alt="ProjectionService folds the log into totals per customer, snapshots the state with its index, and serves it on /state and /stream" width="760"/></p>

**Setup for this step:** EventStore with the plain config. The projection is a pull consumer like
step 2, but instead of a checkpoint it saves a **snapshot**: the state together with the index it
reflects, written atomically to `samples/07-projection/target/projection.snapshot.json`. It serves
that state on <http://localhost:8093>: `/state` returns JSON, and `/stream` gives the current state on
connect, then live updates.

**Terminal 1:**

```bash
samples/bin/run-eventstore.sh
```

**Terminal 2**, some history first, then the projection:

```bash
CUSTOMERS=4 samples/bin/publish-orders.sh 12
mvn -q -f samples/07-projection compile exec:java
```

It loads its snapshot, catches up (`4 alerts suppressed during catch-up`: a VIP alert about last
week is noise), then snapshots every 20 events.

**Terminal 3**, look at the state and subscribe to it:

```bash
curl -s localhost:8093/state
curl -N localhost:8093/stream
```

Publish more orders with `CUSTOMERS=6 samples/bin/publish-orders.sh 12` and watch the `update` events
arrive and the live VIP alerts fire.

**Now break it:** keep publishing and `kill -9` the projection, then start it again. It resumes from
its last snapshot, and its totals match a fresh instance that recomputes everything from index 1:

```bash
SNAPSHOT_FILE=/tmp/fresh.json PORT=8094 mvn -q -f samples/07-projection exec:java
```

**What you learned:** snapshot the state and its index together, never one without the other. Ignore
events at or below the snapshot index, so re-delivery is harmless. Hold back side effects until you
are caught up.
**Full walkthrough:** [07-projection](samples/07-projection).

---

### Starting over

Stop EventStore (Ctrl+C) and remove the containers. They keep no volumes, so this deletes the log,
the interface positions and every queue:

```bash
docker compose -f samples/docker-compose.yml down
```

The consumers keep their own state next to their code, and it would now be **ahead** of the empty
log. Delete it too:

```bash
rm -f samples/02-pull-stream/target/stream.checkpoint \
      samples/03-push-replayable/target/billing.ledger \
      samples/07-projection/target/projection.snapshot.json
```

### Troubleshooting

| Symptom | Cause and fix |
|---|---|
| `port is already allocated` on 5433 | The root `docker-compose.yml` is running. Stop it, or stop whatever else uses 5433. |
| EventStore fails with `Address already in use` | Another EventStore is still running on 8083. Only one should run at a time. |
| A consumer prints nothing after a reset | Its checkpoint, ledger or snapshot is ahead of the new empty log. Delete it (see above). |
| Step 3 or 4 consumer can't find its queue | EventStore declares the queues on startup. Start EventStore with that step's config first. |
| `401` from EventStore | The token is wrong. The development config expects `dev-eventstore-token`. |
| `mvn` can't resolve `eventstore-common` | Run `mvn -q install -DskipTests` from the repository root once. |

## Push or pull?

| | Push: AMQP interfaces | Pull: SSE streams |
|---|---|---|
| Configured | on the server | by the client, per request |
| Position tracked by | EventStore, from your acks | the client |
| History on first start | `replayable`: all of it; `realtime`: none | from any `startIndex` |
| Scale out | competing consumers on one queue | `shardCount` / `shardIndex` |
| Best for | backend services that must process everything | UIs, projections, replays, sharded readers |

Both read the same log, and you can mix them freely. Delivery is **at-least-once** in both
cases, so make consumers idempotent by keying on the message index. [Concepts](docs/concepts.md)
explains why.

## Documentation

| Guide | What it covers |
|---|---|
| [Concepts](docs/concepts.md) | the log, the index, push vs pull, interface types, delivery guarantees, duplicates |
| [Publishing](docs/publishing.md) | REST and AMQP input, with fields, headers and responses |
| [Consuming](docs/consuming.md) | SSE streams and resuming, AMQP interfaces and acknowledgements |
| [Configuration](docs/configuration.md) | every property, profiles, interface definitions |
| [Operations](docs/operations.md) | deployment, single-instance rule, security, BigQuery archiving, observability, known issues |
| [Development](docs/development.md) | code layout, message flow, build and test |

## Client library

`eventstore-common` holds the AMQP header names (`MessageHeaders`) and the acknowledgement payload
(`AcknowledgeTo`) used by AMQP producers and consumers. Install it from source:

```bash
mvn install -DskipTests
```

```xml
<dependency>
    <groupId>com.fifteen02</groupId>
    <artifactId>eventstore-common</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```

It is also published to GitHub Packages (`https://maven.pkg.github.com/fifteen02/eventstore`).
Maven downloads from GitHub Packages need a personal access token with `read:packages` in your
`~/.m2/settings.xml`, even for public packages.

## Good to know

- **Run one instance per database.** Index assignment happens inside a single process. See
  [operations](docs/operations.md#run-exactly-one-instance).
- **Consumers must be idempotent.** Restarts can re-deliver messages; the index is your key.
- **Profiles.** No profile is active by default. `dev` is meant for local use and must be set
  explicitly. `staging` and `live` assume Google Cloud. See [configuration](docs/configuration.md#profiles).
- Open issues are listed under [known issues](docs/operations.md#known-issues).

## Contributing

Issues and pull requests are welcome. To build and run every test (Docker is required for the
integration tests):

```bash
mvn verify
```

[Development](docs/development.md) walks through the code layout and how a message flows through it.
See [CONTRIBUTING.md](CONTRIBUTING.md) for what a pull request needs, and [SECURITY.md](SECURITY.md)
to report a vulnerability.

## License

EventStore is licensed under the [Apache License 2.0](LICENSE). The debug console embeds the Geist
and Geist Mono fonts, licensed under the [SIL Open Font License 1.1](licenses/OFL-Geist.txt). See
[NOTICE](NOTICE).
