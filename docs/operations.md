# Running EventStore

- [Deployment](#deployment)
- [Run exactly one instance](#run-exactly-one-instance)
- [Security](#security)
- [Archiving and retention](#archiving-and-retention)
- [Observability](#observability)
- [Debug UI](#debug-ui)
- [Broker outages](#broker-outages)
- [Known issues](#known-issues)

## Deployment

### Requirements

- Java 25 (or the Docker image);
- PostgreSQL. Tables are created on first start;
- optionally an AMQP 0-9-1 broker (RabbitMQ, LavinMQ) for the input queue and interfaces.

### Docker

```bash
docker build -f eventstore-core/Dockerfile -t eventstore .

docker run -p 8083:8083 \
  -e DB_HOST=postgres.internal -e DB_PORT=5432 \
  -e DB_USERNAME_EVENTSTORE=eventstore -e DB_PASSWORD_EVENTSTORE=... \
  -e EVENTSTORE_BEARER_TOKEN=... \
  -v /etc/eventstore/eventstore.yml:/config/eventstore.yml \
  -e SPRING_CONFIG_ADDITIONAL_LOCATION=file:/config/eventstore.yml \
  eventstore
```

The image runs with no profile unless `SPRING_PROFILES_ACTIVE` is set, so `EVENTSTORE_BEARER_TOKEN`
must be set. Read [profiles](configuration.md#profiles) before choosing one: `staging` and `live`
assume Google Cloud, and `dev` uses a public token.

To include the OpenTelemetry Java agent (see [below](#opentelemetry-java-agent-google-cloud)),
build with the `google-otel` Maven profile:

```bash
docker build -f eventstore-core/Dockerfile --build-arg MAVEN_PROFILES=google-otel -t eventstore .
```

### Plain JVM

```bash
mvn -pl eventstore-core -am -DskipTests package
EVENTSTORE_BEARER_TOKEN=... java -jar eventstore-core/target/eventstore-core-1.0-SNAPSHOT.jar
```

### Health

`GET /actuator/health` (unauthenticated) reports the overall status (`UP` or `DOWN`) for probes.
Details, such as the database check, are not shown.

## Run exactly one instance

EventStore assigns indices, deduplicates and routes in memory, inside one process. **Run a single
instance against a database.** Two instances against the same database would hand out
conflicting indices.

This also applies during deploys. On shutdown, EventStore waits up to
`spring.lifecycle.timeout-per-shutdown-phase` (30 s) for open requests, and open SSE streams count,
so with SSE clients connected a shutdown takes about 30 seconds.
Stop the old instance completely before starting the new one (for example a Kubernetes
`Recreate` strategy rather than `RollingUpdate`). SSE clients reconnect and resume from their
checkpoint, and AMQP producers' messages wait in the input queue.

## Security

- **Bearer token.** One shared token protects `/api/events/message`, `/api/events/aggregate-message`,
  `/api/events/stream`, `/api/debug/messages` and `/api/debug/overview`. It is compared in constant time. There are no
  per-client tokens or scopes. Put EventStore behind your own gateway if you need those.
- **Unauthenticated paths:** `/actuator/**` (only `health` and `info` are exposed, health without
  details) and the debug page `/debug/events`. The debug page itself holds no data, but it is
  served in **every** profile. Block it at your ingress in production.
- **TLS** is not configured in the application. Terminate it in front of EventStore.
- **Database password.** The default `changeme` is for local use only.

## Archiving and retention

By default every message stays in PostgreSQL forever and replays read from there. For large
logs, EventStore can archive to **Google BigQuery** and trim PostgreSQL:

1. Configure a `persister` interface named **`google-big-query`**. The name must match exactly,
   because that is the name of the BigQuery reader:

   ```yaml
   eventstore:
     interfaces:
       - name: google-big-query
         type: persister
         sink: { amqp: { virtual-host: / } }
         ack:  { amqp: { virtual-host: / }, batch: 1000 }
   ```

2. Run an archiver (your code) that consumes `google-big-query.sink`, writes each message to
   BigQuery, and acks it on `google-big-query.ack` like any [acked interface](consuming.md#acknowledging).
   EventStore records the acknowledged range as covered by the archive.
3. The archive table must expose these columns, which EventStore reads back during replay:

   | Column | Type | From sink message |
   |---|---|---|
   | `eventstore_id` | INT64 | `messageId` header |
   | `message_type` | STRING | `messageType` header |
   | `content` | STRING (JSON) | body. Rows whose content is not valid JSON are skipped silently during replay. |
   | `content_hash` | STRING | `contentHash` header |
   | `timestamp` | TIMESTAMP | `timestamp` header |
   | `user_id`, `correlation_id` | STRING | headers |
   | `traceparent`, `tracestate`, `baggage` | STRING | headers |
   | `metadata` | STRING (JSON) | `metadata` header |
   | `insert_time` | TIMESTAMP | time you wrote the row; the newest row per `eventstore_id` wins |

4. Enable reading and retention:

   ```bash
   EVENTSTORE_BIG_QUERY_ENABLED=true DATASET=eventstore TABLE=messages \
   RETENTION_JOB_ENABLED=true RETENTION_DAYS_TO_KEEP=30 DELETE_BATCH_SIZE=10000 RETENTION_JOB_CRON="0 0 3 * * *"
   ```

   The process needs Google application default credentials.

The retention job deletes PostgreSQL rows older than the cutoff only if the archive covers the
whole range being deleted. Otherwise it logs a warning and deletes nothing. Replays then read
old ranges from BigQuery and recent ones from PostgreSQL.

Aggregate fields (`aggregateUuid`, `aggregateIndex`) are not sent as sink headers and are not
read back from BigQuery, so replays from the archive return them as `null`.

## Observability

- **Tracing.** Ingestion, interface publishing and ack handling create OpenTelemetry spans. W3C
  `traceparent` / `tracestate` / `baggage` are stored with each message and propagated to sink
  messages.
- **Metrics** via Spring Boot Actuator / Micrometer. `/actuator/metrics` is not exposed over HTTP
  by default, because `/actuator/**` has no authentication. To expose it, put the management
  endpoints on a port that only your network can reach:

  ```bash
  --management.server.port=9083 --management.endpoints.web.exposure.include=health,info,metrics
  ```

  Health probes then move to that port too (`http://<host>:9083/actuator/health`).
- **Lag.** `[INTERFACE_BEHIND]` ERROR log lines (see [lag monitoring](consuming.md#lag-monitoring)).

### OpenTelemetry Java agent (Google Cloud)

Building `eventstore-core` with the `google-otel` profile also produces the OpenTelemetry Java
agent and a Google auth extension for it:

```bash
mvn -pl eventstore-core -am -Pgoogle-otel -DskipTests package
# eventstore-core/target/opentelemetry-javaagent-*.jar
# eventstore-core/target/eventstore-core-*-otel-javaagent-extension.jar
```

The extension is a separate jar because the agent loads extensions in its own class loader and
cannot see the application's dependencies. A Docker image built with
`--build-arg MAVEN_PROFILES=google-otel` copies both jars to `/app`.
Attach them with `JAVA_TOOL_OPTIONS="-javaagent:/app/opentelemetry-javaagent.jar -Dotel.javaagent.extensions=/app/opentelemetry-gcp-auth-extension.jar"`.

## Debug UI

`http://localhost:8083/debug/events` is a single page for trying EventStore by hand. Paste the
bearer token into the page to use it.

- **The top** shows the head index, how many messages arrived in the last minute, and a ruler from
  just before the furthest-behind consumer to the head, refreshed every two seconds. Every push
  interface is a filled point at its saved position. Every open HTTP stream (`/api/events/stream`)
  is a hollow point at the last index it has passed, labelled with its client (the
  `X-Client-Name` header, or else the `User-Agent`) and filters; the server forgets a stream as soon as it closes. Rows are sorted
  with the furthest behind first, and scroll when there are more than about five. A `realtime` interface keeps no
  position, so it shows as live only. A lag up to the interface's `ack.batch` is shown as on time,
  because positions are only saved every `ack.batch` contiguous acks.
- **Stream** opens a filtered SSE stream (`/api/events/stream`) and starts on the newest 50
  messages. Each `replayable` or `persister` interface draws a line across the log at the position
  it has saved; everything above the line it has still to confirm. Click an interface's point at
  the top to scroll to its line.
- **Publish** sends test messages (`PUT /api/events/message`).
- Stream and Publish both show the request as a `curl` command, built from the form as you type,
  with a Copy button, so you can run the same call from a terminal.
- **Messages** pages through stored messages (`/api/debug/messages`).

The top and the interface lines read `GET /api/debug/overview`, which returns the head index and, for each interface,
its type, filters, saved position, lag and `ackBatch`, and every open HTTP stream with its client,
remote address, filters, position and lag. Both debug endpoints need the bearer token.

## Broker outages

If the broker is unreachable when an interface publishes to `<name>.sink`, the publish fails and
the interface restarts, retrying with a backoff from 1 second up to 30 seconds until the broker is
back. A `replayable` or `persister` interface catches up from its saved position on every restart,
so it loses nothing; delivery just pauses, and the `[INTERFACE_BEHIND]` alert shows the lag. A
`realtime` interface has no history to catch up from: a message whose publish fails is dropped,
and the interface carries on with the messages after it once the broker is back. No restart of
EventStore is needed.

## Known issues

- **Errors return no reason.** `400` responses use Spring's default error body. The reason is
  only in the server log.
