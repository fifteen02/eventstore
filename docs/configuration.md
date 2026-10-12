# Configuration reference

EventStore is a Spring Boot application, so every property below can be set in a YAML file, as a
command-line argument (`--eventstore.lookback=500`) or as an environment variable
(`EVENTSTORE_LOOKBACK=500`).

The easiest way to add your own settings, especially interfaces, is a separate file:

```bash
java -jar eventstore-core-1.0-SNAPSHOT.jar --spring.config.additional-location=file:/etc/eventstore/eventstore.yml
# or: SPRING_CONFIG_ADDITIONAL_LOCATION=file:/etc/eventstore/eventstore.yml
```

Each sample ships a commented example, for instance [`samples/03-push-replayable/eventstore.yml`](../samples/03-push-replayable/eventstore.yml).

## Profiles

The active profile (`SPRING_PROFILES_ACTIVE` or `--spring.profiles.active`) selects one of the
bundled files. No profile is active by default, so `dev` must be set explicitly:

| Profile | Intended for | Notes |
|---|---|---|
| none | deployments outside Google Cloud | `EVENTSTORE_BEARER_TOKEN` has no default and must be set, or startup fails. Console logging with `com.fifteen02` at DEBUG (set `--logging.level.com.fifteen02=INFO` to quieten it), `eventstore.lookback` 100, no `eventstore.input.queue`. |
| `dev` | local development, samples | Bearer token defaults to `dev-eventstore-token`, database port defaults to `5433`, DEBUG logging, SQL logging, metrics/trace export off. |
| `staging`, `live` | deployments on **Google Cloud** | Logs are sent to Google Cloud Logging (needs GCP credentials), INFO logging. `EVENTSTORE_BEARER_TOKEN` has no default and must be set. |

The `dev` token `dev-eventstore-token` is public, so never run `dev` where others can reach it.
Outside Google Cloud, run with no profile and set what you need (for example
`EVENTSTORE_BEARER_TOKEN`, `DB_HOST`, and `--eventstore.input.queue=eventstore.input` for AMQP
input). Log output is only configured for the `default`, `test`, `dev`, `staging` and `live` profiles. A profile with any other name
prints the banner and nothing else, including startup errors.

## Core

| Property | Env var | Default | Description |
|---|---|---|---|
| `eventstore.auth.bearer-token` | `EVENTSTORE_BEARER_TOKEN` | `dev-eventstore-token` (dev only) | Token required on `/api/events/message`, `/api/events/aggregate-message`, `/api/events/stream`, `/api/debug/messages` and `/api/debug/overview`. Startup fails if it is empty. |
| `eventstore.lookback` | | `100` (`2000` in bundled profiles) | Recent content hashes used for [duplicate suppression](concepts.md#duplicate-suppression). `0` disables it. |
| `eventstore.sse.heartbeat-interval-seconds` | | `5` | Interval between SSE `heartbeat` events (minimum 1). |
| `eventstore.catchup.max-segment-size` | | `1000` | Messages loaded per query during replay. |
| `server.port` | `SERVER_PORT` | `8083` | HTTP port. |

## Database (PostgreSQL)

Tables are created and updated automatically on startup (`spring.jpa.hibernate.ddl-auto=update`).

| Env var | Default | Description |
|---|---|---|
| `DB_HOST` | `localhost` | |
| `DB_PORT` | `5432` (`5433` in dev) | |
| `DB_NAME_EVENTSTORE` | `eventstore` | |
| `DB_USERNAME_EVENTSTORE` | `eventstore` | |
| `DB_PASSWORD_EVENTSTORE` | `changeme` | Change it. |

Pool settings live under `spring.datasource.hikari.*` (default pool size 10).

## AMQP

| Property | Default | Description |
|---|---|---|
| `eventstore.amqp.enabled` | `false` | Master switch. When `false`, the input queue and all interfaces are skipped; REST and SSE still work. |
| `spring.rabbitmq.host` / `port` | `localhost` / `5672` | Broker address. Any AMQP 0-9-1 broker (RabbitMQ, LavinMQ). |
| `spring.rabbitmq.username` / `password` | `guest` / `guest` | |
| `spring.rabbitmq.virtual-host` | `/` | All queues are used on this virtual host. |
| `eventstore.input.max-body-bytes` | `1048576` (1 MiB) | Largest body accepted by `PUT /api/events/message` and `/aggregate-message`; larger requests get `413`. |
| `eventstore.input.queue` | `eventstore.input` (bundled profiles) | Queue that producers publish to. Declared durable on startup. Required when AMQP is enabled: if it is empty, the input listener keeps failing to declare a generated queue. |

## Interfaces

`eventstore.interfaces` is a list. Each entry becomes one push consumer (see
[consuming](consuming.md#push-amqp-interfaces)).

```yaml
eventstore:
  interfaces:
    - name: billing                 # queues: billing.sink, billing.ack
      type: replayable              # realtime | replayable | persister
      include-types: [OrderPlaced, OrderCancelled]
      # exclude-types: [StartMarker]
      sink:
        amqp:
          virtual-host: /
      ack:
        amqp:
          virtual-host: /
        batch: 1000
        prefetch: 1024
        lag-alert-batch-ratio: 1.5
        lag-alert-min-messages: 1000
```

| Property | Default | Description |
|---|---|---|
| `name` | required | Unique name. Becomes the queue prefix and the key of the interface's saved position. Renaming an interface starts it from scratch. |
| `type` | required | `realtime`, `replayable` or `persister` (see [interface types](concepts.md#interface-types-push)). |
| `include-types` | all | Only these message types are sent. Case-insensitive. Not allowed on a `persister`, which must archive everything; startup fails otherwise. |
| `exclude-types` | none | These types are never sent. Applied after `include-types`. Not allowed on a `persister`. |
| `sink.amqp.virtual-host` | | **Set `sink.amqp`.** It is what makes EventStore declare `<name>.sink`. Without it EventStore still publishes to that queue name, and if the queue doesn't exist the broker drops the messages. |
| `ack` | | **Required** for `replayable` and `persister`, **forbidden** for `realtime`. Startup fails otherwise. |
| `ack.amqp.virtual-host` | | Makes EventStore declare `<name>.ack`. |
| `ack.batch` | `1000` | Save the interface's position every N contiguous acks. Lower means less redelivery after a restart but more database writes. |
| `ack.prefetch` | `1024` | Prefetch on the `<name>.ack` consumer. |
| `ack.lag-alert-batch-ratio` | `1.5` | Lag alert threshold as a multiple of `batch`. |
| `ack.lag-alert-min-messages` | `batch` | Lower bound of the lag alert threshold. |

> The `virtual-host` values only control where queues are **declared**. Publishing and ack
> consumption always use `spring.rabbitmq.virtual-host`, so set both to the same value.

To configure interfaces with environment variables, use indexed names:
`EVENTSTORE_INTERFACES_0_NAME=billing`, `EVENTSTORE_INTERFACES_0_TYPE=replayable`,
`EVENTSTORE_INTERFACES_0_INCLUDETYPES=OrderPlaced,OrderCancelled`, and so on. A file is usually clearer.

### Pausing an interface

Each interface has a row in the `interface_state` table. Setting `is_running = false` makes the
interface stop publishing live messages. For a `replayable` interface, skipped messages are
auto-acknowledged and are **not** replayed later. A paused `persister` instead holds its position
and catches up when it runs again, so the archive never misses a message. There is no API for this
yet.

## Archiving and retention (Google BigQuery)

These settings only apply when the PostgreSQL table is archived to BigQuery. See
[operations](operations.md#archiving-and-retention).

| Property | Env var | Default | Description |
|---|---|---|---|
| `eventstore.big-query.enabled` | `EVENTSTORE_BIG_QUERY_ENABLED` | `false` | Read archived ranges from BigQuery during replay, and enable the retention job. |
| `eventstore.big-query.dataset` | `DATASET` | | BigQuery dataset. |
| `eventstore.big-query.table` | `TABLE` | | BigQuery table. |
| `eventstore.realtime-retention.enabled` | `RETENTION_JOB_ENABLED` | none, required when BigQuery is enabled | Delete old rows from PostgreSQL. |
| `eventstore.realtime-retention.retention-days` | `RETENTION_DAYS_TO_KEEP` | none, required when BigQuery is enabled | Keep this many days in PostgreSQL. |
| `eventstore.realtime-retention.delete-batch-size` | `DELETE_BATCH_SIZE` | none, required when BigQuery is enabled | Rows deleted per statement. |
| `eventstore.realtime-retention.cron` | `RETENTION_JOB_CRON` | none, required when BigQuery is enabled | Spring cron, for example `0 0 3 * * *`. `-` disables scheduling. |

## Observability

| Property | Description |
|---|---|
| `management.endpoints.web.exposure.include` | Defaults to `health,info,metrics`. Health is at `/actuator/health`. |
| `management.tracing.enabled`, `management.otlp.*` | Standard Spring Boot OpenTelemetry settings. Turned off in `dev`. |

See [operations](operations.md#observability) for the optional OpenTelemetry Java agent build.
