# Development

## Layout

```text
eventstore-parent            root reactor (pom.xml)
├── eventstore-common        client-facing constants and DTOs: MessageHeaders, AcknowledgeTo
├── eventstore-core          the Spring Boot service
│   └── src/main/java/com/fifteen02
│       ├── Application.java          boots the Pekko actor system and AMQP listeners
│       ├── AmqpInitializer.java      declares input/sink/ack queues
│       ├── EventStoreStartup.java    writes the StartMarker on every start
│       └── eventstore
│           ├── controller            REST, SSE and debug endpoints
│           ├── amqp                  AMQP input listener
│           ├── service               ingestion, deduplication, index, catch-up planning, retention
│           ├── pekko                 router, interface actors, ack handling
│           ├── entity, repo          JPA model (messages, interface_state, persister_state)
│           └── config                properties, auth filter, AMQP wiring
├── samples                  runnable samples (not part of the reactor, never published)
└── docs                     user documentation
```

## How a message flows

1. `EventInputController` (REST) or `EventstoreInputHandler` (AMQP) validates the input.
2. `MessageIngestionService.ingest` hashes the body, asks `DuplicateDetectionService`, reserves
   the next index from `IndexService`, stores the row, marks the index committed and sends
   `RouterActor.Route`. These steps run in one synchronized section, so an index becomes visible
   only after its row is committed.
3. `RouterActor` forwards the message to every interface actor and every open SSE stream.
4. `RealtimeInterface`, `ReplayableInterface` and `PersisterInterface` publish to `<name>.sink`.
   `AckQueueActor` consumes `<name>.ack` and feeds `BatchAcknowledger`, which persists contiguous
   progress.
5. `StreamingInterface` serves one SSE connection. It replays through `CatchUpPlannerService`,
   then switches to live messages, and `StreamingInterfaceMessageWriter` applies the filters.

## Build

```bash
mvn -DskipTests package
```

Java 25 is required (see `.java-version`).

## Test

Unit tests:

```bash
mvn test
```

Unit and integration tests. Integration tests (`*IT`, run by Failsafe) use Testcontainers with
PostgreSQL and LavinMQ, so Docker must be running:

```bash
mvn verify
```

A single integration test:

```bash
mvn -pl eventstore-core -am verify -Dit.test=AmqpEndToEndIT -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false
```

## Run locally

```bash
docker compose up -d
```

```bash
java -jar eventstore-core/target/eventstore-core-1.0-SNAPSHOT.jar --spring.profiles.active=dev
```

Without `--spring.profiles.active=dev` no profile is active, and startup fails unless
`EVENTSTORE_BEARER_TOKEN` is set.

The root `docker-compose.yml` starts PostgreSQL only. For AMQP, use the
[samples](../README.md#tutorial) stack instead.

## Releases

`.github/workflows/publish-maven.yml` deploys `eventstore-parent`, `eventstore-common` and
`eventstore-core` to GitHub Packages. It runs as a snapshot on every push to `main` and as a
release for tags matching `v*.*.*`, using the tag as the version.
