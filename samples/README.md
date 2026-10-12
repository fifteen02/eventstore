# EventStore samples

The step-by-step tutorial for these samples, with the setup and a picture for every step, is in the
main [README](../README.md#tutorial).

| Step | Sample | What it shows |
|---|---|---|
| 1 | [01-quickstart](01-quickstart) | publish and read with `curl` |
| 2 | [02-pull-stream](02-pull-stream) | a pull consumer that resumes after any restart |
| 3 | [03-push-replayable](03-push-replayable) | push with acknowledgements and replay |
| 4 | [04-push-realtime](04-push-realtime) | fire-and-forget push |
| 5 | [05-sharded-readers](05-sharded-readers) | splitting the log across readers by customer |
| 6 | [06-amqp-producer](06-amqp-producer) | publishing through a queue instead of REST |
| 7 | [07-projection](07-projection) | a projection with snapshots that survives `kill -9` |

Each sample folder has its own README with the full walkthrough.

```text
samples/
├── docker-compose.yml        PostgreSQL + LavinMQ
├── bin/                      run-eventstore.sh, publish-orders.sh
├── pom.xml                   parent of the Java samples (not part of the main build)
├── 01-quickstart/            curl scripts
├── 02-pull-stream/           StreamConsumer.java
├── 03-push-replayable/       BillingConsumer.java + eventstore.yml
├── 04-push-realtime/         NotificationsConsumer.java + eventstore.yml
├── 05-sharded-readers/       ShardedReaders.java
├── 06-amqp-producer/         AmqpPublisher.java + eventstore.yml
└── 07-projection/            OrderProjection.java, ProjectionService.java
```
