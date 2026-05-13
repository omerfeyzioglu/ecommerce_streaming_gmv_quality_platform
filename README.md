# Kafka to Flink Sandbox

Simple Java project for learning the Flink DataStream API with Kafka.

Flow:

```text
orders Kafka topic
    -> Flink job
    -> enriched-orders Kafka topic
    -> invalid-orders Kafka topic
    -> gmv-by-minute Kafka topic
```

The Flink job reads each order as JSON, validates it, deduplicates by `eventId`, adds learning fields, and writes results back to Kafka.

## Learning Mode

This repository is a Flink learning sandbox first. The bigger e-commerce platform idea lives in `PROJECT.md`, but the current goal is smaller:

```text
learn one Flink concept
    -> make one tiny code change
    -> run it locally
    -> inspect the Kafka output
```

For now, do not rush into ClickHouse, Airflow, Delta Lake, or FastAPI. First, get comfortable with the core Flink shape:

```text
Source -> DataStream -> Transformation -> Sink
```

## Lesson 1: Job Anatomy + Map

Purpose:

- Understand where a Flink job starts.
- See how Kafka becomes a Flink `DataStream`.
- Practice `map`, which transforms every event one by one.

In `OrderStreamingJob.java`, focus on these parts:

- `StreamExecutionEnvironment`: the Flink job builder.
- `KafkaSource`: reads from the `orders` topic.
- `DataStream<String>`: the stream of raw JSON order events.
- `.map(OrderStreamingJob::addProcessedAt)`: adds processing-time metadata.
- `.map(OrderStreamingJob::addLearningStepForLessonOne)`: adds a simple learning marker.
- `KafkaSink`: writes to the `enriched-orders` topic.

Acceptance check:

When you send sample orders, each message in `enriched-orders` should contain:

```json
{
  "processedAt": "...",
  "learningStep": "map-course"
}
```

## What You Need

Only Docker is required. Java and Maven run inside Docker.

## Start Services

```bash
docker compose up -d
```

Kafka topic and broker state are stored in a named Docker volume, so they persist across `docker compose down` and `docker compose up -d`.
If you run `docker compose down -v`, volumes are deleted and topics must be created again.

Useful UIs:

- Flink UI: http://localhost:8081
- Kafka UI: http://localhost:8080

## Build The Job

```bash
docker run --rm \
  -v "$PWD":/workspace \
  -w /workspace \
  maven:3.9.9-eclipse-temurin-17 \
  mvn clean package
```

The jar will be created here:

```text
target/flink-kafka-sandbox.jar
```

## Topics

Create topics once after first startup (or after `docker compose down -v`).

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic orders \
  --partitions 3 \
  --replication-factor 1

docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic enriched-orders \
  --partitions 3 \
  --replication-factor 1

docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic invalid-orders \
  --partitions 3 \
  --replication-factor 1

docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create --if-not-exists \
  --topic gmv-by-minute \
  --partitions 3 \
  --replication-factor 1
```

To verify:

```bash
docker compose exec kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

## Run The Flink Job

```bash
docker compose exec jobmanager flink run \
  /opt/flink/usrlib/flink-kafka-sandbox.jar
```

## Send Sample Orders

```bash
cat samples/orders.jsonl | docker compose exec -T kafka kafka-console-producer \
  --bootstrap-server kafka:29092 \
  --topic orders
```

For the best practice setup, keep data generation in Python and Kafka publishing in Java, but run them through one script:

```bash
bash scripts/run_orders_pipeline.sh
```

Environment variables you can override:

- `KAFKA_BOOTSTRAP_SERVERS` defaults to `localhost:9092`
- `KAFKA_TOPIC` defaults to `orders`
- `PYTHON_BIN` defaults to `python3`
- `JAVA_BIN` defaults to `java`
- `JAR_PATH` defaults to `target/flink-kafka-sandbox.jar`

This keeps the generator independent from Kafka while giving you one repeatable command for local runs.

## Read The Output

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic enriched-orders \
  --from-beginning
```

You should see the original order JSON plus a new `processedAt` field.

To read invalid or duplicate events:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic invalid-orders \
  --from-beginning
```

To read 1-minute GMV window results:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic gmv-by-minute \
  --from-beginning
```

## Lesson: Checkpoint, Watermark, Deduplication

What changed in the Flink job:

- Checkpointing is enabled every 10 seconds in Java and Docker Compose.
- Watermarks are assigned from the order `eventTime` field with 10 seconds of allowed out-of-order data.
- Invalid records go to `invalid-orders`.
- Duplicate records are detected with keyed state by `eventId`.
- `payment-completed` events are aggregated into 1-minute event-time GMV windows by `currency`.

Interview mental model:

- Checkpoint: Flink snapshots state and Kafka source progress so it can recover after failure.
- Watermark: Flink's event-time progress signal; it tells event-time windows when enough data has arrived.
- Deduplication: a stateful operation; key by a unique event id, remember seen ids with TTL, and route repeated ids away from the clean stream.

Small experiments:

1. Open Flink UI and check the job's Checkpoints tab after the job runs for more than 10 seconds.
2. Send the same JSON line twice with the same `eventId`; the second one should appear in `invalid-orders` with `reason=duplicate-event`.
3. Send `payment-completed` events with close `eventTime` values; after the watermark passes the minute boundary, read `gmv-by-minute`.
4. Send malformed JSON or a negative `amount`; it should go to `invalid-orders`.

## Code To Study

Start here:

```text
src/main/java/com/example/flinkkafkasandbox/OrderStreamingJob.java
```

Read it as the table of contents for the Flink job:

- `env.enableCheckpointing(...)`: checkpoint and recovery lesson.
- `KafkaTopics.ordersSource()`: Kafka source setup.
- `OrderEventTime.watermarkStrategy()`: event-time and watermark lesson.
- `InvalidOrderRouter`: validation and DLQ lesson.
- `DeduplicateByEventId`: keyed state and deduplication lesson.
- `GmvAggregate` + `AddGmvWindowMetadata`: event-time window aggregation lesson.
- `KafkaTopics.stringSink(...)`: Kafka sink setup.

Then open only the file for the concept you are studying:

```text
JobConfig.java               constants: topics, checkpoint/window timing knobs
KafkaTopics.java             Kafka source/sink builders
OrderJson.java               JSON parsing and small JSON transformations
OrderValidator.java          business validation rules
InvalidOrderRouter.java      valid stream vs DLQ side output
OrderEventTime.java          eventTime extraction and watermark strategy
DeduplicateByEventId.java    keyBy(eventId) + ValueState TTL
GmvAggregate.java            adds amount values inside a window
AddGmvWindowMetadata.java    turns a closed window into output JSON
```

The best way to learn this code is not to read every file at once. Follow one path:

```text
Checkpoint:
  OrderStreamingJob.java -> Flink UI Checkpoints tab

Watermark:
  OrderStreamingJob.java -> OrderEventTime.java -> gmv-by-minute output

Deduplication:
  OrderStreamingJob.java -> DeduplicateByEventId.java -> invalid-orders output
```

## Practice Ideas

Try these changes manually:

1. Change the output topic name.
2. Add a new field like `sourceSystem`.
3. Filter only `payment-completed` events.
4. Increase `--parallelism` and compare it with Kafka partition count.
5. Send broken JSON and observe that the job fails. Then decide how you would handle invalid events.

## Stop Services

```bash
docker compose down
```

To delete container state too:

```bash
docker compose down -v
```
