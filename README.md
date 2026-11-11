# E-Commerce Streaming GMV & Data Quality Pipeline

A real-time order pipeline built with Apache Flink and Kafka. It reads raw order events, rejects bad data, removes duplicates and computes GMV (gross merchandise value) per currency in 1-minute event-time windows.

```text
                         ┌──────────────────────── Flink job ────────────────────────┐
                         │                                                            │
 orders (Kafka) ──► validate ──► deduplicate ──┬──► add processedAt ──► enriched-orders
   ▲                  │          (keyed state)  │
   │                  │               │         └──► payment-completed ──► 1-min window
   │                  ▼               ▼                                    by currency
 Python generator   invalid-orders (dead-letter topic, with a reason)          │
 + Java producer                                                               ▼
                                                                         gmv-by-minute
```

## What the job does

**Validation.** Every event is parsed and checked before it reaches the business logic. Events with malformed JSON, missing `eventId` / `orderId` / `userId` / `currency` / `eventTime`, a non-numeric or non-positive `amount`, or an unknown `eventType` are not dropped. They are sent to the `invalid-orders` topic with the original payload, a machine-readable reason (`malformed-json`, `non-positive-amount`, `unknown-eventType` ...) and a timestamp, so they can be inspected and replayed later.

**Deduplication.** Producers retry, so the same event can arrive twice. The stream is keyed by `eventId`, and a `ValueState<Boolean>` remembers which ids were already seen. Repeats go to the dead-letter topic as `duplicate-event`. The state has a 1-hour TTL, so it does not grow forever.

**Event time and watermarks.** Timestamps come from the order's own `eventTime`, not from the time Flink happens to process it. A bounded out-of-orderness watermark allows events to arrive up to 10 seconds late and still land in the correct window.

**GMV windows.** Only `payment-completed` events count as revenue. They are keyed by currency and aggregated in 1-minute tumbling event-time windows with an incremental `AggregateFunction`, so the window keeps a running sum and count instead of buffering every event. Each closed window is written to `gmv-by-minute`, for example:

```json
{"windowStart":"2026-05-09T12:00:00Z","windowEnd":"2026-05-09T12:01:00Z","currency":"TRY","totalGmv":4821.4,"orderCount":5,"updatedAt":"..."}
```

**Fault tolerance.** Checkpointing runs every 10 seconds in exactly-once mode, so the dedup state, open windows and Kafka offsets are restored together after a failure.

## Topics

| Topic | Content |
|---|---|
| `orders` | Raw order events (input) |
| `enriched-orders` | Valid, unique events with a `processedAt` field |
| `invalid-orders` | Dead-letter topic: bad and duplicate events with a reason |
| `gmv-by-minute` | GMV and order count per currency, per minute |

## Test data

`scripts/generate_orders.py` produces a continuous stream of realistic orders and deliberately mixes in problems: negative amounts, missing currency, unknown event types, broken JSON and exact duplicates. Generation stays in Python, publishing stays in Java (`JsonLinesKafkaProducer`); `scripts/run_orders_pipeline.sh` pipes one into the other.

`samples/orders.jsonl` holds a small fixed set of events for quick manual checks.

## Running locally

Only Docker is needed; Java 17 and Maven run in containers.

```bash
# 1. Start Kafka, Flink (JobManager + TaskManager) and Kafka UI
docker compose up -d

# 2. Build the job
docker run --rm -v "$PWD":/workspace -w /workspace maven:3.9.9-eclipse-temurin-17 mvn clean package

# 3. Create the topics (once)
for t in orders enriched-orders invalid-orders gmv-by-minute; do
  docker compose exec kafka kafka-topics --bootstrap-server kafka:29092 \
    --create --if-not-exists --topic "$t" --partitions 3 --replication-factor 1
done

# 4. Submit the job
docker compose exec jobmanager flink run /opt/flink/usrlib/flink-kafka-sandbox.jar

# 5. Send data
bash scripts/run_orders_pipeline.sh          # continuous generator
# or a fixed sample:
cat samples/orders.jsonl | docker compose exec -T kafka \
  kafka-console-producer --bootstrap-server kafka:29092 --topic orders

# 6. Watch the results
docker compose exec kafka kafka-console-consumer --bootstrap-server kafka:29092 \
  --topic gmv-by-minute --from-beginning
```

Flink UI: http://localhost:8081 (checkpoints, watermarks, operator metrics)
Kafka UI: http://localhost:8080

Kafka data is kept in a named volume; `docker compose down -v` removes it.

## Code layout

```text
OrderStreamingJob.java       wires the pipeline: source → validate → dedup → sinks and windows
OrderValidator.java          validation rules and reasons
InvalidOrderRouter.java      splits valid events from the dead-letter side output
DeduplicateByEventId.java    keyed ValueState with TTL
OrderEventTime.java          eventTime extraction and watermark strategy
GmvAggregate.java            incremental sum and count inside a window
AddGmvWindowMetadata.java    turns a closed window into the output record
KafkaTopics.java             Kafka source and sink builders
JobConfig.java               topic names, lateness, dedup TTL
```

## Stack

Java 17 · Apache Flink 1.20 (DataStream API) · Apache Kafka (Confluent 7.6) · Jackson · Python · Docker Compose

## Next steps

- Switch the Kafka sinks to transactional `EXACTLY_ONCE` delivery so outputs match the exactly-once checkpoints end to end.
- Handle events that arrive after the watermark (allowed lateness or a late-data side output) instead of dropping them.
- Run with parallelism above 1 and match it to the topic partitions.
- Add unit tests for the validator and window logic with Flink's test harness.
- Load the GMV stream into ClickHouse for dashboards.
