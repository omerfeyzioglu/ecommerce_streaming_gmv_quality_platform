# Kafka to Flink Sandbox

Simple Java project for learning the Flink DataStream API with Kafka.

Flow:

```text
orders Kafka topic
    -> Flink job
    -> enriched-orders Kafka topic
```

The Flink job reads each order as JSON, adds a `processedAt` field, and writes the result back to Kafka.

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

## Read The Output

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic enriched-orders \
  --from-beginning
```

You should see the original order JSON plus a new `processedAt` field.

## Code To Study

Open:

```text
src/main/java/com/example/flinkkafkasandbox/OrderStreamingJob.java
```

Focus on:

- `StreamExecutionEnvironment`: starts the Flink job definition.
- `KafkaSource`: reads records from Kafka.
- `map`: transforms each record.
- `KafkaSink`: writes records back to Kafka.

For now, topic names and parallelism are fixed inside the Java file to keep the first version easy to read.

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
