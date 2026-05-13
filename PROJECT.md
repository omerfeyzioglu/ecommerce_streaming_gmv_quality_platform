
# Flink Learning Sandbox

This project is not primarily about finishing a big platform quickly.

The project is a learning tool. The current goal is to learn Apache Flink step by step, starting from the smallest useful concepts:

1. What a Flink job is
2. What a source is
3. What a `DataStream` is
4. What `map` does
5. What `filter` does
6. What `keyBy` does
7. What state and windows do

## Learning Contract

For each learning step:

1. Explain the concept briefly.
2. Add only a small skeleton or TODO.
3. Leave the important logic for me to implement.
4. Tell me exactly what I should change.
5. Run or describe a small verification.
6. Review the result before moving to the next concept.

## Current Lesson

Lesson 1 is:

```text
Flink job anatomy + map
```

Current data flow:

```text
Kafka topic: orders
    -> Flink KafkaSource
    -> DataStream<String>
    -> map transformation
    -> Flink KafkaSink
    -> Kafka topic: enriched-orders
```

My task:

In `OrderStreamingJob.java`, find the `TODO(you)` inside `enrichForLessonOne` and add one extra JSON field next to `processedAt`.

Suggested field:

```java
order.put("learningStep", "lesson-1-map");
```

After that, rebuild and verify that `enriched-orders` contains both `processedAt` and `learningStep`.

---

# Later Roadmap: ecommerce-streaming-gmv-quality-platform

The goal is NOT to generate the entire project blindly. I want to learn while building it. So do not produce a full finished solution at once. Work with me module by module. For each module:
1. Explain the purpose briefly.
2. Generate a clean skeleton.
3. Leave critical business/data engineering logic as TODOs.
4. Tell me exactly which parts I should implement myself.
5. Add small tests or test skeletons.
6. Explain how I can verify that the module works.

Project goal:
Build a production-like mini e-commerce streaming data platform.

High-level architecture:

Synthetic Order Event Generator
→ Kafka topic: order_events
→ Flink Java streaming job
→ ClickHouse serving tables
→ FastAPI metric endpoints

Side path:
Kafka order_events
→ Delta Lake raw archive on MinIO/S3
→ Airflow reconciliation/backfill checks
→ optional ClickHouse correction/backfill

Main learning goals:
- Kafka event streaming
- Java Flink stream processing
- event-time processing
- watermark
- duplicate event handling
- invalid event handling
- GMV aggregation
- ClickHouse OLAP table design
- FastAPI API serving
- Delta Lake raw archive
- Airflow reconciliation/backfill
- JUnit tests
- pytest API tests
- GitHub Actions CI
- Jenkinsfile basics
- Docker Compose local environment

Tech stack:
- Python 3.11
- Faker
- Kafka
- Java 17
- Apache Flink
- Maven
- ClickHouse
- FastAPI
- Delta Lake
- PySpark if needed for Delta raw writer
- MinIO as S3-compatible storage
- Apache Airflow
- GitHub Actions
- Jenkinsfile
- JUnit
- pytest
- Docker Compose

Important design decisions:
- Flink is used for real-time/near-real-time stream processing.
- ClickHouse is used as the fast OLAP serving layer for API/dashboard queries.
- Delta Lake is used as raw archive for replay, audit, historical analysis and backfill.
- Airflow is used for scheduled reconciliation and backfill orchestration, not for real-time stream processing.
- FastAPI exposes processed metrics from ClickHouse.
- The event generator must simulate real streaming problems such as duplicates, late events and invalid records.

Project structure:

ecommerce-streaming-gmv-quality-platform/
  event-generator/
    producer.py
    config.yaml
    requirements.txt

  flink-job/
    pom.xml
    src/main/java/...
    src/test/java/...

  clickhouse/
    init.sql

  api/
    main.py
    requirements.txt
    tests/

  delta-raw-writer/
    raw_writer.py
    requirements.txt

  airflow/
    dags/
      reconcile_daily_orders.py

  .github/
    workflows/
      ci.yml

  Jenkinsfile
  docker-compose.yml
  README.md

Module 1: Event Generator

Build a Python Kafka producer skeleton using Faker.

Event schema:
- event_id
- order_id
- customer_id
- country
- category
- amount
- currency
- payment_status
- event_time
- produced_at

Configurable values:
- events_per_second
- duplicate_rate
- late_event_rate
- invalid_event_rate
- countries
- categories

Generate these scenarios:
- valid paid order
- duplicate event_id
- late event where event_time is older than produced_at
- invalid event with negative amount
- invalid event with missing country
- invalid event with invalid payment_status

Do not fully implement all bad-event logic for me. Create clean TODOs so I can implement them.

Module 2: ClickHouse Schema

Create ClickHouse DDL skeletons for:

1. gmv_by_country
- window_start
- window_end
- country
- total_gmv
- order_count
- updated_at

2. gmv_by_category
- window_start
- window_end
- country
- category
- total_gmv
- order_count
- updated_at

3. data_quality_metrics
- window_start
- window_end
- processed_event_count
- invalid_event_count
- duplicate_event_count
- late_event_count
- updated_at

4. invalid_events
- event_id
- reason
- raw_payload
- created_at

Use ClickHouse MergeTree family tables.
Explain briefly why ORDER BY matters.
Leave comments where I should think about partitioning or ordering.

Module 3: FastAPI

Build a FastAPI skeleton that connects to ClickHouse.

Endpoints:
- GET /health
- GET /gmv/country?country=TR
- GET /gmv/category?country=TR&category=electronics
- GET /quality/summary

Do not complete all SQL queries. Put TODOs where I should write SQL myself.
Add pytest skeletons for these endpoints.

Module 4: Flink Java Job

Create a Maven-based Flink Java project skeleton.

The Flink job should:
- consume JSON events from Kafka topic order_events
- parse JSON into OrderEvent class
- validate required fields
- detect invalid records
- deduplicate by event_id
- use event_time as event time
- assign watermarks
- compute 5-minute window GMV by country
- compute 5-minute window GMV by category
- compute data quality metrics
- write results to ClickHouse

Important:
Do not implement the full final logic immediately.
First create:
- OrderEvent class
- basic Kafka source skeleton
- parsing function skeleton
- validation function skeleton
- aggregation TODOs
- ClickHouse sink TODOs
- JUnit test skeletons

I want to implement the critical logic myself.

Module 5: Delta Lake Raw Archive

Create a PySpark raw writer skeleton:
Kafka order_events
→ read raw JSON
→ add ingestion_time
→ write to Delta Lake path on MinIO/S3

Leave the exact S3/MinIO configuration as TODO if needed.
Explain how Delta is used here as raw archive, not serving layer.

Module 6: Airflow Reconciliation

Create an Airflow DAG skeleton named reconcile_daily_orders.

Tasks:
- check_clickhouse_freshness
- check_processed_event_count
- check_invalid_event_ratio
- compare_delta_raw_count_vs_clickhouse_processed_count
- optional_backfill_last_hour

Do not implement all queries fully.
Put TODOs for the SQL and thresholds.
Explain what each task is supposed to validate.

Module 7: CI/CD

Create:
- GitHub Actions workflow skeleton
- Jenkinsfile skeleton

CI should eventually:
- run Maven/JUnit tests
- run pytest
- optionally build Docker images

Leave Docker image build details as TODO if needed.

Module 8: Docker Compose

Create a Docker Compose skeleton for:
- Kafka
- Zookeeper or Redpanda
- ClickHouse
- Flink JobManager
- Flink TaskManager
- FastAPI
- event-generator
- MinIO
- Airflow webserver/scheduler if feasible

Prioritize local development.
If Airflow/MinIO/Flink integration becomes too heavy, keep placeholders and document the TODOs clearly.

README requirements:

Create a README skeleton with these sections:
- Project goal
- Architecture
- Tech stack
- Data flow
- Why Flink?
- Why ClickHouse?
- Why Delta Lake?
- Why Airflow?
- Data quality scenarios
- API examples
- Testing strategy
- CI/CD
- How to run locally
- What I learned
- Production improvements

Very important:
After each module, ask me before continuing.
Do not generate all modules at once.
Act like a mentor, not like an autopilot code generator.
