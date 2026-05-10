#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python3}"
JAVA_BIN="${JAVA_BIN:-java}"
BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${KAFKA_TOPIC:-orders}"
JAR_PATH="${JAR_PATH:-$ROOT_DIR/target/flink-kafka-sandbox.jar}"
PYTHON_SCRIPT="${PYTHON_SCRIPT:-$ROOT_DIR/scripts/generate_orders.py}"

if [[ ! -f "$JAR_PATH" ]]; then
  cat <<EOF
Missing jar: $JAR_PATH

Build it once with:
  docker run --rm -v "$ROOT_DIR":/workspace -w /workspace maven:3.9.9-eclipse-temurin-17 mvn clean package
EOF
  exit 1
fi

exec "$PYTHON_BIN" -u "$PYTHON_SCRIPT" | \
  "$JAVA_BIN" -cp "$JAR_PATH" \
  com.example.flinkkafkasandbox.JsonLinesKafkaProducer "$TOPIC" "$BOOTSTRAP_SERVERS"