package com.example.flinkkafkasandbox;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public final class JsonLinesKafkaProducer {
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_TOPIC = "orders";

    private JsonLinesKafkaProducer() {
    }

    public static void main(String[] args) throws IOException {
        String topic = args.length > 0 ? args[0] : DEFAULT_TOPIC;
        String bootstrapServers = args.length > 1 ? args[1] : DEFAULT_BOOTSTRAP_SERVERS;

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("enable.idempotence", "true");
        props.put("compression.type", "lz4");
        props.put("retries", "5");
        props.put("delivery.timeout.ms", "120000");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props);
             BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String payload = line.trim();
                if (payload.isEmpty()) {
                    continue;
                }

                producer.send(
                        new ProducerRecord<>(topic, payload),
                        new LoggingCallback(topic, payload)
                );
            }

            producer.flush();
        }
    }

    private static final class LoggingCallback implements Callback {
        private final String topic;
        private final String payload;

        private LoggingCallback(String topic, String payload) {
            this.topic = topic;
            this.payload = payload;
        }

        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception != null) {
                System.err.println("delivery failed: " + exception.getMessage() + " | payload=" + payload);
                return;
            }

            System.out.println(
                    "sent to " + topic + "[" + metadata.partition() + "] offset=" + metadata.offset()
            );
        }
    }
}