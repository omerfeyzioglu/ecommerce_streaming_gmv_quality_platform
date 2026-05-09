package com.example.flinkkafkasandbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Instant;

public class OrderStreamingJob {
    private static final String BOOTSTRAP_SERVERS = "kafka:29092";
    private static final String INPUT_TOPIC = "orders";
    private static final String OUTPUT_TOPIC = "enriched-orders";
    private static final String CONSUMER_GROUP_ID = "flink-kafka-sandbox";

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId(CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> orders = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Read orders from Kafka"
        );

        orders
                .map(OrderStreamingJob::addProcessedAt)
                .name("Add processedAt field")
                // 4. Sink: Sonucu enriched-orders topic'ine yaz.
                .sinkTo(kafkaSink())
                .name("Write enriched orders to Kafka");

        env.execute("Simple Kafka to Flink Job");
    }

    private static String addProcessedAt(String rawJson) throws Exception {
        ObjectNode order = (ObjectNode) JSON.readTree(rawJson);
        order.put("processedAt", Instant.now().toString());
        return JSON.writeValueAsString(order);
    }

    private static KafkaSink<String> kafkaSink() {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(OUTPUT_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }
}
