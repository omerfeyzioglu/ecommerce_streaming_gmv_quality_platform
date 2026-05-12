package com.example.flinkkafkasandbox;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public final class KafkaTopics {
    private KafkaTopics() {
    }

    public static KafkaSource<String> ordersSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(JobConfig.BOOTSTRAP_SERVERS)
                .setTopics(JobConfig.ORDERS_TOPIC)
                .setGroupId(JobConfig.CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public static KafkaSink<String> stringSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(JobConfig.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }
}
