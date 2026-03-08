package com.spb.flink.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * KafkaSourceFactory
 * - 将 KafkaSource 的创建封装为一个工厂方法，便于在不同地方复用并进行单元测试。
 */
public final class KafkaSourceFactory {

    private KafkaSourceFactory() {}

    public static KafkaSource<String> create(String bootstrapServers, String topic, String groupId) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}

