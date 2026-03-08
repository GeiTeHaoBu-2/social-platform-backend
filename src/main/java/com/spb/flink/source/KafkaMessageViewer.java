package com.spb.flink.source;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class KafkaMessageViewer {
    public static void main(String[] args) {
        // 配置 Kafka 消费者
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092"); // Kafka broker 地址
        props.put("group.id", "test-group"); // 消费者组 ID
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅主题
        consumer.subscribe(Collections.singletonList("weibo.hotsearch")); // 替换为你的主题名称

        // 消费消息
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("offset = %d, key = %s, value = %s%n", 
                        record.offset(), record.key(), record.value());
                }
            }
        } finally {
            consumer.close();
        }
    }
}