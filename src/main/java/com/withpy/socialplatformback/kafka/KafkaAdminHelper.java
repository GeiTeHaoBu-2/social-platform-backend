package com.withpy.socialplatformback.kafka;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DescribeTopicsResult;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * KafkaAdminHelper
 * - 封装与 Kafka AdminClient 交互的逻辑，例如等待 topic 准备好（存在且有分区）
 */
public final class KafkaAdminHelper {

    private static final Logger log = LoggerFactory.getLogger(KafkaAdminHelper.class);

    private KafkaAdminHelper() {}

    /**
     * 等待指定 topic 在 Kafka 中准备就绪（存在且有分区）。
     *
     * @param bootstrapServers Kafka bootstrap.servers
     * @param topic            期待的 topic 名称
     * @param maxRetries       最大重试次数
     * @param backoffMs        每次重试的回退时间（毫秒）
     * @return 如果准备就绪返回 true，否则返回 false
     */
    public static boolean waitForTopic(String bootstrapServers, String topic, int maxRetries, long backoffMs) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient admin = AdminClient.create(adminProps)) {
            try {
                Collection<Node> nodes = admin.describeCluster().nodes().get(5, TimeUnit.SECONDS);
                if (nodes == null || nodes.isEmpty()) {
                    log.error("No Kafka brokers found at {}. Please ensure Kafka is running and reachable.", bootstrapServers);
                    return false;
                }
                log.info("Kafka cluster nodes: {}", nodes);

                for (int attempt = 1; attempt <= maxRetries; attempt++) {
                    try {
                        DescribeTopicsResult tr = admin.describeTopics(java.util.Collections.singletonList(topic));
                        TopicDescription desc = tr.topicNameValues().get(topic).get(5, TimeUnit.SECONDS);
                        if (desc.partitions().isEmpty()) {
                            log.warn("Kafka topic '{}' found but has no partitions (attempt {}/{}).", topic, attempt, maxRetries);
                        } else {
                            log.info("Topic '{}' found with {} partitions", topic, desc.partitions().size());
                            return true;
                        }
                    } catch (Exception e) {
                        log.warn("Attempt {}/{}: failed to describe topic '{}': {}.", attempt, maxRetries, topic, e.toString());
                    }

                    Thread.sleep(backoffMs);
                }

                log.error("Topic '{}' wasn't ready after {} attempts", topic, maxRetries);
                return false;
            } catch (Exception e) {
                log.error("Failed to connect to Kafka at {} or to describe topic '{}'. Cause: {}", bootstrapServers, topic, e.toString());
                log.debug("AdminClient exception", e);
                return false;
            }
        }
    }
}

