package com.spb.flink.source;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.Node;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * KafkaTopicInspector
 * <p>
 * 轻量级 Kafka topic 检查工具（简化版）：
 * - 只显示集群信息与指定 topic 的分区/leader/replicas/isr（不打印 topic configs）
 * - 默认进入 watch 模式（每 5000ms 刷新），也可通过命令行参数改变
 * - 代码结构被拆分为若干小方法，便于阅读与单元测试
 */
public final class KafkaTopicInspector {

    private static final Logger log = LoggerFactory.getLogger(KafkaTopicInspector.class);

    private KafkaTopicInspector() {}

    /**
     * 获取指定 topic 的简洁信息（只包含集群信息和 topic 分区详情）
     */
    public static String describeTopicInfo(String bootstrapServers, String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        StringBuilder sb = new StringBuilder();

        try (AdminClient admin = AdminClient.create(props)) {
            // 集群信息
            appendClusterInfo(admin, sb);

            // topic 分区信息
            appendTopicDescription(admin, topic, sb);

            return sb.toString();
        } catch (Exception e) {
            String msg = "Failed to create AdminClient or other error: " + e.toString();
            log.error(msg, e);
            return msg;
        }
    }

    // 将集群信息写入 StringBuilder（broker 列表 + controller）
    private static void appendClusterInfo(AdminClient admin, StringBuilder sb) {
        sb.append("Cluster info:\n");
        try {
            Collection<Node> nodes = admin.describeCluster().nodes().get(5, TimeUnit.SECONDS);
            Node controller = admin.describeCluster().controller().get(5, TimeUnit.SECONDS);
            sb.append("Brokers: ");
            if (nodes != null && !nodes.isEmpty()) {
                for (Node n : nodes) {
                    sb.append(String.format("%s:%d (id=%d) ", n.host(), n.port(), n.id()));
                }
            } else {
                sb.append("<none>");
            }
            sb.append(System.lineSeparator());
            sb.append("Controller: ");
            sb.append(controller != null ? controller.toString() : "<none>");
            sb.append(System.lineSeparator()).append(System.lineSeparator());
        } catch (Exception e) {
            sb.append("Failed to describe cluster: ").append(e.toString()).append(System.lineSeparator()).append(System.lineSeparator());
        }
    }

    // 将 topic 的分区详情写入 StringBuilder（不包含 configs）
    private static void appendTopicDescription(AdminClient admin, String topic, StringBuilder sb) {
        sb.append("Topic description:\n");
        try {
            DescribeTopicsResult tr = admin.describeTopics(Collections.singletonList(topic));
            TopicDescription desc = tr.topicNameValues().get(topic).get(5, TimeUnit.SECONDS);
            sb.append(String.format("Name: %s\n", desc.name()));
            sb.append(String.format("Internal: %s\n", desc.isInternal()));
            sb.append(String.format("Partitions: %d\n\n", desc.partitions().size()));

            sb.append("Partitions detail:\n");
            desc.partitions().forEach(p -> {
                sb.append(String.format("Partition %d: leader=%s, replicas=%s, isr=%s\n",
                        p.partition(),
                        nodeToShortStr(p.leader()),
                        nodesToShortStr(p.replicas()),
                        nodesToShortStr(p.isr())));
            });
            sb.append(System.lineSeparator());
        } catch (Exception e) {
            sb.append("Failed to describe topic: ").append(e.toString()).append(System.lineSeparator()).append(System.lineSeparator());
        }
    }

    private static String nodeToShortStr(Node n) {
        if (n == null) return "<none>";
        return String.format("%s:%d(id=%d)", n.host(), n.port(), n.id());
    }

    private static String nodesToShortStr(List<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) return "<none>";
        StringBuilder sb = new StringBuilder();
        for (Node n : nodes) {
            if (sb.length() > 0) sb.append("|");
            sb.append(nodeToShortStr(n));
        }
        return sb.toString();
    }

    private static String nodesToShortStr(Collection<Node> nodes) {
        if (nodes == null || nodes.isEmpty()) return "<none>";
        StringBuilder sb = new StringBuilder();
        for (Node n : nodes) {
            if (sb.length() > 0) sb.append("|");
            sb.append(nodeToShortStr(n));
        }
        return sb.toString();
    }

    /**
     * 命令行运行用法：
     * java -Dkafka.bootstrap.servers=localhost:9092 -cp yourjar com.withpy.socialplatformback.kafka.KafkaTopicInspector [topic] [--watch <ms>]
     * 示例：
     *  - 单次查看（也支持）： java -Dkafka.bootstrap.servers=localhost:9092 -cp app.jar com.withpy.socialplatformback.kafka.KafkaTopicInspector weibo_hotsearch
     *  - 默认（不传参数）进入 watch 模式，每 5000ms 刷新一次：
     *      java -Dkafka.bootstrap.servers=localhost:9092 -cp app.jar com.withpy.socialplatformback.kafka.KafkaTopicInspector
     */
    public static void main(String[] args) {
        String bootstrap = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "weibo_hotsearch");

        // 默认 watch 模式，每 5000ms 刷新；如果传入 --once 则只执行一次
        boolean watch = true;
        long defaultIntervalMs = 5000L;
        long watchIntervalMs = defaultIntervalMs;

        if (args != null && args.length > 0) {
            int i = 0;
            if (!args[0].startsWith("-")) {
                topic = args[0];
                i = 1;
            }
            for (; i < args.length; i++) {
                String a = args[i];
                if ("--watch".equals(a) || "-w".equals(a)) {
                    if (i + 1 < args.length) {
                        try {
                            watchIntervalMs = Long.parseLong(args[i + 1]);
                            watch = true;
                            i++; // skip next
                        } catch (NumberFormatException nfe) {
                            System.err.println("Invalid interval for --watch: " + args[i + 1]);
                            return;
                        }
                    } else {
                        System.err.println("--watch requires an interval in milliseconds");
                        return;
                    }
                } else if ("--once".equals(a)) {
                    watch = false;
                } else if ("--help".equals(a) || "-h".equals(a)) {
                    System.out.println("Usage: KafkaTopicInspector [topic] [--watch <ms>] [--once]\n  bootstrap: -Dkafka.bootstrap.servers=<host:port>");
                    return;
                }
            }
        }

        System.out.println("Inspecting topic '" + topic + "' on " + bootstrap);

        do {
            String info = describeTopicInfo(bootstrap, topic);
            System.out.println(info);

            if (watch) {
                try {
                    Thread.sleep(watchIntervalMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    System.out.println("Interrupted, exiting watch mode.");
                    break;
                }
                System.out.println("----- refreshing -----\n");
            }
        } while (watch);
    }
}

