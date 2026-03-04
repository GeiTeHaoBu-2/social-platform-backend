package com.withpy.socialplatformback.web;

import com.withpy.socialplatformback.flink.job.WeiboHotSearchJob;
import com.withpy.socialplatformback.kafka.KafkaAdminHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * SocialPlatformApplication
 * -------------------------
 * Spring Boot 应用主入口，同时负责启动 Flink 作业。
 *
 * 启动顺序：
 *   1. Spring Boot 容器初始化（Web / MyBatis / Security 等）
 *   2. 等待 Kafka topic 就绪（KafkaAdminHelper.waitForTopic）
 *   3. 在独立线程中启动 Flink 流处理作业（WeiboHotSearchJob.run）
 *
 * 配置项（可通过 application.properties 或 -D 系统属性覆盖）：
 *   kafka.bootstrap.servers  默认 localhost:9092
 *   kafka.topic              默认 weibo_hotsearch
 *   kafka.group.id           默认 weibo-hotsearch-group
 *   analysis.window.ms       默认 60000（1 分钟滚动窗口）
 */
@SpringBootApplication(scanBasePackages = "com.withpy.socialplatformback")
public class SocialPlatformApplication {

    private static final Logger log = LoggerFactory.getLogger(SocialPlatformApplication.class);

    public static void main(String[] args) throws Exception {
        // 1. 启动 Spring Boot 容器
        SpringApplication.run(SocialPlatformApplication.class, args);

        // 2. 读取配置
        String bootstrapServers = System.getProperty("kafka.bootstrap.servers", "localhost:9092");
        String topic            = System.getProperty("kafka.topic",              "weibo_hotsearch");
        String groupId          = System.getProperty("kafka.group.id",           "weibo-hotsearch-group");
        long   windowMs         = Long.parseLong(System.getProperty("analysis.window.ms", "60000"));

        log.info("Kafka config → servers={}, topic={}, groupId={}", bootstrapServers, topic, groupId);

        // 3. 等待 Kafka topic 就绪
        boolean ready = KafkaAdminHelper.waitForTopic(bootstrapServers, topic, 5, 2000);
        if (!ready) {
            log.error("Kafka topic '{}' is not ready after retries. Flink job will NOT start.", topic);
            return;
        }

        // 4. 在独立线程中启动 Flink 作业（避免阻塞 Spring Boot 主线程）
        Thread flinkThread = new Thread(() -> {
            try {
                WeiboHotSearchJob.run(bootstrapServers, topic, groupId, windowMs);
            } catch (Exception e) {
                log.error("Flink job terminated with exception", e);
            }
        }, "flink-job-thread");
        flinkThread.setDaemon(false);
        flinkThread.start();

        log.info("Flink job thread started.");
    }
}
