package com.spb.web;

import com.spb.flink.job.WeiboHotSearchJob;
import com.spb.flink.source.KafkaAdminHelper;
import com.spb.common.config.KafkaConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;

/**
 * SocialPlatformApplication
 * -------------------------
 * Spring Boot 应用主入口，同时负责启动 Flink 作业。
 *
 * 修改内容：
 * 1. 修复静态上下文中无法引用非静态变量的问题。
 * 2. 使用 Spring 的 ApplicationContext 获取 KafkaConfig Bean。
 */
@SpringBootApplication(scanBasePackages = "com.withpy.socialplatformback")
public class SocialPlatformApplication {

    private static final Logger log = LoggerFactory.getLogger(SocialPlatformApplication.class);

    public static void main(String[] args) throws Exception {
        // 1. 启动 Spring Boot 容器
        ApplicationContext context = SpringApplication.run(SocialPlatformApplication.class, args);

        // 2. 获取 KafkaConfig Bean
        KafkaConfig kafkaConfig = context.getBean(KafkaConfig.class);

        // 3. 读取配置
        String bootstrapServers = kafkaConfig.getBootstrapServers();
        String topic = kafkaConfig.getTopic();
        String groupId = kafkaConfig.getGroupId();
        long windowMs = Long.parseLong(System.getProperty("analysis.window.ms", "60000"));

        log.info("Kafka config → servers={}, topic={}, groupId={}", bootstrapServers, topic, groupId);

        // 4. 等待 Kafka topic 就绪
        boolean ready = KafkaAdminHelper.waitForTopic(bootstrapServers, topic, 5, 2000);
        if (!ready) {
            log.error("Kafka topic '{}' is not ready after retries. Flink job will NOT start.", topic);
            return;
        }

        // 5. 在独立线程中启动 Flink 作业（避免阻塞 Spring Boot 主线程）
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
