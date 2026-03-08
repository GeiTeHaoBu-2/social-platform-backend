package com.spb.flink.job;

public final class WeiboHotSearchJob {
    // 独立的 main 方法，Flink 程序直接从这里启动！
    public static void main(String[] args) throws Exception {
        // TODO: 这里可以通过 args 获取配置，或者直接写死
        String bootstrapServers = "localhost:9092";
        String topic = "weibo.hotsearch";
        String groupId = "flink-consumer-group";
        long windowMs = 60000;

        run(bootstrapServers, topic, groupId, windowMs);
    }

    public static void run(...) throws Exception {
        // ... 原来的 Flink 算子逻辑保持不变 ...
    }
}