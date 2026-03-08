package com.spb.common.config;

import java.io.Serializable;

/**
 * AppConfig 全局配置类
 * -------------------
 * 存放系统所有的外部依赖连接信息和核心参数。
 * ⚠️ 注意：实现了 Serializable 接口。因为 Flink 是分布式运行的，
 * JobManager 需要将这些配置序列化后通过网络发送给各个 TaskManager 节点。
 */
public class AppConfig implements Serializable {

    // ==================== Kafka 相关配置 ====================
    // Kafka Broker 的地址，也就是你刚才 Python 写入的地址
    public static final String KAFKA_BOOTSTRAP_SERVERS = "localhost:9092";
    // 微博热搜的数据主题
    public static final String KAFKA_TOPIC = "weibo.hotsearch";
    // 消费者组 ID。Flink 依靠这个 ID 记录自己消费到了哪一条数据 (Offset)
    public static final String KAFKA_GROUP_ID = "flink-weibo-consumer";

    // ==================== MySQL 相关配置 (Flink Sink) ====================
    // MySQL 连接地址，注意加上时区和字符集防止乱码
    public static final String MYSQL_URL = "jdbc:mysql://localhost:3306/social_platforms_analysis?useSSL=false&serverTimezone=Asia/Shanghai&characterEncoding=utf8";
    public static final String MYSQL_USERNAME = "root";
    // TODO: 请将这里的密码替换为你本地 MySQL 的实际密码
    public static final String MYSQL_PASSWORD = "";
    public static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";

    // ==================== Flink 业务计算配置 ====================
    // 滚动窗口大小：默认 60000 毫秒（1分钟），即每分钟输出一次该分钟内的热点统计
    public static final long WINDOW_SIZE_MS = 60000L;
}