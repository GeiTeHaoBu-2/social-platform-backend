package com.spb.flink.sink;

import com.spb.common.config.AppConfig;
import com.spb.common.model.ProcessedHotSearch;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * SinkFactory
 * ---------
 * 数据输出工厂类。
 * 职责：封装所有 Flink Sink 的创建逻辑（纯构造，无业务逻辑）。
 * 遵循 "调度与执行分离" 原则，本类只负责创建 Sink 组件，不做数据转换。
 */
public final class SinkFactory {

    // MySQL UPSERT SQL：插入或更新语义
    // 当 id 重复时，更新排名、热度、情感分数、标签和时间戳
    private static final String UPSERT_SQL =
            "INSERT INTO processed_weibo_hot_search " +
                    "(id, rank_num, title, hot_count, sentiment_score, sentiment_label, event_time) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                    "ON DUPLICATE KEY UPDATE " +
                    "rank_num=VALUES(rank_num), " +
                    "hot_count=VALUES(hot_count), " +
                    "sentiment_score=VALUES(sentiment_score), " +
                    "sentiment_label=VALUES(sentiment_label), " +
                    "event_time=VALUES(event_time)";

    // 默认批处理配置
    private static final int DEFAULT_BATCH_SIZE = 50;
    private static final int DEFAULT_BATCH_INTERVAL_MS = 2000;
    private static final int DEFAULT_MAX_RETRIES = 3;

    private SinkFactory() {}

    /**
     * 创建 MySQL JDBC Sink（使用默认批处理配置）。
     * 这是第6步的抽象方法实现。
     *
     * 批处理策略：
     * - 每累积 50 条数据执行一次批量写入
     * - 或每隔 2 秒执行一次写入（以先到者为准）
     * - 失败时最多重试 3 次
     *
     * @return ProcessedHotSearch 的 JDBC Sink 函数
     */
    public static SinkFunction<ProcessedHotSearch> createMySqlSink() {
        return createMySqlSink(
                DEFAULT_BATCH_SIZE,
                DEFAULT_BATCH_INTERVAL_MS,
                DEFAULT_MAX_RETRIES
        );
    }

    /**
     * 创建 MySQL JDBC Sink（支持自定义批处理配置）。
     *
     * @param batchSize        批处理大小
     * @param batchIntervalMs  批处理间隔（毫秒）
     * @param maxRetries       最大重试次数
     * @return ProcessedHotSearch 的 JDBC Sink 函数
     */
    public static SinkFunction<ProcessedHotSearch> createMySqlSink(
            int batchSize,
            int batchIntervalMs,
            int maxRetries) {

        return JdbcSink.sink(
                UPSERT_SQL,
                (statement, processedHotSearch) -> {
                    // 将 ProcessedHotSearch 对象的属性映射到 SQL 的 '?' 占位符
                    statement.setString(1, processedHotSearch.getId());
                    statement.setInt(2, processedHotSearch.getRank());
                    statement.setString(3, processedHotSearch.getTitle());
                    statement.setLong(4, processedHotSearch.getHotCount());
                    statement.setInt(5, processedHotSearch.getSentimentScore());
                    statement.setString(6, processedHotSearch.getSentimentLabel());
                    statement.setLong(7, processedHotSearch.getEventTime());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(batchSize)
                        .withBatchIntervalMs(batchIntervalMs)
                        .withMaxRetries(maxRetries)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(AppConfig.MYSQL_URL)
                        .withDriverName(AppConfig.MYSQL_DRIVER)
                        .withUsername(AppConfig.MYSQL_USERNAME)
                        .withPassword(AppConfig.MYSQL_PASSWORD)
                        .build()
        );
    }

    /**
     * 创建带有自定义名称的 MySQL JDBC Sink（便于监控和日志追踪）。
     *
     * @param name Sink 组件名称
     * @return 命名的 Sink 函数包装
     */
    public static SinkFunction<ProcessedHotSearch> createMySqlSink(String name) {
        SinkFunction<ProcessedHotSearch> sink = createMySqlSink();
        // 返回带名称的包装（Flink 会通过 uid/name 追踪）
        return new NamedSinkFunction<>(sink, name);
    }

    /**
     * 内部类：为 SinkFunction 提供命名包装。
     * 用于在 Flink UI 中更好地识别组件。
     */
    private static class NamedSinkFunction<T> implements SinkFunction<T> {
        private final SinkFunction<T> delegate;
        private final String name;

        NamedSinkFunction(SinkFunction<T> delegate, String name) {
            this.delegate = delegate;
            this.name = name;
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            delegate.invoke(value, context);
        }

        public String getName() {
            return name;
        }
    }
}
