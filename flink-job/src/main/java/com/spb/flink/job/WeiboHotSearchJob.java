package com.spb.flink.job;

import com.spb.common.config.AppConfig;
import com.spb.common.model.HotSearchItem;
import com.spb.common.model.ProcessedHotSearch;
import com.spb.flink.pipeline.SentimentAnalysisPipeline;
import com.spb.flink.source.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * WeiboHotSearchJob
 * -----------------
 * Flink 实时计算程序的唯一入口类。
 * 职责：组装 Source(数据源) -> Transformation(转换处理) -> Sink(输出目标)
 */
public final class WeiboHotSearchJob {

    public static void main(String[] args) throws Exception {
        // 1. 初始化 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 构建 Kafka 数据源 (利用我们之前封装好的工厂类和 AppConfig 配置)
        KafkaSource<String> kafkaSource = KafkaSourceFactory.create(
                AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                AppConfig.KAFKA_TOPIC,
                AppConfig.KAFKA_GROUP_ID
        );

        // 3. 从 Kafka 摄取原始 JSON 字符串流
        DataStream<String> rawJsonStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(), // 当前版本暂不处理乱序事件，不加水位线
                "Weibo HotSearch Kafka Source"
        );
        rawJsonStream.print("【Kafka原始数据】");

        // 4. 将 JSON 字符串解析为 HotSearchItem 实体流 (复用你写好的 Pipeline)
        DataStream<HotSearchItem> itemStream = SentimentAnalysisPipeline.parseJsonStream(rawJsonStream);

        // 5. 核心计算：将原始数据映射为附带情感分析结果的 ProcessedHotSearch 实体
        DataStream<ProcessedHotSearch> processedStream = itemStream.map(new MapFunction<HotSearchItem, ProcessedHotSearch>() {
            @Override
            public ProcessedHotSearch map(HotSearchItem item) throws Exception {
                // 调用 SentimentAnalyzer 计算情感得分和标签
                int score = SentimentAnalyzer.analyzeScore(item.getTitle());
                String label = SentimentAnalyzer.analyzeLabel(item.getTitle());

                // 转换热度值为 Long 类型（去除可能带有的非数字字符，这里为了安全做简单解析）
                long hotCount = 0L;
                try {
                    if (item.getHotCount() != null) {
                        hotCount = Long.parseLong(item.getHotCount().replaceAll("[^0-9]", ""));
                    }
                } catch (Exception e) {
                    hotCount = 0L; // 解析失败则赋默认值
                }

                // 决定唯一 ID (优先使用 URL，否则用排名代替)
                String id = (item.getUrl() != null && !item.getUrl().isEmpty()) ? item.getUrl() : String.valueOf(item.getRank());
                // 时间戳判定
                long ts = item.getFirstCrawled() > 0 ? item.getFirstCrawled() * 1000L : System.currentTimeMillis();

                // 返回组装好的落库实体
                return new ProcessedHotSearch(id, item.getRank(), item.getTitle(), hotCount, score, label, ts);
            }
        }).name("Sentiment Analysis Transformation");

        // 6. 将处理后的数据写入 MySQL (Sink 算子)
        // 编写 SQL：如果由于多次爬取导致数据重复(id相同)，则更新热度、情感和时间 (UPSERT 语义)
        String insertSql = "INSERT INTO processed_weibo_hot_search " +
                "(id, rank_num, title, hot_count, sentiment_score, sentiment_label, event_time) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?) " +
                "ON DUPLICATE KEY UPDATE " +
                "rank_num=VALUES(rank_num), hot_count=VALUES(hot_count), " +
                "sentiment_score=VALUES(sentiment_score), sentiment_label=VALUES(sentiment_label), event_time=VALUES(event_time)";

        processedStream.addSink(JdbcSink.sink(
                insertSql,
                (statement, processedHotSearch) -> {
                    // 将 ProcessedHotSearch 对象的属性映射到 SQL 的 '?' 占位符上
                    statement.setString(1, processedHotSearch.getId());
                    statement.setInt(2, processedHotSearch.getRank());
                    statement.setString(3, processedHotSearch.getTitle());
                    statement.setLong(4, processedHotSearch.getHotCount());
                    statement.setInt(5, processedHotSearch.getSentimentScore());
                    statement.setString(6, processedHotSearch.getSentimentLabel());
                    statement.setLong(7, processedHotSearch.getEventTime());
                },
                // 批处理执行选项：每累积 50 条数据，或者每隔 2 秒钟，执行一次批量写入（大幅提升 MySQL 写入性能）
                JdbcExecutionOptions.builder()
                        .withBatchSize(50)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                // 数据库连接配置
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(AppConfig.MYSQL_URL)
                        .withDriverName(AppConfig.MYSQL_DRIVER)
                        .withUsername(AppConfig.MYSQL_USERNAME)
                        .withPassword(AppConfig.MYSQL_PASSWORD)
                        .build()
        )).name("MySQL JDBC Sink");

        // 7. 触发执行 (必须要有这一句，否则 Flink 只会构建数据流向图而不会真正运行)
        env.execute("Weibo HotSearch Real-time Pipeline");
    }
}