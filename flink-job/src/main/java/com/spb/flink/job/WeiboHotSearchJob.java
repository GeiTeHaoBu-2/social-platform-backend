package com.spb.flink.job;

import com.spb.common.config.AppConfig;
import com.spb.common.model.HotSearchItem;
import com.spb.common.model.ProcessedHotSearch;
import com.spb.flink.pipeline.SentimentAnalysisPipeline;
import com.spb.flink.sink.SinkFactory;
import com.spb.flink.source.KafkaSourceFactory;
import com.spb.flink.transform.TransformationFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * WeiboHotSearchJob
 * -----------------
 * Flink 实时计算程序的统一入口类。
 *
 * 【架构设计原则：调度与执行分离】
 * ==================================
 * 本类作为调度者（Orchestrator），只负责掌控全局数据流向，
 * 将所有具体执行逻辑委托给专门的工厂类：
 *
 *   Source 层  : KafkaSourceFactory      - 创建 Kafka 数据源
 *   转换层     : TransformationFactory   - 数据清洗、情感分析、实体转换
 *   Sink 层    : SinkFactory             - 创建 MySQL 输出目标
 *
 * 这种设计的优势：
 * 1. 彻底解耦：每个模块只做纯输入输出，通过方法参数传递数据
 * 2. 高度可测试：每个工厂方法可独立单元测试，无需启动完整 Flink 环境
 * 3. 职责清晰：Job 只编排流程，工厂负责实现，符合单一职责原则
 */
public final class WeiboHotSearchJob {

    public static void main(String[] args) throws Exception {
        // ========== 1. 初始化执行环境 ==========
        StreamExecutionEnvironment env = createExecutionEnvironment();

        // ========== 2. 构建数据源 (Source) ==========
        DataStream<HotSearchItem> itemStream = buildSourcePipeline(env);

        // ========== 3. 数据转换 (Transformation) ==========
        DataStream<ProcessedHotSearch> processedStream = buildTransformationPipeline(itemStream);

        // ========== 4. 构建输出目标 (Sink) ==========
        buildSinkPipeline(processedStream);

        // ========== 5. 触发执行 ==========
        env.execute("Weibo HotSearch Real-time Pipeline");
    }

    /**
     * 创建并配置 Flink 流处理执行环境。
     * 独立的配置方法，便于测试时注入不同环境。
     *
     * @return 配置好的 StreamExecutionEnvironment
     */
    private static StreamExecutionEnvironment createExecutionEnvironment() {
        return StreamExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * 构建数据源流水线。
     * 职责：从 Kafka 摄取原始数据并解析为领域实体。
     *
     * 步骤：
     * 1. 使用 KafkaSourceFactory 创建 Kafka Source
     * 2. 从 Source 读取原始 JSON 字符串流
     * 3. 使用 SentimentAnalysisPipeline 解析为 HotSearchItem
     *
     * @param env Flink 执行环境
     * @return HotSearchItem 数据流
     */
    private static DataStream<HotSearchItem> buildSourcePipeline(StreamExecutionEnvironment env) {
        // 2.1 创建 Kafka Source（委托给工厂类）
        KafkaSource<String> kafkaSource = KafkaSourceFactory.create(
                AppConfig.KAFKA_BOOTSTRAP_SERVERS,
                AppConfig.KAFKA_TOPIC,
                AppConfig.KAFKA_GROUP_ID
        );

        // 2.2 读取原始 JSON 流
        DataStream<String> rawJsonStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Weibo HotSearch Kafka Source"
        );

        // 调试输出：打印原始数据（可在生产环境移除）
        rawJsonStream.print("【Kafka原始数据】");

        // 2.3 解析为实体流（委托给 Pipeline）
        return SentimentAnalysisPipeline.parseJsonStream(rawJsonStream);
    }

    /**
     * 构建数据转换流水线。
     * 职责：将原始实体转换为带情感分析结果的落库实体。
     *
     * 步骤：
     * 4. 使用 TransformationFactory 进行情感分析和实体转换
     *
     * @param itemStream HotSearchItem 输入流
     * @return ProcessedHotSearch 输出流
     */
    private static DataStream<ProcessedHotSearch> buildTransformationPipeline(
            DataStream<HotSearchItem> itemStream) {

        // 第5步抽象：委托给 TransformationFactory
        return TransformationFactory.toProcessedHotSearch(itemStream);
    }

    /**
     * 构建输出目标流水线。
     * 职责：将处理后的数据持久化到 MySQL。
     *
     * 步骤：
     * 6. 使用 SinkFactory 创建 JDBC Sink 并添加
     *
     * @param processedStream ProcessedHotSearch 输入流
     */
    private static void buildSinkPipeline(DataStream<ProcessedHotSearch> processedStream) {
        // 第6步抽象：委托给 SinkFactory
        processedStream
                .addSink(SinkFactory.createMySqlSink())
                .name("MySQL JDBC Sink");
    }
}
