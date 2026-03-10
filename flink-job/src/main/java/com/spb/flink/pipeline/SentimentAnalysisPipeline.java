package com.spb.flink.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spb.flink.job.SentimentAnalyzer;
import com.spb.flink.sink.SentimentResult;
import com.spb.common.model.HotSearchItem;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

import java.util.Objects;

/**
 * SentimentAnalysisPipeline
 * -------------------------
 * 封装从 Kafka 字符串流到 SentimentResult 的完整转换逻辑。
 *
 * 数据流向：
 *   DataStream<String> (JSON)
 *     → parseJsonStream() → DataStream<HotSearchItem>
 *     → toSentiment()     → DataStream<SentimentResult>
 *
 * 设计说明：
 * 1. 本类专注于解析和轻量级转换，保持无状态
 * 2. 复杂业务转换已迁移至 TransformationFactory
 * 3. ID 生成策略：使用 title + rank 组合（取代已移除的 url 字段）
 */
@Slf4j
public final class SentimentAnalysisPipeline {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private SentimentAnalysisPipeline() {}

    /**
     * 将 JSON 字符串流解析为 HotSearchItem 流，解析失败的条目自动过滤。
     *
     * @param jsonStream 来自 Kafka Source 的原始 JSON 字符串流
     * @return 解析成功的 HotSearchItem 流
     */
    public static SingleOutputStreamOperator<HotSearchItem> parseJsonStream(DataStream<String> jsonStream) {
        return jsonStream
                .map((MapFunction<String, HotSearchItem>) json -> {
                    try {
                        return MAPPER.readValue(json, HotSearchItem.class);
                    } catch (Exception e) {
                        log.error("❌ 解析 JSON 失败！收到的脏数据是: {}", json, e);
                        return null; // 解析失败 → 返回 null，后续 filter 过滤
                    }
                })
                .returns(Types.POJO(HotSearchItem.class))
                .filter(Objects::nonNull);
    }

    /**
     * 方便单元测试：解析单条 JSON 为 HotSearchItem（不依赖 Flink 运行时）。
     *
     * @param json 单条 JSON 字符串
     * @return 解析成功返回 HotSearchItem，失败返回 null
     */
    public static HotSearchItem parseJson(String json) {
        try {
            return MAPPER.readValue(json, HotSearchItem.class);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * 生成唯一标识符（基于 title + rank，取代已移除的 url 字段）。
     * 策略：title + "#" + rank 的组合哈希，确保同一话题的稳定性。
     *
     * @param title 热搜标题
     * @param rank  当前排名
     * @return 唯一标识符
     */
    public static String generateId(String title, int rank) {
        String raw = title + "#" + rank;
        return Integer.toHexString(raw.hashCode());
    }

    /**
     * 处理时间戳（秒级转毫秒级）。
     *
     * @param firstCrawled 首次爬取时间戳（秒级）
     * @return 毫秒级时间戳
     */
    public static long resolveTimestamp(long firstCrawled) {
        return firstCrawled > 0 ? firstCrawled * 1000L : System.currentTimeMillis();
    }

    /**
     * 将 HotSearchItem 流映射为 SentimentResult 流。
     *
     * 注意：此方法提供轻量级转换，完整业务转换请使用 TransformationFactory。
     *
     * @param items HotSearchItem 数据流
     * @return SentimentResult 数据流
     */
    public static SingleOutputStreamOperator<SentimentResult> toSentiment(DataStream<HotSearchItem> items) {
        return items
                .map((MapFunction<HotSearchItem, SentimentResult>) item -> {
                    // 使用 SentimentAnalyzer 分析标题的情感得分和标签
                    int score = SentimentAnalyzer.analyzeScore(item.getTitle());
                    String label = SentimentAnalyzer.analyzeLabel(item.getTitle());

                    // 处理时间戳
                    long ts = resolveTimestamp(item.getFirstCrawled());

                    // 生成唯一 ID（使用 title + rank，取代 url）
                    String id = generateId(item.getTitle(), item.getRank());

                    // 返回情感分析结果
                    return new SentimentResult(id, score, label, ts);
                })
                .returns(Types.POJO(SentimentResult.class));
    }
}
