package com.spb.common.flink.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.spb.common.flink.analysis.SentimentAnalyzer;
import com.spb.common.flink.analysis.SentimentResult;
import com.spb.common.flink.dto.HotSearchItem;
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
 * 修改内容：
 * 1. 添加注释，详细说明每个方法的功能。
 * 2. 确认逻辑完整性，确保从 JSON 到情感分析结果的转换无遗漏。
 */
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
     * 将 HotSearchItem 流映射为 SentimentResult 流。
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

                    // 确定时间戳，优先使用 firstCrawled，否则使用当前时间
                    long ts = item.getFirstCrawled() > 0
                            ? item.getFirstCrawled() * 1000L
                            : System.currentTimeMillis();

                    // 确定唯一标识符，优先使用 URL，否则使用排名
                    String id = item.getUrl() != null ? item.getUrl() : String.valueOf(item.getRank());

                    // 返回情感分析结果
                    return new SentimentResult(id, score, label, ts);
                })
                .returns(Types.POJO(SentimentResult.class));
    }
}
