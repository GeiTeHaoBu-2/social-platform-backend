package com.withpy.socialplatformback.flink.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.withpy.socialplatformback.flink.analysis.SentimentAnalyzer;
import com.withpy.socialplatformback.flink.analysis.SentimentResult;
import com.withpy.socialplatformback.flink.dto.HotSearchItem;
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
 *   DataStream&lt;String&gt; (JSON)
 *     → parseJsonStream() → DataStream&lt;HotSearchItem&gt;
 *     → toSentiment()     → DataStream&lt;SentimentResult&gt;
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
                    int score = SentimentAnalyzer.analyzeScore(item.getTitle());
                    String label = SentimentAnalyzer.analyzeLabel(item.getTitle());
                    long ts = item.getFirstCrawled() > 0
                            ? item.getFirstCrawled() * 1000L
                            : System.currentTimeMillis();
                    String id = item.getUrl() != null ? item.getUrl() : String.valueOf(item.getRank());
                    return new SentimentResult(id, score, label, ts);
                })
                .returns(Types.POJO(SentimentResult.class));
    }
}
