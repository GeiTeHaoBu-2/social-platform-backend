package com.withpy.socialplatformback.flink.job;

import com.withpy.socialplatformback.flink.analysis.SentimentResult;
import com.withpy.socialplatformback.flink.dto.HotSearchItem;
import com.withpy.socialplatformback.flink.pipeline.SentimentAnalysisPipeline;
import com.withpy.socialplatformback.kafka.KafkaSourceFactory;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * WeiboHotSearchJob
 * -----------------
 * Flink 流处理作业主逻辑。
 *
 * 数据流向：
 *   Kafka Source
 *     → parseJsonStream()  → DataStream&lt;HotSearchItem&gt;
 *     → toSentiment()      → DataStream&lt;SentimentResult&gt;
 *     → keyBy + window     → 滚动窗口聚合统计
 *     → print              → 控制台输出（后续替换为 MySQL Sink）
 *
 * 该类只提供 run() 方法，不包含 main()，由 web 层的启动入口调用。
 */
public final class WeiboHotSearchJob {

    private static final Logger log = LoggerFactory.getLogger(WeiboHotSearchJob.class);

    private WeiboHotSearchJob() {}

    /**
     * 启动 Flink 作业。
     *
     * @param bootstrapServers Kafka bootstrap.servers 地址
     * @param topic            Kafka topic 名称
     * @param groupId          Kafka consumer group id
     * @param windowMs         滚动窗口大小（毫秒）
     */
    public static void run(String bootstrapServers, String topic, String groupId, long windowMs) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开发模式下禁用 ClosureCleaner，避免反射问题（可替换为 JVM --add-opens）
        env.getConfig().disableClosureCleaner();

        KafkaSource<String> source = KafkaSourceFactory.create(bootstrapServers, topic, groupId);

        SingleOutputStreamOperator<HotSearchItem> items =
                SentimentAnalysisPipeline.parseJsonStream(
                        env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source"));

        SingleOutputStreamOperator<SentimentResult> sentiments =
                SentimentAnalysisPipeline.toSentiment(items);

        sentiments
                .keyBy(r -> "global")
                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(windowMs)))
                .apply((WindowFunction<SentimentResult, String, String, TimeWindow>) (key, window, input, out) -> {
                    Map<String, Integer> counts = new HashMap<>();
                    counts.put("POSITIVE", 0);
                    counts.put("NEUTRAL", 0);
                    counts.put("NEGATIVE", 0);
                    int total = 0;
                    for (SentimentResult r : input) {
                        counts.merge(r.label, 1, Integer::sum);
                        total++;
                    }
                    String summary = String.format(
                            "window [%d - %d]: total=%d, POS=%d, NEU=%d, NEG=%d",
                            window.getStart(), window.getEnd(), total,
                            counts.get("POSITIVE"), counts.get("NEUTRAL"), counts.get("NEGATIVE"));
                    out.collect(summary);
                })
                .print()
                .name("print-window-summary");

        env.execute("Weibo Hot Search Sentiment Analysis Job");
    }
}
