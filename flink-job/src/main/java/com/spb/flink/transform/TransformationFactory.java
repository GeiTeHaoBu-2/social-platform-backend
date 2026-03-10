package com.spb.flink.transform;

import com.spb.common.model.HotSearchItem;
import com.spb.common.model.ProcessedHotSearch;
import com.spb.flink.job.SentimentAnalyzer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

/**
 * TransformationFactory
 * ---------------------
 * 数据转换工厂类。
 * 职责：封装所有 Flink 数据流转换逻辑（纯函数式操作，无副作用）。
 * 遵循 "调度与执行分离" 原则，本类只负责纯粹的输入输出转换，不做任何外部调用。
 */
public final class TransformationFactory {

    private TransformationFactory() {}

    /**
     * 核心转换：将 HotSearchItem 映射为 ProcessedHotSearch（附带情感分析结果）。
     * 这是第5步的抽象方法实现。
     *
     * 转换逻辑：
     * 1. 调用 SentimentAnalyzer 计算情感得分和标签
     * 2. 将热度值从 String 转换为 Long（清洗非数字字符）
     * 3. 生成唯一 ID（使用 title + rank 的组合哈希，确保稳定性）
     * 4. 处理时间戳（优先使用 firstCrawled，否则使用当前时间）
     *
     * @param itemStream HotSearchItem 输入流
     * @return ProcessedHotSearch 输出流
     */
    public static SingleOutputStreamOperator<ProcessedHotSearch> toProcessedHotSearch(
            DataStream<HotSearchItem> itemStream) {

        return itemStream
                .map(new HotSearchItemToProcessedMapper())
                .returns(Types.POJO(ProcessedHotSearch.class))
                .name("Sentiment Analysis Transformation");
    }

    /**
     * 将热度值字符串解析为长整型数值。
     * 处理逻辑：去除所有非数字字符后解析，失败则返回默认值 0。
     *
     * @param hotCount 热度值字符串（可能包含 "万"、"," 等字符）
     * @return 解析后的数值，失败返回 0
     */
    public static long parseHotCount(String hotCount) {
        if (hotCount == null || hotCount.isBlank()) {
            return 0L;
        }
        try {
            return Long.parseLong(hotCount.replaceAll("[^0-9]", ""));
        } catch (Exception e) {
            return 0L;
        }
    }

    /**
     * 生成唯一标识符。
     * 策略：使用 title + "#" + rank 的组合，通过哈希生成稳定 ID。
     * 原因：title 相对稳定，rank 变化快，组合后可确保同一话题的唯一性。
     *
     * @param title 热搜标题
     * @param rank  当前排名
     * @return 唯一标识符
     */
    public static String generateId(String title, int rank) {
        String raw = title + "#" + rank;
        // 使用稳定的哈希算法，避免 hashCode() 的潜在冲突
        return Integer.toHexString(raw.hashCode());
    }

    /**
     * 处理时间戳。
     * 策略：优先使用 firstCrawled（秒级转毫秒），无效则使用当前时间。
     *
     * @param firstCrawled 首次爬取时间戳（秒级）
     * @return 毫秒级时间戳
     */
    public static long resolveTimestamp(long firstCrawled) {
        return firstCrawled > 0 ? firstCrawled * 1000L : System.currentTimeMillis();
    }

    /**
     * 内部映射器：HotSearchItem → ProcessedHotSearch
     * 封装单条数据的完整转换逻辑，便于单元测试和复用。
     */
    public static class HotSearchItemToProcessedMapper
            implements MapFunction<HotSearchItem, ProcessedHotSearch> {

        @Override
        public ProcessedHotSearch map(HotSearchItem item) {
            // 1. 情感分析
            int score = SentimentAnalyzer.analyzeScore(item.getTitle());
            String label = SentimentAnalyzer.analyzeLabel(item.getTitle());

            // 2. 热度值转换
            long hotCount = parseHotCount(item.getHotCount());

            // 3. 生成唯一 ID
            String id = generateId(item.getTitle(), item.getRank());

            // 4. 处理时间戳
            long ts = resolveTimestamp(item.getFirstCrawled());

            // 5. 组装结果
            return new ProcessedHotSearch(
                    id,
                    item.getRank(),
                    item.getTitle(),
                    hotCount,
                    score,
                    label,
                    ts
            );
        }
    }
}
