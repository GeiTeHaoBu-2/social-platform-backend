package com.withpy.socialplatformback.flink.analysis;

/**
 * SentimentResult
 * ---------------
 * Flink 情感分析算子的输出 POJO。
 * 作为 Flink DataStream 的元素类型，需满足：
 *   1. 公有无参构造器
 *   2. 所有字段公有（或提供 getter/setter）
 */
public class SentimentResult {

    /** 热搜条目标识（url 或 rank 字符串） */
    public String id;
    /** 情感得分：-1（负面）/ 0（中性）/ 1（正面） */
    public int score;
    /** 情感标签：NEGATIVE / NEUTRAL / POSITIVE */
    public String label;
    /** 事件时间戳（毫秒），来自 first_crawled 或处理时间 */
    public long eventTs;

    /** Flink POJO 要求的无参构造器 */
    public SentimentResult() {}

    public SentimentResult(String id, int score, String label, long eventTs) {
        this.id = id;
        this.score = score;
        this.label = label;
        this.eventTs = eventTs;
    }

    @Override
    public String toString() {
        return "SentimentResult{" +
                "id='" + id + '\'' +
                ", score=" + score +
                ", label='" + label + '\'' +
                ", eventTs=" + eventTs +
                '}';
    }
}
