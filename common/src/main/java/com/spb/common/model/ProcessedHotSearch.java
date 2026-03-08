package com.spb.common.model;

import java.io.Serializable;

/**
 * ProcessedHotSearch 舆情分析结果实体类
 * -------------------
 * 用于封装 Flink 处理完毕后，准备写入 MySQL 的最终数据。
 * 这个类的结构应该与 MySQL 中的表结构保持高度一致。
 */
public class ProcessedHotSearch implements Serializable {

    // 1. 基础维度 (继承自爬虫原始数据)
    /** 唯一标识 (通常使用微博条目的 URL 或生成的 UUID) */
    private String id;
    /** 当前热搜排名 */
    private Integer rank;
    /** 热搜话题标题 */
    private String title;
    /** 热度值 (建议在 Flink 中将爬虫的 String 转为 Long，方便后续做趋势分析和图表渲染) */
    private Long hotCount;

    // 2. 分析维度 (由 Flink 实时计算得出)
    /** 情感得分：1(正面), 0(中性), -1(负面) */
    private Integer sentimentScore;
    /** 情感标签："POSITIVE", "NEUTRAL", "NEGATIVE" */
    private String sentimentLabel;

    // 3. 时间维度
    /** 记录该条热搜首次被抓取或处理的时间戳 (毫秒级别) */
    private Long eventTime;

    /**
     * Flink POJO 规范强制要求 1：必须包含一个公共的无参构造函数。
     * 否则 Flink 无法利用其高效的 POJO 序列化器，会退化为低效的 Kryo 序列化。
     */
    public ProcessedHotSearch() {
    }

    /**
     * 全参构造函数，方便我们在 Flink 算子中通过 new ProcessedHotSearch(...) 快速赋值
     */
    public ProcessedHotSearch(String id, Integer rank, String title, Long hotCount,
                              Integer sentimentScore, String sentimentLabel, Long eventTime) {
        this.id = id;
        this.rank = rank;
        this.title = title;
        this.hotCount = hotCount;
        this.sentimentScore = sentimentScore;
        this.sentimentLabel = sentimentLabel;
        this.eventTime = eventTime;
    }

    /* * Flink POJO 规范强制要求 2：所有的私有属性必须拥有 public 的 getter 和 setter 方法。
     * （如果你在工程里引入了 Lombok 依赖，这里可以直接用 @Data 注解代替下面这堆代码）
     */
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }

    public Integer getRank() { return rank; }
    public void setRank(Integer rank) { this.rank = rank; }

    public String getTitle() { return title; }
    public void setTitle(String title) { this.title = title; }

    public Long getHotCount() { return hotCount; }
    public void setHotCount(Long hotCount) { this.hotCount = hotCount; }

    public Integer getSentimentScore() { return sentimentScore; }
    public void setSentimentScore(Integer sentimentScore) { this.sentimentScore = sentimentScore; }

    public String getSentimentLabel() { return sentimentLabel; }
    public void setSentimentLabel(String sentimentLabel) { this.sentimentLabel = sentimentLabel; }

    public Long getEventTime() { return eventTime; }
    public void setEventTime(Long eventTime) { this.eventTime = eventTime; }

    @Override
    public String toString() {
        return "ProcessedHotSearch{" +
                "rank=" + rank +
                ", title='" + title + '\'' +
                ", sentimentLabel='" + sentimentLabel + '\'' +
                '}';
    }
}