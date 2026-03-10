package com.spb.common.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * HotSearchItem
 * 从 Kafka 消费到的单条微博热搜数据 DTO。
 * 由 Python 爬虫序列化为 JSON 后写入 Kafka，Flink 端反序列化使用。
 */
//ignore开了，如果爬虫后续添加了新的字段，Flink 端不会因为找不到对应属性而抛异常，保持兼容性。
@JsonIgnoreProperties(ignoreUnknown = true)
public class HotSearchItem {

    @JsonProperty("rank")
    private int rank;
    @JsonProperty("title")
    private String title;
    @JsonProperty("hot_count")
    private String hotCount;
    @JsonProperty("first_crawled")
    private long firstCrawled;
    private String source;

    public HotSearchItem() {
    }

    public int getRank() {
        return rank;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getHotCount() {
        return hotCount;
    }

    public void setHotCount(String hotCount) {
        this.hotCount = hotCount;
    }

    public long getFirstCrawled() {
        return firstCrawled;
    }

    public void setFirstCrawled(long firstCrawled) {
        this.firstCrawled = firstCrawled;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    @Override
    public String toString() {
        return "HotSearchItem{" +
                "rank=" + rank +
                ", title='" + title + '\'' +
                ", hotCount='" + hotCount + '\'' +
                ", firstCrawled=" + firstCrawled +
                ", source='" + source + '\'' +
                '}';
    }
}
