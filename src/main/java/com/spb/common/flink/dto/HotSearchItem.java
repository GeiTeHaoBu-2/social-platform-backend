package com.spb.common.flink.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * HotSearchItem
 * 从 Kafka 消费到的单条微博热搜数据 DTO。
 * 由 Python 爬虫序列化为 JSON 后写入 Kafka，Flink 端反序列化使用。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class HotSearchItem {

    private int rank;
    private String title;
    private String url;
    @JsonProperty("hot_count")
    private String hotCount;
    private String tag;
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

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getHotCount() {
        return hotCount;
    }

    public void setHotCount(String hotCount) {
        this.hotCount = hotCount;
    }

    public String getTag() {
        return tag;
    }

    public void setTag(String tag) {
        this.tag = tag;
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
                ", url='" + url + '\'' +
                ", hotCount='" + hotCount + '\'' +
                ", tag='" + tag + '\'' +
                ", firstCrawled=" + firstCrawled +
                ", source='" + source + '\'' +
                '}';
    }
}
