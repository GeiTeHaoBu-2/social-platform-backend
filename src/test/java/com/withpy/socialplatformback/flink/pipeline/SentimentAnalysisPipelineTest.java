package com.withpy.socialplatformback.flink.pipeline;

import com.withpy.socialplatformback.flink.dto.HotSearchItem;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SentimentAnalysisPipelineTest {

    @Test
    public void testParseJson() {
        String json1 = "{\"rank\":1,\"title\":\"很好\",\"url\":\"http://t\",\"hot_count\":100,\"first_crawled\":1700000000,\"source\":\"weibo\"}";
        String json2 = "{\"rank\":2,\"title\":\"糟糕\",\"url\":\"http://t2\",\"hot_count\":50,\"first_crawled\":1700000000,\"source\":\"weibo\"}";

        HotSearchItem h1 = SentimentAnalysisPipeline.parseJson(json1);
        HotSearchItem h2 = SentimentAnalysisPipeline.parseJson(json2);

        assertNotNull(h1);
        assertEquals(1, h1.getRank());
        assertEquals("很好", h1.getTitle());

        assertNotNull(h2);
        assertEquals(2, h2.getRank());
        assertEquals("糟糕", h2.getTitle());
    }
}
