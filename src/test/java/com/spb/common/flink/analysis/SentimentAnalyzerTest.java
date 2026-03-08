package com.spb.common.flink.analysis;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class SentimentAnalyzerTest {

    @Test
    public void testPositive() {
        assertEquals(1, SentimentAnalyzer.analyzeScore("This is great and good"));
        assertEquals("POSITIVE", SentimentAnalyzer.analyzeLabel("非常好 支持"));
    }

    @Test
    public void testNegative() {
        assertEquals(-1, SentimentAnalyzer.analyzeScore("This is bad and terrible"));
        assertEquals("NEGATIVE", SentimentAnalyzer.analyzeLabel("很差 不满"));
    }

    @Test
    public void testNeutral() {
        assertEquals(0, SentimentAnalyzer.analyzeScore("This is a topic"));
        assertEquals("NEUTRAL", SentimentAnalyzer.analyzeLabel("关于天气的讨论"));
    }
}
