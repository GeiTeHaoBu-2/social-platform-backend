package com.withpy.socialplatformback.flink.analysis;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * SentimentAnalyzer
 * -----------------
 * 轻量级关键词情感分析器（无外部依赖）。
 * 使用正/负面关键词词表（中英文）对文本打分：
 *   1  → POSITIVE
 *   0  → NEUTRAL
 *  -1  → NEGATIVE
 *
 * 注意：这是用于快速原型验证的启发式基线方案。
 * 生产环境建议替换为训练好的模型或外部 NLP 服务（如 HanLP、SnowNLP）。
 */
public final class SentimentAnalyzer {

    private static final Set<String> POSITIVE = new HashSet<>(Arrays.asList(
            "good", "great", "happy", "love", "like", "excellent", "positive", "awesome",
            "支持", "赞", "好", "不错", "开心", "喜爱", "正面"
    ));

    private static final Set<String> NEGATIVE = new HashSet<>(Arrays.asList(
            "bad", "sad", "hate", "dislike", "terrible", "poor", "negative", "angry",
            "差", "糟糕", "失望", "不满", "愤怒", "反对", "骂"
    ));

    private SentimentAnalyzer() {}

    /**
     * 对文本进行情感打分。
     *
     * @param text 待分析文本
     * @return 1（正面）/ 0（中性）/ -1（负面）
     */
    public static int analyzeScore(String text) {
        if (text == null || text.isBlank()) {
            return 0;
        }
        String lower = text.toLowerCase();

        int pos = 0;
        int neg = 0;

        // 英文：按非单词字符分词后匹配
        String[] tokens = lower.split("\\W+");
        for (String t : tokens) {
            if (t.isBlank()) continue;
            if (POSITIVE.contains(t)) pos++;
            if (NEGATIVE.contains(t)) neg++;
        }

        // 中文及其他语言：子串匹配
        for (String p : POSITIVE) {
            if (p.codePoints().anyMatch(Character::isIdeographic)) {
                if (text.contains(p)) pos++;
            }
        }
        for (String n : NEGATIVE) {
            if (n.codePoints().anyMatch(Character::isIdeographic)) {
                if (text.contains(n)) neg++;
            }
        }

        if (pos > neg) return 1;
        if (neg > pos) return -1;
        return 0;
    }

    /**
     * 对文本进行情感标签分类。
     *
     * @param text 待分析文本
     * @return "POSITIVE" / "NEUTRAL" / "NEGATIVE"
     */
    public static String analyzeLabel(String text) {
        int s = analyzeScore(text);
        return s > 0 ? "POSITIVE" : (s < 0 ? "NEGATIVE" : "NEUTRAL");
    }
}
