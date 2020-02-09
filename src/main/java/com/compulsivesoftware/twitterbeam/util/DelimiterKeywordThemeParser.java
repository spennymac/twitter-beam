package com.compulsivesoftware.twitterbeam.util;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DelimiterKeywordThemeParser implements KeywordThemeParser {
    private final String _delimiter;

    public DelimiterKeywordThemeParser(String delimiter) {
        this._delimiter = delimiter;
    }

    @Override
    public Map<String, String> parse(List<String> keywords) throws RuntimeException {
        Map<String, String> keywordsToTheme = new HashMap<>();

        for (String keyword : keywords) {
            String[] pair = keyword.split(_delimiter);
            if (pair.length <= 1) {
                throw new RuntimeException(String.format("keyword `%s` must have a theme attached delimited with `%s`", keyword, _delimiter));
            }

            if (keywordsToTheme.containsKey(pair[0].toUpperCase())) {
                throw new RuntimeException(String.format("keyword `%s` already exists", pair[0].toUpperCase()));
            }
            keywordsToTheme.put(pair[0].toUpperCase(), pair[1].toUpperCase());
        }
        return keywordsToTheme;
    }
}
