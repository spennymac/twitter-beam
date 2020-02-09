package com.compulsivesoftware.twitterbeam.util;


import java.util.List;
import java.util.Map;

public interface KeywordThemeParser {
    Map<String, String> parse(List<String> keywords) throws RuntimeException;
}
