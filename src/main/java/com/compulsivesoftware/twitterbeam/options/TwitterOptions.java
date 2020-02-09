package com.compulsivesoftware.twitterbeam.options;

import org.apache.beam.sdk.options.*;

import java.util.List;

public interface TwitterOptions extends PipelineOptions, StreamingOptions {

    @Description("twitter api consumer key")
    @Validation.Required
    String getConsumerKey();

    void setConsumerKey(String value);

    @Description("twitter api consumer secret key")
    @Validation.Required
    String getConsumerSecret();

    void setConsumerSecret(String value);

    @Description("twitter api consumer access token")
    @Validation.Required
    String getAccessToken();

    void setAccessToken(String value);

    @Description("twitter api consumer access token secret")
    @Validation.Required
    String getAccessTokenSecret();

    void setAccessTokenSecret(String value);

    @Description("character to delimit keywords and themes ex: ':' -> keyword:theme")
    @Default.String(":")
    String getKeywordThemeDelimiter();

    void setKeywordThemeDelimiter(String value);


    @Description("comma separated list of keywords with their associated theme")
    @Validation.Required
    List<String> getKeywords();

    void setKeywords(List<String> keywords);
}

