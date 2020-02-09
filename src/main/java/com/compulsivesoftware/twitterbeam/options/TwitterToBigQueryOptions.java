package com.compulsivesoftware.twitterbeam.options;


import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;

public interface TwitterToBigQueryOptions extends BigQueryOptions, TwitterOptions {
    @Description("context of current pipeline")
    @Validation.Required
    String getContext();

    void setContext(String value);
}
