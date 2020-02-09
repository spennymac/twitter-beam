package com.compulsivesoftware.twitterbeam.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface PubsubOptions extends PipelineOptions, StreamingOptions {
    @Description(
            "The Cloud Pub/Sub subscription to consume from. "
                    + "The name should be in the format of "
                    + "projects/<project-id>/subscriptions/<subscription-name>.")
    @Validation.Required
    String getInputSubscription();

    void setInputSubscription(String inputSubscription);
}
