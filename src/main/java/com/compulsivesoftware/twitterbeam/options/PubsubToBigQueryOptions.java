package com.compulsivesoftware.twitterbeam.options;

import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

public interface PubsubToBigQueryOptions extends PipelineOptions, StreamingOptions, BigQueryOptions, PubsubOptions {

}
