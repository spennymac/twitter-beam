package com.compulsivesoftware.twitterbeam.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface BigQueryOptions extends PipelineOptions, StreamingOptions {
    @Description("Table spec to write the output to. Should be in the format of "
            + "<project_id>:<dataset_id>.<table_id>")
    @Validation.Required
    String getOutputTableSpec();

    void setOutputTableSpec(String value);
}
