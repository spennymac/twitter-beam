package com.compulsivesoftware.twitterbeam;


import com.compulsivesoftware.twitterbeam.inflight.StatusTheme;
import com.compulsivesoftware.twitterbeam.io.TwitterIO;
import com.compulsivesoftware.twitterbeam.options.TwitterToBigQueryOptions;
import com.compulsivesoftware.twitterbeam.transforms.ExtractTheme;
import com.compulsivesoftware.twitterbeam.transforms.StatusThemeToTableRow;
import com.compulsivesoftware.twitterbeam.util.DelimiterKeywordThemeParser;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryInsertError;
import org.apache.beam.sdk.io.gcp.bigquery.InsertRetryPolicy;
import org.apache.beam.sdk.io.gcp.bigquery.WriteResult;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;


public class TwitterToBigQuery {
    private static final Logger LOG = LoggerFactory.getLogger(TwitterToBigQuery.class);

    public static void main(String[] args) {
        PipelineOptionsFactory.register(TwitterToBigQueryOptions.class);
        TwitterToBigQueryOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(TwitterToBigQueryOptions.class);
        options.setStreaming(true);
        run(options);
    }

    public static PipelineResult run(TwitterToBigQueryOptions options) {

        Pipeline pipeline = Pipeline.create(options);

        Map<String, String> keywordThemes =
                new DelimiterKeywordThemeParser(options.getKeywordThemeDelimiter()).parse(options.getKeywords());

        PCollection<TwitterIO.StatusMsg> messages = pipeline
                .apply(
                        "Read Live Twitter Feed ",
                        TwitterIO.read().withKeywords(
                                options.getConsumerKey(),
                                options.getConsumerSecret(),
                                options.getAccessToken(),
                                options.getAccessTokenSecret(),
                                new ArrayList(keywordThemes.keySet())
                        ));


        PCollectionTuple extracthemeMsg = messages.apply("ParseThemesFromTweet", new ExtractTheme(keywordThemes));
        PCollection<StatusTheme> statusThemes = extracthemeMsg.get(ExtractTheme.TRANSFORM_OUT_SUCCESS);

        PCollection<TableRow> convertedTableRows = statusThemes.apply("Convert to TableRow", new StatusThemeToTableRow(options.getContext()));

        WriteResult writeResult =
                convertedTableRows
                        .apply(
                                "WriteSuccessfulRecords",
                                BigQueryIO.writeTableRows()
                                        .withoutValidation()
                                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                                        .withExtendedErrorInfo()
                                        .ignoreUnknownValues()
                                        .withMethod(BigQueryIO.Write.Method.STREAMING_INSERTS)
                                        .withFailedInsertRetryPolicy(InsertRetryPolicy.retryTransientErrors())
                                        .to(options.getOutputTableSpec()));

        writeResult.getFailedInsertsWithErr()
                .apply("LogFailedData", ParDo.of(new DoFn<BigQueryInsertError, BigQueryInsertError>() {
                    @DoFn.ProcessElement
                    public void processElement(ProcessContext c) {
                        BigQueryInsertError er = c.element();
                        try {
                            LOG.error(er.getError().toPrettyString());
                        } catch (IOException ex) {
                            er.getError().getErrors().forEach(error -> {
                                LOG.error(String.format("Insert failed - backup error string: %s", error.getDebugInfo()));
                            });
                        }
                    }
                }));


        return pipeline.run();
    }
}
