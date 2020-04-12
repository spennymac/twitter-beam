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
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.Method.FILE_LOADS;


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

        convertedTableRows.apply("Write assets to BigQuery in batch", BigQueryIO
                .writeTableRows()
                .withTriggeringFrequency(Duration.standardMinutes(5))
                .withMethod(FILE_LOADS)
                .withNumFileShards(1)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER)
                .to(options.getOutputTableSpec()));

        return pipeline.run();
    }
}
