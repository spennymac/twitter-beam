package com.compulsivesoftware.twitterbeam.transforms;

import com.compulsivesoftware.twitterbeam.bigquery.JsonToTableRow;
import com.compulsivesoftware.twitterbeam.inflight.StatusTheme;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.services.bigquery.model.TableRow;
import org.apache.beam.sdk.extensions.jackson.AsJsons;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StatusThemeToTableRow extends PTransform<PCollection<StatusTheme>, PCollection<TableRow>> {
    public static final TupleTag<TableRow> TRANSFORM_OUT_SUCCESS = new TupleTag<TableRow>() {
    };
    private static final Logger LOG = LoggerFactory.getLogger(StatusThemeToTableRow.class);
    private static final TupleTag<String> TRANSFORM_OUT_FAILURE = new TupleTag<String>() {
    };

    private final String context;

    public StatusThemeToTableRow(String context) {
        this.context = context;
    }

    @Override
    public PCollection<TableRow> expand(PCollection<StatusTheme> input) {
        PCollectionTuple jsonToTableRowOut = input.apply("Serialize To Json", AsJsons.of(StatusTheme.class).withMapper(new ObjectMapper().findAndRegisterModules()))
                .apply(JsonToTableRow.newBuilder().setFailureTag(TRANSFORM_OUT_FAILURE)
                        .setSuccessTag(TRANSFORM_OUT_SUCCESS).build());

        return jsonToTableRowOut.get(TRANSFORM_OUT_SUCCESS).apply("Add context to table row", MapElements.via(
                new SimpleFunction<TableRow, TableRow>() {
                    public TableRow apply(TableRow row) {
                        TableRow contextAdd = row.clone();
                        return contextAdd.set("context", context);
                    }
                }));
    }
}
