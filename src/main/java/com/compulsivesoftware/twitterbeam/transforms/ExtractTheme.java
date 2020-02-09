package com.compulsivesoftware.twitterbeam.transforms;

import com.compulsivesoftware.twitterbeam.inflight.StatusTheme;
import com.compulsivesoftware.twitterbeam.io.TwitterIO;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExtractTheme extends PTransform<PCollection<TwitterIO.StatusMsg>, PCollectionTuple> {
    public static final TupleTag<StatusTheme> TRANSFORM_OUT_SUCCESS = new TupleTag<StatusTheme>() {
    };
    public static final TupleTag<TwitterIO.StatusMsg> TRANSFORM_OUT_FAILURE = new TupleTag<TwitterIO.StatusMsg>() {
    };
    private static final Logger LOG = LoggerFactory.getLogger(ExtractTheme.class);
    private final Counter noThemeExtracted = Metrics.counter(ExtractTheme.class, "noThemeExtracted");

    private final Map<String, String> keywordToTheme;
    private final Set<String> mapKeys;

    public ExtractTheme(Map<String, String> keywordToTheme) {
        this.keywordToTheme = ImmutableMap.copyOf(keywordToTheme);
        this.mapKeys = ImmutableSet.copyOf(keywordToTheme.keySet());
    }

    @Override
    public PCollectionTuple expand(PCollection<TwitterIO.StatusMsg> input) {
        PCollectionTuple extractTheme = input.apply(
                ParDo.of(new DoFn<TwitterIO.StatusMsg, StatusTheme>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                        String normal = context.element().getStatus().getText().toUpperCase();
                        Set<String> themes = mapKeys.stream().filter(normal::contains).map(keywordToTheme::get).collect(Collectors.toSet());
                        if (themes.size() != 0) {
                            context.output(StatusTheme.of(context.element(), themes));
                        } else {
                            noThemeExtracted.inc();
                            context.output(TRANSFORM_OUT_FAILURE, context.element());
                        }
                    }
                }).withOutputTags(TRANSFORM_OUT_SUCCESS, TupleTagList.of(TRANSFORM_OUT_FAILURE)));

        return PCollectionTuple.of(TRANSFORM_OUT_SUCCESS, extractTheme.get(TRANSFORM_OUT_SUCCESS))
                .and(TRANSFORM_OUT_FAILURE, extractTheme.get(TRANSFORM_OUT_FAILURE));
    }
}
