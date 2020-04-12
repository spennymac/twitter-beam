package com.compulsivesoftware.twitterbeam.io;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.SerializableCoder;
import org.apache.beam.sdk.io.UnboundedSource;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.conf.Configuration;
import twitter4j.conf.ConfigurationBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkArgument;


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


@Experimental(Experimental.Kind.SOURCE_SINK)
public class TwitterIO {
    private TwitterIO() {
    }

    public static Read read() {
        return new AutoValue_TwitterIO_Read.Builder().build();
    }

    //Wrapper for status - Coder unable to verify equuals message on Status
    public static class StatusMsg implements Serializable {
        private final Status status;

        private StatusMsg(Status status) {
            this.status = status;
        }

        public static StatusMsg of(Status status) {
            return new StatusMsg(status);
        }

        @Override
        public int hashCode() {
            return status.hashCode();
        }

        public Status getStatus() {
            return status;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof StatusMsg) {
                StatusMsg other = (StatusMsg) obj;
                return Objects.equals(status.getId(), other.getStatus().getId());
            }
            return false;
        }


    }

    private static class StreamHandler {
        private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(StreamHandler.class);

        final LinkedBlockingQueue<Status> messages = new LinkedBlockingQueue<>();
        Configuration config;
        TwitterStream stream;
        FilterQuery filterQuery;
        Read spec;

        public StreamHandler(Read spec) {
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();
            configurationBuilder
                    .setOAuthConsumerKey(spec.consumerKey())
                    .setOAuthConsumerSecret(spec.consumerSecret())
                    .setOAuthAccessToken(spec.accessToken())
                    .setOAuthAccessTokenSecret(spec.accessTokenSecret());
            this.spec = spec;
            this.filterQuery = new FilterQuery();
            filterQuery.track(spec.keywords().toArray(new String[0]));
            //TODO Add languages
            stream = new TwitterStreamFactory(configurationBuilder.build()).getInstance();
            stream.addListener(new StatusListener() {
                @Override
                public void onStatus(Status status) {
                    messages.offer(status);
                }

                @Override
                public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

                }

                @Override
                public void onTrackLimitationNotice(int i) {

                }

                @Override
                public void onScrubGeo(long l, long l1) {

                }

                @Override
                public void onStallWarning(StallWarning stallWarning) {

                }

                @Override
                public void onException(Exception e) {
                    LOG.warn(e.getMessage());
                }
            });
        }

        public LinkedBlockingQueue<Status> getQueue() {
            return this.messages;
        }

        public void start() {
            LOG.info("starting stream handler");
            stream.filter(this.filterQuery);
        }

        public void stop() {
            LOG.info("stopping stream handler");
            stream.shutdown();
        }
    }

    @AutoValue
    public abstract static class Read extends PTransform<PBegin, PCollection<StatusMsg>> {
        @Nullable
        abstract String consumerKey();

        @Nullable
        abstract String consumerSecret();

        @Nullable
        abstract String accessToken();

        @Nullable
        abstract String accessTokenSecret();

        @Nullable
        abstract ImmutableList<String> keywords();

        abstract Builder builder();

        public Read withKeywords(String consumerKey, String consumerSecret, String accessToken, String accessTokenSecret, List<String> keywords) {
            checkArgument(consumerKey != null, "consumerKey can not be null");
            checkArgument(consumerSecret != null, "consumerSecret can not be null");
            checkArgument(accessToken != null, "accessToken can not be null");
            checkArgument(accessTokenSecret != null, "accessTokenSecret can not be null");
            checkArgument(keywords.size() != 0, "keywords can not be empty");
            return builder()
                    .setConsumerKey(consumerKey)
                    .setConsumerSecret(consumerSecret)
                    .setAccessToken(accessToken)
                    .setAccessTokenSecret(accessTokenSecret)
                    .setKeywords(keywords)
                    .build();
        }

        @Override
        public PCollection<StatusMsg> expand(PBegin input) {
            org.apache.beam.sdk.io.Read.Unbounded<StatusMsg> unbounded =
                    org.apache.beam.sdk.io.Read.from(new TwitterSource(this));

            PTransform<PBegin, PCollection<StatusMsg>> transform = unbounded;
            return input.getPipeline().apply(transform);
        }

        @AutoValue.Builder
        abstract static class Builder {
            abstract Builder setConsumerKey(String consumerKey);

            abstract Builder setConsumerSecret(String consumerSecret);

            abstract Builder setAccessToken(String accessToken);

            abstract Builder setAccessTokenSecret(String accessTokenSecret);

            abstract public Builder setKeywords(List<String> keywords);

            abstract Read build();
        }
    }

    static class TwitterSource extends UnboundedSource<StatusMsg, UnboundedSource.CheckpointMark.NoopCheckpointMark> {
        final Read spec;

        TwitterSource(Read spec) {
            this.spec = spec;
        }

        @Override
        public Coder<StatusMsg> getOutputCoder() {
            return SerializableCoder.of(StatusMsg.class);
        }


        @Override
        public List<TwitterSource> split(int desiredNumSplits, PipelineOptions options) {
            // only can split once!
            List<TwitterSource> sources = new ArrayList<>();
            sources.add(this);
            return sources;
        }

        @Override
        public UnboundedReader<StatusMsg> createReader(
                PipelineOptions options, CheckpointMark.NoopCheckpointMark checkpointMark) throws IOException {
            return new UnboundedTwitterReader(this);
        }

        @Override
        public Coder<CheckpointMark.NoopCheckpointMark> getCheckpointMarkCoder() {
            return null;
        }

        @Override
        public boolean requiresDeduping() {
            return false;
        }
    }


    private static class UnboundedTwitterReader
            extends UnboundedSource.UnboundedReader<StatusMsg> {
        private final TwitterSource source;

        private Status current;
        private byte[] currentRecordId;
        private StreamHandler streamHandler;
        private LinkedBlockingQueue<Status> consumerQueue = null;
        private Instant currentTimestamp;

        UnboundedTwitterReader(TwitterSource source)
                throws IOException {
            this.source = source;
            this.current = null;
            this.streamHandler = new StreamHandler(source.spec);
        }

        @Override
        public Instant getWatermark() {
            return currentTimestamp;
        }

        @Override
        public UnboundedSource.CheckpointMark getCheckpointMark() {
            return new UnboundedSource.CheckpointMark.NoopCheckpointMark();
        }

        @Override
        public TwitterSource getCurrentSource() {
            return source;
        }

        @Override
        public byte[] getCurrentRecordId() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            if (currentRecordId != null) {
                return currentRecordId;
            } else {
                return "".getBytes(StandardCharsets.UTF_8);
            }
        }

        @Override
        public Instant getCurrentTimestamp() {
            if (currentTimestamp == null) {
                throw new NoSuchElementException();
            }
            return currentTimestamp;
        }

        @Override
        public StatusMsg getCurrent() {
            if (current == null) {
                throw new NoSuchElementException();
            }
            return StatusMsg.of(current);
        }

        @Override
        public boolean start() {
            consumerQueue = streamHandler.getQueue();
            streamHandler.start();
            return advance();
        }

        @Override
        public boolean advance() {
            currentTimestamp = Instant.now();
            if (consumerQueue == null) {
                return false;
            }

            current = consumerQueue.poll();
            if (current == null) {
                return false;
            }

            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(current.getId());
            currentRecordId = buffer.array();
            currentTimestamp = new Instant(current.getCreatedAt().toInstant().toEpochMilli());
            return true;
        }


        @Override
        public void close() throws IOException {
            if (streamHandler != null) {
                streamHandler.stop();
            }
        }
    }
}

