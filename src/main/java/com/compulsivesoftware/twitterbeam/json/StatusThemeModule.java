package com.compulsivesoftware.twitterbeam.json;

import com.compulsivesoftware.twitterbeam.inflight.StatusTheme;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.api.client.util.DateTime;
import com.google.auto.service.AutoService;
import twitter4j.HashtagEntity;
import twitter4j.Place;
import twitter4j.Status;
import twitter4j.User;

import java.io.IOException;
import java.io.Serializable;

@AutoService(Module.class)
public class StatusThemeModule extends SimpleModule implements Serializable {
    public StatusThemeModule() {
        super();
        addSerializer(StatusTheme.class, new StatusThemeSerializer());
    }

    static public class StatusThemeSerializer extends JsonSerializer<StatusTheme> implements Serializable {

        static final long serialVersionUID = 1L;

        @Override
        public void serialize(StatusTheme statusTheme, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
            jsonGenerator.writeStartObject();
            jsonGenerator.writeArrayFieldStart("themes");
            for (String t : statusTheme.getThemes()) {
                jsonGenerator.writeString(t);
            }
            jsonGenerator.writeEndArray();

            Status status = statusTheme.getStatusMsg().getStatus();
            jsonGenerator.writeStringField("created_at", new DateTime(status.getCreatedAt()).toString());
            jsonGenerator.writeStringField("id", Long.toString(status.getId()));
            jsonGenerator.writeStringField("text", status.getText());
            jsonGenerator.writeNumberField("retweet_count", status.getRetweetCount());
            jsonGenerator.writeNumberField("favorite_count", status.getFavoriteCount());
            jsonGenerator.writeStringField("language", status.getLang());

            jsonGenerator.writeArrayFieldStart("hashtags");
            for (HashtagEntity i : status.getHashtagEntities()) {
                jsonGenerator.writeString(i.getText());
            }
            jsonGenerator.writeEndArray();

            jsonGenerator.writeObjectFieldStart("user");
            User user = status.getUser();
            jsonGenerator.writeStringField("id", Long.toString(user.getId()));
            jsonGenerator.writeStringField("created_at", new DateTime(user.getCreatedAt()).toString());
            jsonGenerator.writeStringField("screen_name", user.getScreenName());
            jsonGenerator.writeStringField("location", user.getLocation());
            jsonGenerator.writeNumberField("followers_count", user.getFollowersCount());
            jsonGenerator.writeNumberField("friends_count", user.getFriendsCount());
            jsonGenerator.writeNumberField("listed_count", user.getListedCount());
            jsonGenerator.writeEndObject();


            jsonGenerator.writeObjectFieldStart("place");
            Place place = status.getPlace();
            if (place != null) {
                jsonGenerator.writeStringField("id", place.getId());
                jsonGenerator.writeStringField("name", place.getName());
                jsonGenerator.writeStringField("full_name", place.getName());
                jsonGenerator.writeStringField("place_type", place.getPlaceType());
                jsonGenerator.writeStringField("country_code", place.getCountryCode());
                jsonGenerator.writeStringField("country", place.getCountry());
            }
            jsonGenerator.writeEndObject();
        }
    }
}
