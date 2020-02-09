package com.compulsivesoftware.twitterbeam.inflight;

import com.compulsivesoftware.twitterbeam.io.TwitterIO;
import com.google.auto.value.AutoValue;

import java.io.Serializable;
import java.util.Set;

@AutoValue
abstract public class StatusTheme implements Serializable {

    static public StatusTheme of(TwitterIO.StatusMsg status, Set<String> themes) {
        return new AutoValue_StatusTheme(status, themes);
    }

    abstract public TwitterIO.StatusMsg getStatusMsg();

    abstract public Set<String> getThemes();

}
