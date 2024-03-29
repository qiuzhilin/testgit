package com.qzl.vo;

import java.sql.Timestamp;

public class Event {
    public String user; public String url; public Long timestamp;

    public Event(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() { return "Event{" +
            "user='" + user + '\'' +
            ", url='" + url + '\'' +
            ", timestamp=" + new Timestamp(timestamp) + '}';
    }

}
