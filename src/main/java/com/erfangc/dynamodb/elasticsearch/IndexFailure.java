package com.erfangc.dynamodb.elasticsearch;

public class IndexFailure {
    private String id;
    private String cause;
    private String index;
    private String timestamp;

    public String getId() {
        return id;
    }

    public IndexFailure setId(String id) {
        this.id = id;
        return this;
    }

    public String getCause() {
        return cause;
    }

    public IndexFailure setCause(String cause) {
        this.cause = cause;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public IndexFailure setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public IndexFailure setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }
}
