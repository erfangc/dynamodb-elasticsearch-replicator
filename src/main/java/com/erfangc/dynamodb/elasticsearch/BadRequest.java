package com.erfangc.dynamodb.elasticsearch;

import org.elasticsearch.action.DocWriteRequest;

public class BadRequest {
    private String id;
    private DocWriteRequest.OpType opType;
    private String cause;
    private String index;
    private String timestamp;
    private String source;

    public String getId() {
        return id;
    }

    public BadRequest setId(String id) {
        this.id = id;
        return this;
    }

    public String getCause() {
        return cause;
    }

    public BadRequest setCause(String cause) {
        this.cause = cause;
        return this;
    }

    public String getIndex() {
        return index;
    }

    public BadRequest setIndex(String index) {
        this.index = index;
        return this;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public BadRequest setTimestamp(String timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public DocWriteRequest.OpType getOpType() {
        return opType;
    }

    public BadRequest setOpType(DocWriteRequest.OpType opType) {
        this.opType = opType;
        return this;
    }

    public String getSource() {
        return source;
    }

    public BadRequest setSource(String source) {
        this.source = source;
        return this;
    }
}
