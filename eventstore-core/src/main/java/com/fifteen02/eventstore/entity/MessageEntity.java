package com.fifteen02.eventstore.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Entity
@Table(name = "messages", indexes = {
        @Index(name = "idx_messages_timestamp_id", columnList = "timestamp,id")
})
public class MessageEntity {

    @Id
    @Column(name = "id")
    private Long id;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "content", nullable = false, columnDefinition = "jsonb")
    private JsonNode content;

    @Column(name = "content_hash", length = 64)
    private String contentHash;

    @Column(name = "timestamp", nullable = false)
    private Instant timestamp;

    @Column(name = "type", nullable = false)
    private String type;

    @Column(name = "user_id")
    private String userId;

    @Column(name = "aggregate_uuid")
    private UUID aggregateUuid;

    @Column(name = "aggregate_index")
    private Long aggregateIndex;

    // OpenTelemetry W3C context
    @Column(name = "traceparent", length = 256)
    private String traceparent;

    @Column(name = "tracestate", length = 512)
    private String tracestate;

    @Column(name = "baggage", columnDefinition = "TEXT")
    private String baggage;

    @JdbcTypeCode(SqlTypes.JSON)
    @Column(name = "metadata")
    private Map<String, Object> metadata;

    public Long getId() {
        return id;
    }

    public void setId(Long globalIndex) {
        this.id = globalIndex;
    }

    public JsonNode getContent() {
        return content;
    }

    public void setContent(JsonNode content) {
        this.content = content;
    }

    public String getContentHash() {
        return contentHash;
    }

    public void setContentHash(String contentHash) {
        this.contentHash = contentHash;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public UUID getAggregateUuid() {
        return aggregateUuid;
    }

    public void setAggregateUuid(UUID aggregateUuid) {
        this.aggregateUuid = aggregateUuid;
    }

    public Long getAggregateIndex() {
        return aggregateIndex;
    }

    public void setAggregateIndex(Long aggregateIndex) {
        this.aggregateIndex = aggregateIndex;
    }

    public String getTraceparent() {
        return traceparent;
    }

    public void setTraceparent(String traceparent) {
        this.traceparent = traceparent;
    }

    public String getTracestate() {
        return tracestate;
    }

    public void setTracestate(String tracestate) {
        this.tracestate = tracestate;
    }

    public String getBaggage() {
        return baggage;
    }

    public void setBaggage(String baggage) {
        this.baggage = baggage;
    }

    public Map<String, Object> getMetadata() {
        return metadata;
    }

    public void setMetadata(Map<String, Object> metadata) {
        this.metadata = metadata;
    }
}
