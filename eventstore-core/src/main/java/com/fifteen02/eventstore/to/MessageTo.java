package com.fifteen02.eventstore.to;

import com.fifteen02.eventstore.entity.Message;
import tools.jackson.databind.JsonNode;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record MessageTo(long id, String messageType, JsonNode body, String traceParent, String traceState,
                        String baggage, String userId, String correlationId, Instant timestamp, String contentHash,
                        UUID aggregateUuid, Long aggregateIndex, Map<String, Object> metadata) {

    public MessageTo(long id, String messageType, JsonNode body, String traceParent, String traceState,
                     String baggage, String userId, String correlationId, Instant timestamp, String contentHash) {
        this(id, messageType, body, traceParent, traceState, baggage, userId, correlationId, timestamp, contentHash,
                null, null, null);
    }

    public MessageTo(long id, String messageType, JsonNode body, String traceParent, String traceState,
                     String baggage, String userId, String correlationId, Instant timestamp, String contentHash,
                     Map<String, Object> metadata) {
        this(id, messageType, body, traceParent, traceState, baggage, userId, correlationId, timestamp, contentHash,
                null, null, metadata);
    }

    public MessageTo(long id, String messageType, JsonNode body, String traceParent, String traceState,
                     String baggage, String userId, String correlationId, Instant timestamp, String contentHash,
                     UUID aggregateUuid, Long aggregateIndex) {
        this(id, messageType, body, traceParent, traceState, baggage, userId, correlationId, timestamp, contentHash,
                aggregateUuid, aggregateIndex, null);
    }

    public static MessageTo from(Message m) {
        return new MessageTo(
                m.getId(),
                m.getType(),
                m.getContent(),
                m.getTraceparent(),
                m.getTracestate(),
                m.getBaggage(),
                m.getUserId(),
                m.getCorrelationId(),
                m.getTimestamp(),
                m.getContentHash(),
                m.getAggregateUuid(),
                m.getAggregateIndex(),
                m.getMetadata());
    }
}
