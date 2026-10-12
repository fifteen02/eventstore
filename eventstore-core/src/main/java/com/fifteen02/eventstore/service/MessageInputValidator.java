package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;
import tools.jackson.databind.JsonNode;

import java.util.Collection;
import java.util.Map;

/**
 * Checks input against what the {@code messages} table accepts, so a bad message fails with a clear
 * reason instead of a database error after an id has been reserved.
 */
final class MessageInputValidator {

    private static final char NUL = '\u0000';

    private MessageInputValidator() {
    }

    static void validate(MessageIngestionService.MessageInput input) {
        if (input.messageType() == null || input.messageType().trim().isEmpty()) {
            throw new InvalidMessageException("messageType is required");
        }
        JsonNode content = input.content();
        if (content == null || content.isNull() || content.isMissingNode()) {
            throw new InvalidMessageException("content is required");
        }
        // messageType is stored trimmed, so that is the length that has to fit.
        checkString("messageType", input.messageType().trim(), Message.TYPE_MAX_LENGTH);
        checkString("userId", input.userId(), Message.USER_ID_MAX_LENGTH);
        checkString("traceParent", input.traceParent(), Message.TRACEPARENT_MAX_LENGTH);
        checkString("traceState", input.traceState(), Message.TRACESTATE_MAX_LENGTH);
        checkString("correlationId", input.correlationId(), Message.CORRELATION_ID_MAX_LENGTH);
        checkString("baggage", input.baggage(), Integer.MAX_VALUE);
        // PostgreSQL text and jsonb both refuse NUL, in values and in object keys.
        if (containsNul(content)) {
            throw new InvalidMessageException("content must not contain NUL (\\u0000) characters");
        }
        if (containsNulIn(input.metadata())) {
            throw new InvalidMessageException("metadata must not contain NUL (\\u0000) characters");
        }
    }

    private static void checkString(String field, String value, int maxLength) {
        if (value == null) {
            return;
        }
        if (value.length() > maxLength) {
            throw new InvalidMessageException(field + " must be at most " + maxLength + " characters");
        }
        if (value.indexOf(NUL) >= 0) {
            throw new InvalidMessageException(field + " must not contain NUL (\\u0000) characters");
        }
    }

    private static boolean containsNul(JsonNode node) {
        if (node.isString()) {
            return node.stringValue().indexOf(NUL) >= 0;
        }
        if (node.isObject()) {
            for (Map.Entry<String, JsonNode> property : node.properties()) {
                if (property.getKey().indexOf(NUL) >= 0 || containsNul(property.getValue())) {
                    return true;
                }
            }
            return false;
        }
        if (node.isArray()) {
            for (JsonNode element : node.values()) {
                if (containsNul(element)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean containsNulIn(Object value) {
        return switch (value) {
            case null -> false;
            case String string -> string.indexOf(NUL) >= 0;
            case JsonNode node -> containsNul(node);
            case Map<?, ?> map -> map.entrySet().stream()
                    .anyMatch(entry -> containsNulIn(entry.getKey()) || containsNulIn(entry.getValue()));
            case Collection<?> collection -> collection.stream().anyMatch(MessageInputValidator::containsNulIn);
            default -> false;
        };
    }
}
