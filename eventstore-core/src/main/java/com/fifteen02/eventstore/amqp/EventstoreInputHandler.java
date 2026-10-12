package com.fifteen02.eventstore.amqp;

import com.fifteen02.eventstore.constants.MessageHeaders;
import com.fifteen02.eventstore.service.MessageIngestionService;
import com.fifteen02.eventstore.to.MessageTo;
import com.rabbitmq.client.Channel;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Component;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Component
@ConditionalOnProperty(name = "eventstore.amqp.enabled", havingValue = "true")
public class EventstoreInputHandler {

    private static final Duration REQUEUE_DELAY = Duration.ofSeconds(1);

    private static final Logger log = LoggerFactory.getLogger(EventstoreInputHandler.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final MessageIngestionService messageIngestionService;

    public EventstoreInputHandler(MessageIngestionService messageIngestionService) {
        this.messageIngestionService = messageIngestionService;
    }

    @RabbitListener(queues = "${eventstore.input.queue}", ackMode = "MANUAL", autoStartup = "false")
    @WithSpan(value = "Eventstore Input Handler", kind = SpanKind.CONSUMER)
    public void onMessage(Message amqpMessage, Channel channel) {
        long tag = amqpMessage.getMessageProperties().getDeliveryTag();
        MessageProperties props = amqpMessage.getMessageProperties();

        String typeHeader = getHeader(props, MessageHeaders.MESSAGE_TYPE);
        String messageType = typeHeader == null ? null : typeHeader.trim();
        if (messageType == null || messageType.isEmpty()) {
            reject(channel, tag, "without a " + MessageHeaders.MESSAGE_TYPE + " header");
            return;
        }

        MessageIngestionService.MessageInput input;
        try {
            input = new MessageIngestionService.MessageInput(
                    messageType,
                    parseBody(amqpMessage.getBody()),
                    getHeader(props, MessageHeaders.USER_ID),
                    parseTimestamp(getHeader(props, MessageHeaders.TIMESTAMP)),
                    parseUuid(getHeader(props, "aggregateUuid")),
                    parseLong(getHeader(props, "aggregateIndex")),
                    getHeader(props, MessageHeaders.TRACE_PARENT),
                    getHeader(props, MessageHeaders.TRACE_STATE),
                    getHeader(props, MessageHeaders.BAGGAGE),
                    getHeader(props, MessageHeaders.CORRELATION_ID),
                    parseMetadata(getHeader(props, MessageHeaders.METADATA)));
        } catch (IllegalArgumentException e) {
            // A malformed message never becomes valid, so redelivering it would only block prefetch.
            reject(channel, tag, "of type " + messageType + ": " + e.getMessage());
            return;
        }

        MessageTo messageTo;
        try {
            messageTo = messageIngestionService.ingest(input);
        } catch (IllegalArgumentException | DataIntegrityViolationException e) {
            // Covers InvalidMessageException: the same input fails the same way on every redelivery.
            reject(channel, tag, "of type " + messageType + ": " + e.getMessage());
            return;
        } catch (RuntimeException e) {
            // Likely transient (database down): requeue it. The listener container does not nack in MANUAL
            // mode, and rethrowing lets it log the failure.
            log.error("Failed to ingest AMQP message of type {} (delivery tag {}); requeueing", messageType, tag, e);
            nackAndRequeue(channel, tag);
            throw e;
        }

        try {
            channel.basicAck(tag, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (messageTo == null) {
            log.info("Duplicate AMQP message skipped type={}", messageType);
            return;
        }
        log.debug("Persisted AMQP message id={} type={}", messageTo.id(), messageTo.messageType());

    }

    /**
     * Reject without requeue: the broker drops the message, or dead-letters it if the input queue has a
     * dead-letter policy. Throwing instead would stop the listener container.
     */
    private void reject(Channel channel, long tag, String reason) {
        log.warn("Rejecting AMQP input {} (delivery tag {})", reason, tag);
        try {
            channel.basicReject(tag, false);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void nackAndRequeue(Channel channel, long tag) {
        // Without a pause the broker redelivers at once, so a database outage becomes a hot loop that
        // floods the log. Holding the listener thread for a moment is the back-off.
        try {
            Thread.sleep(REQUEUE_DELAY.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
        try {
            channel.basicNack(tag, false, true);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private JsonNode parseBody(byte[] body) {
        JsonNode content;
        try {
            content = OBJECT_MAPPER.readTree(body);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid message body JSON", exception);
        }
        // Same rule as the REST input: an empty or JSON null body is not a message.
        if (content == null || content.isNull() || content.isMissingNode()) {
            throw new IllegalArgumentException("Message body is required");
        }
        return content;
    }

    private String getHeader(MessageProperties props, String key) {
        Object v = props.getHeaders().get(key);
        return v != null ? v.toString() : null;
    }

    private UUID parseUuid(String value) {
        return value == null || value.isBlank() ? null : UUID.fromString(value);
    }

    private Long parseLong(String value) {
        return value == null || value.isBlank() ? null : Long.parseLong(value);
    }

    private Map<String, Object> parseMetadata(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return OBJECT_MAPPER.readValue(value, new TypeReference<Map<String, Object>>() {
            });
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid metadata JSON", exception);
        }
    }

    private Instant parseTimestamp(String ts) {
        if (ts == null)
            return Instant.now();
        try {
            if (ts.matches("\\d+")) {
                return Instant.ofEpochMilli(Long.parseLong(ts));
            }
            return Instant.parse(ts);
        } catch (Exception e) {
            return Instant.now();
        }
    }
}
