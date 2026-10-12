package com.fifteen02.eventstore.controller;

import tools.jackson.databind.JsonNode;
import com.fifteen02.eventstore.pekko.ActorRegistry;
import com.fifteen02.eventstore.service.InvalidMessageException;
import com.fifteen02.eventstore.service.MessageIngestionService;
import com.fifteen02.eventstore.to.MessageTo;
import io.opentelemetry.instrumentation.annotations.WithSpan;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

/**
 * REST input. Both endpoints require {@code Authorization: Bearer <eventstore.auth.bearer-token>}.
 *
 * <p>Responses: {@code 201} with the stored {@link MessageTo} (its {@code id} is the global index),
 * {@code 202} when identical content was recently stored and the message was skipped, {@code 400}
 * when {@code messageType} or {@code content} is missing, a field is longer than its column or a
 * value contains a NUL character, and {@code 503} with {@code Retry-After} while EventStore is
 * still starting. See {@code docs/publishing.md}.
 */
@RestController
@RequestMapping("/api/events")
public class EventInputController {

    private final MessageIngestionService messageIngestionService;
    private final ActorRegistry actorRegistry;

    public EventInputController(MessageIngestionService messageIngestionService, ActorRegistry actorRegistry) {
        this.messageIngestionService = messageIngestionService;
        this.actorRegistry = actorRegistry;
    }

    /**
     * Store a message. {@code messageType} and {@code content} are required.
     */
    @PutMapping("/message")
    @ResponseStatus(HttpStatus.CREATED)
    @WithSpan("REST Eventstore Message Input")
    public MessageTo putMessage(@RequestBody RestMessageInput input) {
        requireReady();
        validateMessage(input);
        return ingest(
                input.messageType(),
                input.content(),
                input.userId(),
                input.timestamp(),
                input.traceParent(),
                input.traceState(),
                input.baggage(),
                input.correlationId(),
                input.metadata(),
                null,
                null);
    }

    /**
     * Store a message that belongs to an aggregate. Also requires {@code aggregateUuid} and
     * {@code aggregateIndex}; both are stored as-is and not validated for sequence.
     */
    @PutMapping("/aggregate-message")
    @ResponseStatus(HttpStatus.CREATED)
    @WithSpan("REST Eventstore Aggregate Message Input")
    public MessageTo putAggregateMessage(@RequestBody RestAggregateMessageInput input) {
        requireReady();
        validateMessage(input.messageType(), input.content());
        if (input.aggregateUuid() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "aggregateUuid is required");
        }
        if (input.aggregateIndex() == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "aggregateIndex is required");
        }
        return ingest(
                input.messageType(),
                input.content(),
                input.userId(),
                input.timestamp(),
                input.traceParent(),
                input.traceState(),
                input.baggage(),
                input.correlationId(),
                input.metadata(),
                input.aggregateUuid(),
                input.aggregateIndex());
    }

    private MessageTo ingest(String messageType,
                             JsonNode content,
                             String userId,
                             Instant timestamp,
                             String traceParent,
                             String traceState,
                             String baggage,
                             String correlationId,
                             Map<String, Object> metadata,
                             UUID aggregateUuid,
                             Long aggregateIndex) {
        MessageTo messageTo;
        try {
            messageTo = messageIngestionService.ingest(new MessageIngestionService.MessageInput(
                    messageType,
                    content,
                    userId,
                    timestamp,
                    aggregateUuid,
                    aggregateIndex,
                    traceParent,
                    traceState,
                    baggage,
                    correlationId,
                    metadata));
        } catch (InvalidMessageException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, e.getMessage());
        }
        if (messageTo == null) {
            throw new ResponseStatusException(HttpStatus.ACCEPTED, "duplicate message skipped");
        }
        return messageTo;
    }

    /**
     * A message stored before the router is up is never sent to realtime interfaces, so refuse it
     * before persisting and let the producer retry.
     */
    private void requireReady() {
        if (!actorRegistry.isReady()) {
            throw new NotReadyException();
        }
    }

    private void validateMessage(RestMessageInput input) {
        validateMessage(input.messageType(), input.content());
    }

    private void validateMessage(String messageType, JsonNode content) {
        if (messageType == null || messageType.trim().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "messageType is required");
        }
        if (content == null || content.isNull()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "content is required");
        }
    }

    private static final class NotReadyException extends ResponseStatusException {
        private static final HttpHeaders HEADERS = HttpHeaders.readOnlyHttpHeaders(retryAfterOneSecond());

        NotReadyException() {
            super(HttpStatus.SERVICE_UNAVAILABLE, "EventStore is starting; retry in a moment");
        }

        @Override
        public HttpHeaders getHeaders() {
            return HEADERS;
        }

        private static HttpHeaders retryAfterOneSecond() {
            var headers = new HttpHeaders();
            headers.set(HttpHeaders.RETRY_AFTER, "1");
            return headers;
        }
    }

    /**
     * Body of {@code PUT /api/events/message}. {@code content} becomes the stored message body;
     * {@code timestamp} defaults to the ingestion time.
     */
    public record RestMessageInput(String messageType,
                                   JsonNode content,
                                   String userId,
                                   Instant timestamp,
                                   String traceParent,
                                   String traceState,
                                   String baggage,
                                   String correlationId,
                                   Map<String, Object> metadata) {
    }

    /**
     * Body of {@code PUT /api/events/aggregate-message}: {@link RestMessageInput} plus the aggregate identity.
     */
    public record RestAggregateMessageInput(String messageType,
                                            JsonNode content,
                                            String userId,
                                            Instant timestamp,
                                            String traceParent,
                                            String traceState,
                                            String baggage,
                                            String correlationId,
                                            Map<String, Object> metadata,
                                            UUID aggregateUuid,
                                            Long aggregateIndex) {
    }
}
