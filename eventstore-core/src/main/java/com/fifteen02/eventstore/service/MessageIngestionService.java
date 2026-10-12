package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.pekko.ActorRegistry;
import com.fifteen02.eventstore.pekko.actors.RouterActor;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.to.MessageTo;
import io.opentelemetry.api.trace.Span;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionOperations;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Instant;
import java.util.HexFormat;
import java.util.Map;
import java.util.UUID;

@Service
public class MessageIngestionService {

    private static final String MESSAGE_ID_ATTRIBUTE = "message.id";
    private static final String MESSAGE_TYPE_ATTRIBUTE = "message.type";
    private static final String SERVER_CORRELATION_ATTRIBUTE = "server.correlation.uuid";
    private static final String USER_ID_ATTRIBUTE = "user.id";

    private final MessageRepository messageRepository;
    private final ActorRegistry actorRegistry;
    private final IndexService indexService;
    private final DuplicateDetectionService duplicateDetectionService;
    private final TransactionOperations transactionOperations;
    private final ObjectMapper objectMapper;

    @Autowired
    public MessageIngestionService(MessageRepository messageRepository,
                                   ActorRegistry actorRegistry,
                                   IndexService indexService,
                                   DuplicateDetectionService duplicateDetectionService,
                                   TransactionOperations transactionOperations,
                                   ObjectMapper objectMapper) {
        this.messageRepository = messageRepository;
        this.actorRegistry = actorRegistry;
        this.indexService = indexService;
        this.duplicateDetectionService = duplicateDetectionService;
        this.transactionOperations = transactionOperations;
        this.objectMapper = objectMapper;
    }

    MessageIngestionService(MessageRepository messageRepository,
                            ActorRegistry actorRegistry,
                            IndexService indexService,
                            DuplicateDetectionService duplicateDetectionService) {
        this(messageRepository, actorRegistry, indexService, duplicateDetectionService,
                TransactionOperations.withoutTransaction(), new ObjectMapper());
    }

    /**
     * @throws InvalidMessageException when the input can never be stored
     */
    public synchronized MessageTo ingest(MessageInput input) {
        // Keep reservation, commit visibility, duplicate cache updates, and live routing in one
        // single-instance critical section so SSE replay never sees an id before its row is committed.
        MessageInputValidator.validate(input);
        var contentHash = contentHash(input.content());
        if (duplicateDetectionService.isDuplicate(contentHash)) {
            return null;
        }

        var messageTo = store(input, contentHash);
        duplicateDetectionService.recordProcessedHash(contentHash);
        duplicateDetectionService.incrementMessageCount();
        return messageTo;
    }

    /**
     * Stores a message EventStore writes itself, such as the {@code StartMarker}, under the same lock and
     * id sequence as client input so the two can never share an id. It has no content hash, so it is
     * never treated as a duplicate and never makes a client message look like one.
     */
    public synchronized MessageTo ingestSystemMessage(MessageInput input) {
        MessageInputValidator.validate(input);
        return store(input, null);
    }

    private MessageTo store(MessageInput input, String contentHash) {
        var messageTo = transactionOperations.execute(_ -> {
            var message = buildMessage(input, contentHash, indexService.nextCandidate().value());
            return MessageTo.from(messageRepository.save(message));
        });
        if (messageTo == null) {
            throw new IllegalStateException("Message transaction completed without a result");
        }

        indexService.markCommitted(messageTo.id());
        addSpanAttributes(messageTo);
        actorRegistry.tellRouter(new RouterActor.Route(messageTo));
        return messageTo;
    }

    private Message buildMessage(MessageInput input, String contentHash, long messageId) {
        var message = new Message();
        message.setId(messageId);
        message.setType(input.messageType().trim());
        message.setContent(input.content());
        message.setContentHash(contentHash);
        message.setTimestamp(input.timestamp() == null ? Instant.now() : input.timestamp());
        message.setUserId(input.userId());
        message.setAggregateUuid(input.aggregateUuid());
        message.setAggregateIndex(input.aggregateIndex());
        message.setTraceparent(input.traceParent());
        message.setTracestate(input.traceState());
        message.setBaggage(input.baggage());
        message.setCorrelationId(input.correlationId());
        message.setMetadata(input.metadata());
        return message;
    }

    private void addSpanAttributes(MessageTo message) {
        var span = Span.current();
        span.setAttribute(MESSAGE_ID_ATTRIBUTE, message.id());
        setAttribute(span, MESSAGE_TYPE_ATTRIBUTE, message.messageType());
        setAttribute(span, USER_ID_ATTRIBUTE, message.userId());
        span.setAttribute(SERVER_CORRELATION_ATTRIBUTE, "eventstore");
    }

    private void setAttribute(Span span, String attribute, String value) {
        if (value != null) {
            span.setAttribute(attribute, value);
        }
    }

    private String contentHash(JsonNode content) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(objectMapper.writeValueAsBytes(content));
            return HexFormat.of().formatHex(hash);
        } catch (Exception e) {
            return null;
        }
    }

    public record MessageInput(String messageType,
                               JsonNode content,
                               String userId,
                               Instant timestamp,
                               UUID aggregateUuid,
                               Long aggregateIndex,
                               String traceParent,
                               String traceState,
                               String baggage,
                               String correlationId,
                               Map<String, Object> metadata) {
        public MessageInput(String messageType,
                            JsonNode content,
                            String userId,
                            Instant timestamp,
                            UUID aggregateUuid,
                            Long aggregateIndex,
                            String traceParent,
                            String traceState,
                            String baggage,
                            String correlationId) {
            this(messageType, content, userId, timestamp, aggregateUuid, aggregateIndex,
                    traceParent, traceState, baggage, correlationId, null);
        }
    }
}
