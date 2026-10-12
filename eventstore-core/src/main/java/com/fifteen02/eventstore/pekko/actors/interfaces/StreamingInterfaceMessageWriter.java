package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.fifteen02.eventstore.controller.StreamRequestParameters;
import com.fifteen02.eventstore.to.MessageTo;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

public class StreamingInterfaceMessageWriter {

    private static final HashFunction SHARD_HASH_FUNCTION = Hashing.murmur3_128();

    private final SseEmitter emitter;
    private final StreamRequestParameters requestParameters;
    private final Long startIndex;
    private final Long endIndex;
    private final Set<String> requestedMessageTypes;
    private final Set<String> requestedUserIds;
    private final Integer shardCount;
    private final Integer shardIndex;
    private final Object emissionLock = new Object();

    private volatile boolean streamTerminated;

    public StreamingInterfaceMessageWriter(SseEmitter emitter,
                                           StreamRequestParameters requestParameters) {
        this.emitter = emitter;
        this.requestParameters = requestParameters;
        this.startIndex = requestParameters.startIndex();
        this.endIndex = requestParameters.endIndex();
        this.requestedMessageTypes = normaliseMessageTypes(requestParameters.messageType());
        this.requestedUserIds = requestParameters.userIds() == null
                ? Set.of()
                : requestParameters.userIds();
        this.shardCount = requestParameters.shardCount();
        this.shardIndex = requestParameters.shardIndex();
    }

    public boolean writeConnected(StreamProgress progress) {
        try {
            sendConnected(progress);
            return true;
        } catch (IOException exception) {
            markStreamTerminated();
            return false;
        } catch (Exception exception) {
            completeWithErrorSafely(exception);
            throw new IllegalStateException("Failed to publish SSE connected event", exception);
        }
    }

    public boolean write(Interface.Routed routed, StreamProgress progress) {
        var messageTo = routed.messageTo();
        if (streamTerminated || isBeforeStartIndex(messageTo)) {
            return true;
        }
        if (isAfterEndIndex(messageTo)) {
            return completeStreamIfEndReached(messageTo.id(), progress);
        }

        try {
            if (isRequestedMessageType(messageTo) && isRequestedUser(messageTo) && isRequestedShard(messageTo)) {
                sendMessage(messageTo, progress);
            }
            if (shouldCompleteAfterMessage(messageTo)) {
                sendStreamEnded(messageTo.id(), progress.lastPersistedIndex());
                terminateStream();
            }
            return true;
        } catch (IOException exception) {
            markStreamTerminated();
            return false;
        } catch (Exception exception) {
            completeWithErrorSafely(exception);
            throw new IllegalStateException(
                    "Failed to publish SSE message id=" + messageTo.id() + " type=" + messageTo.messageType(),
                    exception);
        }
    }

    public boolean completeStreamIfEndReached(Long reachedIndex, StreamProgress progress) {
        if (reachedIndex == null || endIndex == null || reachedIndex < endIndex || streamTerminated) {
            return true;
        }

        try {
            sendStreamEnded(reachedIndex, progress.lastPersistedIndex());
            terminateStream();
            return true;
        } catch (IOException exception) {
            markStreamTerminated();
            return false;
        } catch (Exception exception) {
            completeWithErrorSafely(exception);
            throw new IllegalStateException(
                    "Failed to publish SSE stream-ended event for endIndex=" + endIndex,
                    exception);
        }
    }

    public boolean writeHeartbeat(StreamProgress progress) {
        try {
            sendHeartbeat(progress);
            return true;
        } catch (IOException exception) {
            markStreamTerminated();
            return false;
        } catch (Exception exception) {
            completeWithErrorSafely(exception);
            throw new IllegalStateException("Failed to publish SSE heartbeat event", exception);
        }
    }

    public void completeWithError(Exception exception) {
        synchronized (emissionLock) {
            if (streamTerminated) {
                return;
            }
            streamTerminated = true;
            emitter.completeWithError(exception);
        }
    }

    private void markStreamTerminated() {
        synchronized (emissionLock) {
            streamTerminated = true;
        }
    }

    private void completeWithErrorSafely(Exception exception) {
        try {
            emitter.completeWithError(exception);
        } catch (RuntimeException ignored) {
            // The servlet container may already have recycled the async request.
        }
    }

    private boolean isBeforeStartIndex(MessageTo messageTo) {
        return startIndex != null && messageTo.id() < startIndex;
    }

    private boolean isAfterEndIndex(MessageTo messageTo) {
        return endIndex != null && messageTo.id() > endIndex;
    }

    private boolean shouldCompleteAfterMessage(MessageTo messageTo) {
        return endIndex != null && messageTo.id() >= endIndex;
    }

    private boolean isRequestedMessageType(MessageTo messageTo) {
        if (requestedMessageTypes.isEmpty()) {
            return true;
        }
        return requestedMessageTypes.contains(normaliseMessageType(messageTo.messageType()));
    }

    private boolean isRequestedUser(MessageTo messageTo) {
        if (requestedUserIds.isEmpty()) {
            return true;
        }
        if (messageTo.userId() == null) {
            return false;
        }
        return requestedUserIds.contains(messageTo.userId());
    }

    private boolean isRequestedShard(MessageTo messageTo) {
        if (shardCount == null && shardIndex == null) {
            return true;
        }
        if (shardCount == null || shardIndex == null) {
            return false;
        }
        return Math.floorMod(shardHash(messageTo), shardCount) == shardIndex;
    }

    private int shardHash(MessageTo messageTo) {
        String key = normaliseShardKeyPart(messageTo.userId());
        return SHARD_HASH_FUNCTION.hashString(key, StandardCharsets.UTF_8).asInt();
    }

    private String normaliseShardKeyPart(String value) {
        return value == null ? "" : value;
    }

    private Set<String> normaliseMessageTypes(Set<String> messageTypes) {
        if (messageTypes == null || messageTypes.isEmpty()) {
            return Set.of();
        }
        return messageTypes.stream()
                .map(this::normaliseMessageType)
                .collect(Collectors.toUnmodifiableSet());
    }

    private String normaliseMessageType(String value) {
        if (value == null) {
            return "";
        }
        return value.trim().toLowerCase(Locale.ROOT);
    }

    private void sendMessage(MessageTo messageTo, StreamProgress progress) throws Exception {
        synchronized (emissionLock) {
            if (streamTerminated) {
                return;
            }
            emitter.send(SseEmitter.event()
                    .id(Long.toString(messageTo.id()))
                    .name("message")
                    .data(new SseStreamMessage(
                            messageTo,
                            progress.lastPersistedIndex())));
        }
    }

    private void sendConnected(StreamProgress progress) throws Exception {
        synchronized (emissionLock) {
            if (streamTerminated) {
                return;
            }
            emitter.send(SseEmitter.event()
                    .name("connected")
                    .data(new StreamStatusEvent(
                            "connected at " + Instant.now(),
                            requestParameters,
                            progress.lastPersistedIndex())));
        }
    }

    private void sendHeartbeat(StreamProgress progress) throws Exception {
        synchronized (emissionLock) {
            if (streamTerminated) {
                return;
            }
            emitter.send(SseEmitter.event()
                    .name("heartbeat")
                    .data(new StreamStatusEvent(
                            ">> " + Instant.now(),
                            requestParameters,
                            progress.lastPersistedIndex())));
        }
    }

    private void sendStreamEnded(long lastSentIndex, long lastPersistedIndex) throws Exception {
        synchronized (emissionLock) {
            if (streamTerminated) {
                return;
            }
            emitter.send(SseEmitter.event()
                    .id(Long.toString(lastSentIndex))
                    .name("stream-ended")
                    .data(new StreamEndedEvent(
                            "endIndex reached",
                            lastSentIndex,
                            endIndex,
                            lastPersistedIndex)));
        }
    }

    private void terminateStream() {
        synchronized (emissionLock) {
            streamTerminated = true;
            emitter.complete();
        }
    }

    private record SseStreamMessage(MessageTo message, long lastPersistedIndex) {
    }

    private record StreamStatusEvent(String message,
                                     StreamRequestParameters filters,
                                     long lastPersistedIndex) {
    }

    private record StreamEndedEvent(String reason,
                                    long lastSentIndex,
                                    Long endIndex,
                                    long lastPersistedIndex) {
    }
}
