package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.google.common.hash.Hashing;
import com.fifteen02.eventstore.controller.StreamRequestParameters;
import com.fifteen02.eventstore.to.MessageTo;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyEmitter;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

@ExtendWith(MockitoExtension.class)
class StreamingInterfaceMessageWriterTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mock
    private SseEmitter emitter;

    @Test
    void write_sendsMatchingMessage() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("ExternalSystemEvent"), Set.of(), 100L, null, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);
        var routed = new Interface.Routed(message(123L, "ExternalSystemEvent", null));

        writer.write(routed, new StreamProgress(150L));

        verify(emitter).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void write_sendsPersistedProgressOnly() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("ExternalSystemEvent"), Set.of(), 100L, null, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.write(
                new Interface.Routed(message(123L, "ExternalSystemEvent", null)),
                new StreamProgress(150L));

        var fieldNames = recordComponentNames(eventData());
        assertThat(eventDataLongField("lastPersistedIndex")).isEqualTo(150L);
        assertThat(fieldNames).doesNotContain("lastSeenIndex", "lag");
    }

    @Test
    void writeHeartbeat_sendsPersistedProgressOnly() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("ExternalSystemEvent"), Set.of(), 100L, null, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.writeHeartbeat(new StreamProgress(160L));

        var fieldNames = recordComponentNames(eventData());
        assertThat(eventDataLongField("lastPersistedIndex")).isEqualTo(160L);
        assertThat(fieldNames).doesNotContain("lastSeenIndex", "lag");
    }

    @Test
    void writeHeartbeat_returnsFalseWhenEmitterIsAlreadyClosed() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of(), Set.of(), 100L, null, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);
        var sendException = new IOException("stream closed");
        doThrow(sendException).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        assertThat(writer.writeHeartbeat(progress(150L))).isFalse();

        verify(emitter, never()).completeWithError(any());
    }

    @Test
    void write_matchesMessageTypeCaseInsensitively() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("externalsystemevent"), Set.of(), 100L, null, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);
        var routed = new Interface.Routed(message(123L, "ExternalSystemEvent", null));

        writer.write(routed, progress(150L));

        verify(emitter).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void write_doesNothingWhenMessageTypeDoesNotMatch() {
        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.write(new Interface.Routed(message(123L, "ExternalSystemEvent", null)), progress(150L));

        verifyNoInteractions(emitter);
    }

    @Test
    void write_doesNothingWhenUserDoesNotMatch() {
        var requestParameters = new StreamRequestParameters(
                Set.of("ProfileUpdatedEvent"),
                Set.of("user-1"),
                100L,
                null,
                null,
                null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.write(new Interface.Routed(message(123L, "ProfileUpdatedEvent", "user-9")), progress(150L));

        verifyNoInteractions(emitter);
    }

    @Test
    void write_sendsMessageWhenShardMatches() throws Exception {
        var userId = "user-1";
        int shard = shardFor(userId, 16);
        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, 16, shard);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.write(new Interface.Routed(message(123L, "ProfileUpdatedEvent", userId)), progress(150L));

        verify(emitter).send(any(SseEmitter.SseEventBuilder.class));
    }

    @Test
    void write_doesNothingWhenShardDoesNotMatch() {
        var userId = "user-1";
        int shard = (shardFor(userId, 16) + 1) % 16;
        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, 16, shard);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.write(new Interface.Routed(message(123L, "ProfileUpdatedEvent", userId)), progress(150L));

        verifyNoInteractions(emitter);
    }

    @Test
    void write_completesEmitterWhenEndIndexIsReached() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, 123L, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.write(new Interface.Routed(message(123L, "ProfileUpdatedEvent", null)), progress(150L));

        verify(emitter, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        verify(emitter).complete();
    }

    @Test
    void write_completesEmitterWhenEndIndexIsReachedByFilteredMessage() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, 123L, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.write(new Interface.Routed(message(123L, "ExternalSystemEvent", null)), progress(150L));

        verify(emitter).send(any(SseEmitter.SseEventBuilder.class));
        verify(emitter).complete();
    }

    @Test
    void completeStreamIfEndReached_sendsStreamEndedEvent() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, 123L, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.completeStreamIfEndReached(123L, progress(150L));

        verify(emitter).send(any(SseEmitter.SseEventBuilder.class));
        verify(emitter).complete();
    }

    @Test
    void write_returnsFalseWhenEmitterSendFails() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);
        var ioException = new IOException("stream closed");
        doThrow(ioException).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        assertThat(writer.write(
                new Interface.Routed(message(123L, "ProfileUpdatedEvent", null)),
                progress(123L))).isFalse();

        verify(emitter, never()).completeWithError(any());
    }

    @Test
    void write_doesNotEmitAfterStreamTerminates() throws Exception {
        var requestParameters = new StreamRequestParameters(Set.of(), Set.of(), 100L, 123L, null, null);
        var writer = new StreamingInterfaceMessageWriter(emitter, requestParameters);

        writer.completeStreamIfEndReached(123L, progress(150L));
        writer.write(new Interface.Routed(message(124L, "ProfileUpdatedEvent", null)), progress(150L));

        verify(emitter).send(any(SseEmitter.SseEventBuilder.class));
        verify(emitter).complete();
        verify(emitter, never()).completeWithError(any());
    }

    private MessageTo message(long id, String messageType, String userId) {
        return new MessageTo(id, messageType, json("{\"ok\":true}"),
                null, null, null, userId, null, null, null);
    }

    private JsonNode json(String value) {
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid test JSON", exception);
        }
    }

    private Object eventData() throws Exception {
        var eventCaptor = ArgumentCaptor.forClass(SseEmitter.SseEventBuilder.class);
        verify(emitter, org.mockito.Mockito.atLeastOnce()).send(eventCaptor.capture());
        var lastEvent = eventCaptor.getAllValues().getLast();
        return lastEvent.build().stream()
                .map(ResponseBodyEmitter.DataWithMediaType::getData)
                .filter(data -> switch (data) {
                    case String _ -> false;
                    default -> true;
                })
                .reduce((_, second) -> second)
                .orElseThrow();
    }

    private StreamProgress progress(long lastPersistedIndex) {
        return new StreamProgress(lastPersistedIndex);
    }

    private long eventDataLongField(String name) throws Exception {
        return OBJECT_MAPPER.valueToTree(eventData()).get(name).asLong();
    }

    private Set<String> recordComponentNames(Object eventData) {
        return OBJECT_MAPPER.valueToTree(eventData)
                .propertyNames()
                .stream()
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private int shardFor(String userId, int shardCount) {
        int hash = Hashing.murmur3_128()
                .hashString(userId, StandardCharsets.UTF_8)
                .asInt();
        return Math.floorMod(hash, shardCount);
    }
}
