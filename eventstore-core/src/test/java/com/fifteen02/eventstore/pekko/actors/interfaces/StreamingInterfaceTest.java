package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.controller.StreamRequestParameters;
import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.to.Index;
import com.fifteen02.eventstore.to.MessageTo;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
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
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.after;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class StreamingInterfaceTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Mock
    private CatchUpPlannerService catchUpPlannerService;

    @Mock
    private SseEmitter emitter;

    @Test
    void routedMessage_isSentToEmitter() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-live-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());

        CountDownLatch emitted = new CountDownLatch(2);
        doAnswer(_ -> {
            emitted.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);
        var routedMessage = new MessageTo(123L, "ProfileUpdatedEvent", json("{\"ok\":true}"),
                null, null, null, null, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 200L), "streaming-interface");
            streamingInterface.tell(new Interface.Routed(routedMessage));
            return Behaviors.empty();
        }), "streaming-live-test-" + UUID.randomUUID());

        try {
            assertThat(emitted.await(2, TimeUnit.SECONDS)).isTrue();
            verify(emitter, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void filteredOutMessage_stillMovesTheObservedPosition() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-observed-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());
        var observed = new java.util.concurrent.atomic.AtomicLong();
        CountDownLatch seen = new CountDownLatch(1);

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);
        var otherType = new MessageTo(123L, "SomethingElse", json("{\"ok\":true}"),
                null, null, null, null, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 200L,
                    index -> {
                        observed.set(index);
                        if (index == 123L) {
                            seen.countDown();
                        }
                    }), "streaming-interface");
            streamingInterface.tell(new Interface.Routed(otherType));
            return Behaviors.empty();
        }), "streaming-observed-test-" + UUID.randomUUID());

        try {
            assertThat(seen.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(observed.get()).isEqualTo(123L);
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void heartbeat_isSentByActorTimer() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-heartbeat-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());

        CountDownLatch heartbeatEmitted = new CountDownLatch(2);
        doAnswer(_ -> {
            heartbeatEmitted.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of(), Set.of(), 100L, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofMillis(50),
                    () -> 120L), "streaming-interface");
            return Behaviors.empty();
        }), "streaming-heartbeat-test-" + UUID.randomUUID());

        try {
            assertThat(heartbeatEmitted.await(2, TimeUnit.SECONDS)).isTrue();
            verify(emitter, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void heartbeat_isSentOnWireBeforeQueuedMessagesWhileParentActorIsBlockedInCatchUp() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-heartbeat-during-catchup-test");
        CountDownLatch catchUpStarted = new CountDownLatch(1);
        CountDownLatch releaseCatchUp = new CountDownLatch(1);
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.of(
                new CatchUpPlannerService.Plan(
                        99L,
                        123L,
                        List.of(new CatchUpPlannerService.Segment(
                                "realtime",
                                100L,
                                123L,
                                () -> {
                                    catchUpStarted.countDown();
                                    try {
                                        releaseCatchUp.await(2, TimeUnit.SECONDS);
                                    } catch (InterruptedException exception) {
                                        Thread.currentThread().interrupt();
                                    }
                                    return Collections.emptyList();
                                })))));

        CountDownLatch heartbeatOnWire = new CountDownLatch(1);
        List<String> wireEventNames = new CopyOnWriteArrayList<>();
        doAnswer(invocation -> {
            var eventName = eventName(invocation.getArgument(0));
            wireEventNames.add(eventName);
            if ("heartbeat".equals(eventName)) {
                heartbeatOnWire.countDown();
            }
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of(), Set.of(), 100L, null, null, null);
        var streamingInterfaceRef = new AtomicReference<ActorRef<Interface.Command>>();

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            streamingInterfaceRef.set(ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofMillis(50),
                    () -> 120L), "streaming-interface"));
            return Behaviors.empty();
        }), "streaming-heartbeat-during-catchup-test-" + UUID.randomUUID());

        try {
            assertThat(catchUpStarted.await(2, TimeUnit.SECONDS)).isTrue();
            streamingInterfaceRef.get().tell(new Interface.Routed(messageTo(123L, "ProfileUpdatedEvent")));
            streamingInterfaceRef.get().tell(new Interface.Routed(messageTo(124L, "ProfileUpdatedEvent")));

            assertThat(heartbeatOnWire.await(300, TimeUnit.MILLISECONDS)).isTrue();
            assertThat(wireEventNames).contains("connected", "heartbeat");
            assertThat(wireEventNames).doesNotContain("message");
        } finally {
            releaseCatchUp.countDown();
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void heartbeatProgress_omitsInternalObservedIndex() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-filtered-progress-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());

        var lastPersistedIndex = new AtomicLong(150L);
        CountDownLatch statusEmitted = new CountDownLatch(2);
        doAnswer(_ -> {
            statusEmitted.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofMillis(50),
                    lastPersistedIndex::get), "streaming-interface");
            streamingInterface.tell(new Interface.Routed(messageTo(149L, "ExternalSystemEvent")));
            return Behaviors.empty();
        }), "streaming-filtered-progress-test-" + UUID.randomUUID());

        try {
            assertThat(statusEmitted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(eventDataComponentNames()).doesNotContain("lastSeenIndex", "lag");
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void messageProgress_omitsInternalObservedIndex() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-message-progress-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());

        var lastPersistedIndex = new AtomicLong(151L);
        CountDownLatch messageEmitted = new CountDownLatch(2);
        doAnswer(_ -> {
            messageEmitted.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    lastPersistedIndex::get), "streaming-interface");
            streamingInterface.tell(new Interface.Routed(messageTo(149L, "ExternalSystemEvent")));
            streamingInterface.tell(new Interface.Routed(messageTo(150L, "ProfileUpdatedEvent")));
            return Behaviors.empty();
        }), "streaming-message-progress-test-" + UUID.randomUUID());

        try {
            assertThat(messageEmitted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(eventDataComponentNames()).doesNotContain("lastSeenIndex", "lag");
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void catchUpAndLiveRouting_emitEachMessageIndexAtMostOnce() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-exactly-once-test");
        var catchUpMessage = message(123L, "ProfileUpdatedEvent");
        when(catchUpPlannerService.planFromStartInclusive(123L)).thenReturn(Optional.of(
                new CatchUpPlannerService.Plan(
                        122L,
                        124L,
                        List.of(new CatchUpPlannerService.Segment(
                                "realtime",
                                123L,
                                124L,
                                () -> List.of(catchUpMessage, message(124L, "ProfileUpdatedEvent")))))));

        AtomicInteger sendCount = new AtomicInteger(0);
        CountDownLatch emittedDistinctMessages = new CountDownLatch(4);
        doAnswer(_ -> {
            sendCount.incrementAndGet();
            emittedDistinctMessages.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 123L, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 200L), "streaming-interface");
            streamingInterface.tell(new Interface.Routed(MessageTo.from(catchUpMessage)));
            streamingInterface.tell(new Interface.Routed(new MessageTo(125L, "ProfileUpdatedEvent", json("{\"ok\":true}"),
                    null, null, null, null, null, null, null)));
            return Behaviors.empty();
        }), "streaming-exactly-once-test-" + UUID.randomUUID());

        try {
            assertThat(emittedDistinctMessages.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(sendCount).hasValue(4);
            verify(emitter, times(4)).send(any(SseEmitter.SseEventBuilder.class));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void liveRouting_emitSameMessageIndexOnlyOnceWhenEnqueuedMultipleTimes() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-repeated-live-message-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());

        AtomicInteger sendCount = new AtomicInteger(0);
        CountDownLatch emittedDistinctMessages = new CountDownLatch(4);
        doAnswer(_ -> {
            sendCount.incrementAndGet();
            emittedDistinctMessages.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);
        var repeatedMessage = messageTo(123L, "ProfileUpdatedEvent");

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 200L), "streaming-interface");
            streamingInterface.tell(new Interface.Routed(repeatedMessage));
            streamingInterface.tell(new Interface.Routed(repeatedMessage));
            streamingInterface.tell(new Interface.Routed(repeatedMessage));
            streamingInterface.tell(new Interface.Routed(messageTo(124L, "ProfileUpdatedEvent")));
            streamingInterface.tell(new Interface.Routed(messageTo(125L, "ProfileUpdatedEvent")));
            return Behaviors.empty();
        }), "streaming-repeated-live-message-test-" + UUID.randomUUID());

        try {
            assertThat(emittedDistinctMessages.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(sendCount).hasValue(4);
            verify(emitter, after(200).times(4)).send(any(SseEmitter.SseEventBuilder.class));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void liveRouting_ignoresOlderIndicesAfterNewerMessageWasEmitted() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-older-live-message-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());

        AtomicInteger sendCount = new AtomicInteger(0);
        CountDownLatch emittedDistinctMessages = new CountDownLatch(3);
        doAnswer(_ -> {
            sendCount.incrementAndGet();
            emittedDistinctMessages.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 200L), "streaming-interface");
            streamingInterface.tell(new Interface.Routed(messageTo(123L, "ProfileUpdatedEvent")));
            streamingInterface.tell(new Interface.Routed(messageTo(121L, "ProfileUpdatedEvent")));
            streamingInterface.tell(new Interface.Routed(messageTo(122L, "ProfileUpdatedEvent")));
            streamingInterface.tell(new Interface.Routed(messageTo(124L, "ProfileUpdatedEvent")));
            return Behaviors.empty();
        }), "streaming-older-live-message-test-" + UUID.randomUUID());

        try {
            assertThat(emittedDistinctMessages.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(sendCount).hasValue(3);
            verify(emitter, after(200).times(3)).send(any(SseEmitter.SseEventBuilder.class));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void externalAckDoesNotStopStreamingInterface() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-ack-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.empty());

        CountDownLatch emitted = new CountDownLatch(2);
        doAnswer(_ -> {
            emitted.countDown();
            return null;
        }).when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);
        var routedMessage = new MessageTo(123L, "ProfileUpdatedEvent", json("{\"ok\":true}"),
                null, null, null, null, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var streamingInterface = ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 200L), "streaming-interface");
            streamingInterface.tell(new Interface.ExternalAck(new Index(123L), () -> null));
            streamingInterface.tell(new Interface.Routed(routedMessage));
            return Behaviors.empty();
        }), "streaming-ack-test-" + UUID.randomUUID());

        try {
            assertThat(emitted.await(2, TimeUnit.SECONDS)).isTrue();
            verify(emitter, times(2)).send(any(SseEmitter.SseEventBuilder.class));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void boundedCatchUp_emitsStreamEndedWhenEndIndexIsReachedWithoutMessages() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-bounded-test");
        when(catchUpPlannerService.planFromStartInclusiveToEndInclusive(100L, 123L)).thenReturn(Optional.of(
                new CatchUpPlannerService.Plan(
                        99L,
                        123L,
                        List.of(new CatchUpPlannerService.Segment(
                                "realtime",
                                100L,
                                123L,
                                Collections::emptyList)))));

        CountDownLatch completed = new CountDownLatch(1);
        doAnswer(_ -> null).when(emitter).send(any(SseEmitter.SseEventBuilder.class));
        doAnswer(_ -> {
            completed.countDown();
            return null;
        }).when(emitter).complete();

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, 123L, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 150L), "streaming-interface");
            return Behaviors.empty();
        }), "streaming-bounded-test-" + UUID.randomUUID());

        try {
            assertThat(completed.await(2, TimeUnit.SECONDS)).isTrue();
            verify(emitter, times(2)).send(any(SseEmitter.SseEventBuilder.class));
            verify(emitter).complete();
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void catchUp_stopsQuietlyWhenClientDisconnectsDuringSend() throws Exception {
        var interfaceDefinition = interfaceDefinition("streaming-client-disconnect-test");
        when(catchUpPlannerService.planFromStartInclusive(100L)).thenReturn(Optional.of(
                new CatchUpPlannerService.Plan(
                        99L,
                        123L,
                        List.of(new CatchUpPlannerService.Segment(
                                "realtime",
                                100L,
                                123L,
                                () -> List.of(message(123L, "ProfileUpdatedEvent")))))));

        CountDownLatch connectedSent = new CountDownLatch(1);
        doAnswer(invocation -> {
            connectedSent.countDown();
            return null;
        }).doThrow(new IOException("Broken pipe"))
                .when(emitter).send(any(SseEmitter.SseEventBuilder.class));

        var requestParameters = new StreamRequestParameters(Set.of("ProfileUpdatedEvent"), Set.of(), 100L, null, null, null);

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            ctx.spawn(StreamingInterface.create(
                    interfaceDefinition,
                    catchUpPlannerService,
                    emitter,
                    requestParameters,
                    Duration.ofSeconds(60),
                    () -> 150L), "streaming-interface");
            return Behaviors.empty();
        }), "streaming-client-disconnect-test-" + UUID.randomUUID());

        try {
            assertThat(connectedSent.await(2, TimeUnit.SECONDS)).isTrue();
            verify(emitter, never()).completeWithError(any());
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    private InterfacesConfigProperties.InterfaceDefinition interfaceDefinition(String name) {
        var definition = new InterfacesConfigProperties.InterfaceDefinition();
        definition.setName(name);
        return definition;
    }

    private Message message(long id, String type) {
        var message = new Message();
        message.setId(id);
        message.setType(type);
        message.setContent(json("{\"ok\":true}"));
        message.setTimestamp(Instant.parse("2026-01-01T00:00:00Z"));
        return message;
    }

    private MessageTo messageTo(long id, String type) {
        return new MessageTo(id, type, json("{\"ok\":true}"),
                null, null, null, null, null, null, null);
    }

    private static JsonNode json(String value) {
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid test JSON", exception);
        }
    }

    private Set<String> eventDataComponentNames() throws Exception {
        return OBJECT_MAPPER.valueToTree(eventData())
                .propertyNames()
                .stream()
                .collect(java.util.stream.Collectors.toUnmodifiableSet());
    }

    private Object eventData() throws Exception {
        var eventCaptor = ArgumentCaptor.forClass(SseEmitter.SseEventBuilder.class);
        verify(emitter, org.mockito.Mockito.atLeastOnce()).send(eventCaptor.capture());
        return eventCaptor.getAllValues().getLast().build().stream()
                .map(ResponseBodyEmitter.DataWithMediaType::getData)
                .filter(data -> switch (data) {
                    case String _ -> false;
                    default -> true;
                })
                .reduce((_, second) -> second)
                .orElseThrow();
    }

    private String eventName(SseEmitter.SseEventBuilder eventBuilder) {
        return eventBuilder.build().stream()
                .map(ResponseBodyEmitter.DataWithMediaType::getData)
                .flatMap(data -> switch (data) {
                    case String value -> value.lines();
                    default -> java.util.stream.Stream.empty();
                })
                .filter(line -> line.startsWith("event:"))
                .map(line -> line.substring("event:".length()).trim())
                .findFirst()
                .orElse("");
    }
}
