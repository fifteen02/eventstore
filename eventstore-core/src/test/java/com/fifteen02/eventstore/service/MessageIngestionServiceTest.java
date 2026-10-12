package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.pekko.ActorRegistry;
import com.fifteen02.eventstore.pekko.actors.RouterActor;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.to.Index;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MessageIngestionServiceTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private MessageRepository messageRepository;

    @Mock
    private ActorRegistry actorRegistry;

    @Mock
    private IndexService indexService;

    @Mock
    private DuplicateDetectionService duplicateDetectionService;

    @Test
    void ingest_persistsAndRoutesMessage() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(42L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));
        var metadata = Map.<String, Object>of(
                "source", "external-system-api",
                "campaign", "spring");

        var input = new MessageIngestionService.MessageInput(
                "ExternalSystemEvent",
                json("{\"ok\":true}"),
                "user-1",
                Instant.parse("2026-06-03T08:15:00Z"),
                null,
                null,
                "traceparent",
                "tracestate",
                "baggage",
                "corr-1",
                metadata);

        var result = service.ingest(input);

        var savedMessage = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(savedMessage.capture());
        assertThat(savedMessage.getValue().getId()).isEqualTo(42L);
        assertThat(savedMessage.getValue().getType()).isEqualTo("ExternalSystemEvent");
        assertThat(savedMessage.getValue().getUserId()).isEqualTo("user-1");
        assertThat(savedMessage.getValue().getTimestamp()).isEqualTo(Instant.parse("2026-06-03T08:15:00Z"));
        assertThat(savedMessage.getValue().getTraceparent()).isEqualTo("traceparent");
        assertThat(savedMessage.getValue().getTracestate()).isEqualTo("tracestate");
        assertThat(savedMessage.getValue().getBaggage()).isEqualTo("baggage");
        assertThat(savedMessage.getValue().getCorrelationId()).isEqualTo("corr-1");
        assertThat(savedMessage.getValue().getMetadata()).isEqualTo(metadata);
        assertThat(savedMessage.getValue().getContentHash()).isNotBlank();
        assertThat(result.id()).isEqualTo(42L);
        assertThat(result.metadata()).isEqualTo(metadata);

        var route = ArgumentCaptor.forClass(RouterActor.Command.class);
        verify(actorRegistry).tellRouter(route.capture());
        assertThat(route.getValue())
                .isInstanceOfSatisfying(RouterActor.Route.class, command ->
                        assertThat(command.messageTo().metadata()).isEqualTo(metadata));
    }

    @Test
    void ingest_trimsMessageTypeBeforePersistingAndRouting() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(44L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        var result = service.ingest(new MessageIngestionService.MessageInput(
                "  ExternalSystemEvent  ",
                json("{\"ok\":true}"),
                null,
                Instant.parse("2026-06-03T08:15:00Z"),
                null,
                null,
                null,
                null,
                null,
                null));

        var savedMessage = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(savedMessage.capture());
        assertThat(savedMessage.getValue().getType()).isEqualTo("ExternalSystemEvent");
        assertThat(result.messageType()).isEqualTo("ExternalSystemEvent");
    }

    @Test
    void ingest_usesCurrentTimeWhenTimestampMissing() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(45L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        var before = Instant.now();
        service.ingest(new MessageIngestionService.MessageInput(
                "ExternalSystemEvent",
                json("{\"ok\":true}"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));
        var after = Instant.now();

        var savedMessage = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(savedMessage.capture());
        assertThat(savedMessage.getValue().getTimestamp()).isBetween(before, after);
    }

    @Test
    void ingest_usesContentHashForDuplicateDetectionBeforePersisting() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(46L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.ingest(new MessageIngestionService.MessageInput(
                "ExternalSystemEvent",
                json("{\"ok\":true}"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));

        var savedMessage = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(savedMessage.capture());
        verify(duplicateDetectionService).isDuplicate(eq(savedMessage.getValue().getContentHash()));
    }

    @Test
    void ingest_incrementsDuplicateCounterOnlyAfterSaving() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(47L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.ingest(new MessageIngestionService.MessageInput(
                "ExternalSystemEvent",
                json("{\"ok\":true}"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));

        verify(duplicateDetectionService).incrementMessageCount();
    }

    @Test
    void ingest_recordsContentHashAfterSaving() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(48L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.ingest(new MessageIngestionService.MessageInput(
                "ExternalSystemEvent",
                json("{\"ok\":true}"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));

        var savedMessage = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(savedMessage.capture());
        verify(duplicateDetectionService).recordProcessedHash(savedMessage.getValue().getContentHash());
    }

    @Test
    void ingest_persistsAggregateMetadata() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        var aggregateUuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
        when(indexService.nextCandidate()).thenReturn(new Index(43L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        service.ingest(new MessageIngestionService.MessageInput(
                "AggregateChanged",
                json("{\"name\":\"changed\"}"),
                "user-2",
                null,
                aggregateUuid,
                7L,
                null,
                null,
                null,
                null));

        var savedMessage = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository).save(savedMessage.capture());
        assertThat(savedMessage.getValue().getAggregateUuid()).isEqualTo(aggregateUuid);
        assertThat(savedMessage.getValue().getAggregateIndex()).isEqualTo(7L);
    }

    @Test
    void ingest_skipsDuplicateMessage() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(duplicateDetectionService.isDuplicate(any())).thenReturn(true);

        var result = service.ingest(new MessageIngestionService.MessageInput(
                "ExternalSystemEvent",
                json("{\"ok\":true}"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null));

        assertThat(result).isNull();
        verifyNoInteractions(messageRepository, actorRegistry, indexService);
        verify(duplicateDetectionService, never()).incrementMessageCount();
    }

    @Test
    void ingest_doesNotExposeReservedIndexBeforeSaveCompletes() throws Exception {
        when(messageRepository.findMaxId()).thenReturn(0L);
        var realIndexService = new IndexService(messageRepository);
        realIndexService.init();
        var service = new MessageIngestionService(messageRepository, actorRegistry, realIndexService, duplicateDetectionService);
        var saveStarted = new CountDownLatch(1);
        var saveCanComplete = new CountDownLatch(1);
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> {
            saveStarted.countDown();
            assertThat(saveCanComplete.await(5, TimeUnit.SECONDS)).isTrue();
            return invocation.getArgument(0);
        });

        var executor = Executors.newSingleThreadExecutor();
        try {
            var result = executor.submit(() -> service.ingest(new MessageIngestionService.MessageInput(
                    "ExternalSystemEvent",
                    json("{\"ok\":true}"),
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null)));

            assertThat(saveStarted.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(realIndexService.current().value()).isZero();

            saveCanComplete.countDown();
            assertThat(result.get(5, TimeUnit.SECONDS).id()).isEqualTo(1L);
        } finally {
            saveCanComplete.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void ingest_doesNotAdvanceCurrentOrRouteWhenSaveFails() {
        when(messageRepository.findMaxId()).thenReturn(0L);
        var realIndexService = new IndexService(messageRepository);
        realIndexService.init();
        var service = new MessageIngestionService(messageRepository, actorRegistry, realIndexService, duplicateDetectionService);
        when(messageRepository.save(any(Message.class))).thenThrow(new IllegalStateException("database unavailable"));

        assertThatThrownBy(() -> service.ingest(new MessageIngestionService.MessageInput(
                "ExternalSystemEvent",
                json("{\"ok\":true}"),
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                null)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("database unavailable");

        assertThat(realIndexService.current().value()).isZero();
        verifyNoInteractions(actorRegistry);
        verify(duplicateDetectionService, never()).recordProcessedHash(any());
        verify(duplicateDetectionService, never()).incrementMessageCount();
    }

    @Test
    void ingestSystemMessage_sharesTheIdSequenceWithClientInputAndSkipsDuplicateSuppression() {
        when(messageRepository.findMaxId()).thenReturn(0L);
        var realIndexService = new IndexService(messageRepository);
        realIndexService.init();
        var service = new MessageIngestionService(messageRepository, actorRegistry, realIndexService, duplicateDetectionService);
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        var first = service.ingest(input("ExternalSystemEvent", json("{\"ok\":true}")));
        var marker = service.ingestSystemMessage(input("StartMarker", json("{}")));
        var second = service.ingest(input("ExternalSystemEvent", json("{\"ok\":false}")));

        assertThat(List.of(first.id(), marker.id(), second.id())).containsExactly(1L, 2L, 3L);
        var saved = ArgumentCaptor.forClass(Message.class);
        verify(messageRepository, times(3)).save(saved.capture());
        // No hash: a client message with an empty body after a restart must not be deduped against the marker.
        assertThat(saved.getAllValues().get(1).getContentHash()).isNull();
        verify(duplicateDetectionService, times(2)).isDuplicate(any());
        verify(duplicateDetectionService, times(2)).recordProcessedHash(any());
        verify(duplicateDetectionService, times(2)).incrementMessageCount();
    }

    @Test
    void ingestSystemMessage_waitsForAnInFlightClientMessageInsteadOfReusingItsId() throws Exception {
        when(messageRepository.findMaxId()).thenReturn(0L);
        var realIndexService = new IndexService(messageRepository);
        realIndexService.init();
        var service = new MessageIngestionService(messageRepository, actorRegistry, realIndexService, duplicateDetectionService);
        var clientSaveStarted = new CountDownLatch(1);
        var clientSaveCanComplete = new CountDownLatch(1);
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> {
            Message message = invocation.getArgument(0);
            if (!"StartMarker".equals(message.getType())) {
                clientSaveStarted.countDown();
                assertThat(clientSaveCanComplete.await(5, TimeUnit.SECONDS)).isTrue();
            }
            return message;
        });

        var executor = Executors.newFixedThreadPool(2);
        try {
            var client = executor.submit(() -> service.ingest(input("ExternalSystemEvent", json("{\"ok\":true}"))));
            assertThat(clientSaveStarted.await(5, TimeUnit.SECONDS)).isTrue();
            var marker = executor.submit(() -> service.ingestSystemMessage(input("StartMarker", json("{}"))));

            Thread.sleep(200);
            assertThat(marker).isNotDone();

            clientSaveCanComplete.countDown();
            assertThat(client.get(5, TimeUnit.SECONDS).id()).isEqualTo(1L);
            assertThat(marker.get(5, TimeUnit.SECONDS).id()).isEqualTo(2L);
        } finally {
            clientSaveCanComplete.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void ingest_acceptsValuesAtTheColumnLimits() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(50L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        var result = service.ingest(new MessageIngestionService.MessageInput(
                "  " + "t".repeat(Message.TYPE_MAX_LENGTH) + "  ",
                json("{\"ok\":true}"),
                "u".repeat(Message.USER_ID_MAX_LENGTH),
                null,
                null,
                null,
                "p".repeat(Message.TRACEPARENT_MAX_LENGTH),
                "s".repeat(Message.TRACESTATE_MAX_LENGTH),
                null,
                "c".repeat(Message.CORRELATION_ID_MAX_LENGTH)));

        assertThat(result.id()).isEqualTo(50L);
    }

    @Test
    void ingest_rejectsValuesLongerThanTheirColumns() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        var content = json("{\"ok\":true}");

        assertInvalid(service, new MessageIngestionService.MessageInput("t".repeat(Message.TYPE_MAX_LENGTH + 1),
                content, null, null, null, null, null, null, null, null), "messageType must be at most 255");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content,
                "u".repeat(Message.USER_ID_MAX_LENGTH + 1), null, null, null, null, null, null, null), "userId must be at most 255");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content, null, null, null, null,
                "p".repeat(Message.TRACEPARENT_MAX_LENGTH + 1), null, null, null), "traceParent must be at most 256");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content, null, null, null, null,
                null, "s".repeat(Message.TRACESTATE_MAX_LENGTH + 1), null, null), "traceState must be at most 512");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content, null, null, null, null,
                null, null, null, "c".repeat(Message.CORRELATION_ID_MAX_LENGTH + 1)), "correlationId must be at most 128");
    }

    @Test
    void ingest_rejectsNulCharactersAnywhereTheDatabaseWouldRefuseThem() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        var content = json("{\"ok\":true}");

        // Inside the value: String.trim() strips a leading or trailing NUL along with whitespace.
        assertInvalid(service, input("Ty\u0000pe", content), "messageType must not contain NUL");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content, "user\u0000",
                null, null, null, null, null, null, null), "userId must not contain NUL");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content, null,
                null, null, null, null, null, "bag\u0000", null), "baggage must not contain NUL");
        assertInvalid(service, input("Type", json("{\"nested\":[{\"value\":\"a\\u0000b\"}]}")),
                "content must not contain NUL");
        assertInvalid(service, input("Type", json("{\"k\\u0000ey\":1}")), "content must not contain NUL");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content, null, null, null, null,
                null, null, null, null, Map.of("tags", List.of("ok", "bad\u0000"))), "metadata must not contain NUL");
        assertInvalid(service, new MessageIngestionService.MessageInput("Type", content, null, null, null, null,
                null, null, null, null, Map.of("bad\u0000key", "v")), "metadata must not contain NUL");
    }

    @Test
    void ingest_acceptsAnEscapedBackslashThatOnlyLooksLikeNul() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);
        when(indexService.nextCandidate()).thenReturn(new Index(51L));
        when(messageRepository.save(any(Message.class))).thenAnswer(invocation -> invocation.getArgument(0));

        // A literal backslash followed by "u0000" is six ordinary characters, not a NUL.
        var result = service.ingest(input("Type", json("{\"path\":\"C:\\\\u0000\"}")));

        assertThat(result.id()).isEqualTo(51L);
    }

    @Test
    void ingest_rejectsMissingOrNullContent() {
        var service = new MessageIngestionService(messageRepository, actorRegistry, indexService, duplicateDetectionService);

        assertInvalid(service, input("Type", null), "content is required");
        assertInvalid(service, input("Type", objectMapper.nullNode()), "content is required");
        assertInvalid(service, input("Type", objectMapper.missingNode()), "content is required");
        assertInvalid(service, input("   ", json("{}")), "messageType is required");
    }

    private void assertInvalid(MessageIngestionService service, MessageIngestionService.MessageInput input, String reason) {
        assertThatThrownBy(() -> service.ingest(input))
                .isInstanceOf(InvalidMessageException.class)
                .hasMessageContaining(reason);
        verifyNoInteractions(messageRepository, actorRegistry, indexService, duplicateDetectionService);
    }

    private MessageIngestionService.MessageInput input(String messageType, JsonNode content) {
        return new MessageIngestionService.MessageInput(messageType, content, null, null, null, null, null, null, null, null);
    }

    private JsonNode json(String value) {
        try {
            return objectMapper.readTree(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid test JSON", exception);
        }
    }
}
