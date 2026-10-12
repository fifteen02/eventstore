package com.fifteen02.eventstore.controller;

import com.fifteen02.AmqpInitializer;
import com.fifteen02.eventstore.amqp.EventstoreInputHandler;
import com.fifteen02.eventstore.entity.InterfaceStateEntity;
import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import com.fifteen02.eventstore.service.CatchUpPlannerService;
import com.fifteen02.eventstore.service.GoogleBigQueryClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doAnswer;

@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.url=jdbc:h2:mem:eventstream-e2e;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;INIT=CREATE DOMAIN IF NOT EXISTS JSONB AS JSON",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.hibernate.ddl-auto=create-drop",
                "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
                "spring.jpa.properties.hibernate.format_sql=false",
                "spring.jpa.show-sql=false",
                "eventstore.auth.bearer-token=dev-eventstore-token",
                "eventstore.amqp.enabled=false",
                "eventstore.big-query.dataset=test_dataset",
                "eventstore.big-query.table=test_table",
                "eventstore.realtime-retention.enabled=false",
                "eventstore.realtime-retention.delete-batch-size=100",
                "eventstore.realtime-retention.retention-days=7",
                "eventstore.realtime-retention.cron=-",
                "eventstore.sse.heartbeat-interval-seconds=60"
        }
)
@AutoConfigureTestRestTemplate
@EntityScan(basePackageClasses = {Message.class, PersisterStateEntity.class, InterfaceStateEntity.class})
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class EventStreamEndToEndIT {

    private static final String DEV_BEARER_TOKEN = "dev-eventstore-token";

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersisterStateRepository persisterStateRepository;

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private GoogleBigQueryClient googleBigQueryClient;

    @MockitoSpyBean
    private CatchUpPlannerService catchUpPlannerService;

    private HttpURLConnection connection;

    @AfterEach
    void disconnectSse() {
        if (connection != null) {
            connection.disconnect();
        }
    }

    @Test
    void streamReplaysExistingEventsAndKeepsPipingNewEventsThrough() throws Exception {
        seedRealtimePersister();
        var historicalOne = putMessage("HistoricalOne", "user-1", "corr-1", Map.of("sequence", 1));
        var historicalTwo = putMessage("HistoricalTwo", "user-2", "corr-2", Map.of("sequence", 2));

        try (var reader = openStream("/api/events/stream?startIndex=" + historicalOne.id())) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");

            assertMessageEvent(readEvent(reader), historicalOne, 1);
            assertMessageEvent(readEvent(reader), historicalTwo, 2);

            var liveThree = putMessage("LiveThree", "user-3", "corr-3", Map.of("sequence", 3));

            assertMessageEvent(readEvent(reader), liveThree, 3);
        }
    }

    @Test
    void rejectsUnauthenticatedPublishAndStreamRequests() throws Exception {
        var publishResponse = restTemplate.exchange(
                "http://localhost:" + port + "/api/events/message",
                HttpMethod.PUT,
                new HttpEntity<>(message("Unauthenticated", "user-1", "corr-1", Map.of("sequence", 1))),
                String.class);
        assertThat(publishResponse.getStatusCode()).isEqualTo(HttpStatus.UNAUTHORIZED);

        var uri = URI.create("http://localhost:" + port + "/api/events/stream?startIndex=1");
        var unauthenticatedConnection = (HttpURLConnection) uri.toURL().openConnection();
        try {
            unauthenticatedConnection.setRequestMethod("GET");
            unauthenticatedConnection.setRequestProperty("Accept", "text/event-stream");
            unauthenticatedConnection.setConnectTimeout(5_000);
            unauthenticatedConnection.setReadTimeout(5_000);
            assertThat(unauthenticatedConnection.getResponseCode()).isEqualTo(HttpStatus.UNAUTHORIZED.value());
        } finally {
            unauthenticatedConnection.disconnect();
        }
    }

    @Test
    void streamKeepsReplayOrderWhenLiveMessageArrivesDuringReplay() throws Exception {
        seedRealtimePersister();
        var replayCanLoad = new CountDownLatch(1);
        var replayStarted = new CountDownLatch(1);
        var historicalOne = putMessage("HistoricalOne", "user-1", "corr-1", Map.of("sequence", 1));
        var historicalTwo = putMessage("HistoricalTwo", "user-2", "corr-2", Map.of("sequence", 2));
        var expectedLiveId = historicalTwo.id() + 1;
        pauseReplayBeforeLoadingMessages(historicalOne.id(), expectedLiveId, replayStarted, replayCanLoad);

        var streamPath = "/api/events/stream?startIndex=" + historicalOne.id() + "&endIndex=" + expectedLiveId;
        try (var reader = openStream(streamPath)) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");
            assertThat(replayStarted.await(5, TimeUnit.SECONDS)).isTrue();

            var liveThree = putMessage("LiveThree", "user-3", "corr-3", Map.of("sequence", 3));
            assertThat(liveThree.id()).isEqualTo(expectedLiveId);
            replayCanLoad.countDown();

            var events = List.of(readEvent(reader), readEvent(reader), readEvent(reader));
            assertThat(events.stream().map(SseEvent::id))
                    .containsExactly(
                            Long.toString(historicalOne.id()),
                            Long.toString(historicalTwo.id()),
                            Long.toString(liveThree.id()));
            assertMessageEvent(events.get(0), historicalOne, 1);
            assertMessageEvent(events.get(1), historicalTwo, 2);
            assertMessageEvent(events.get(2), liveThree, 3);
            assertThat(readEvent(reader).name()).isEqualTo("stream-ended");
        }
    }

    @Test
    void boundedStreamReplaysRequestedRangeAndEndsWithoutWaitingForNewEvents() throws Exception {
        seedRealtimePersister();
        putMessage("HistoricalOne", "user-1", "corr-1", Map.of("sequence", 1));
        var historicalTwo = putMessage("HistoricalTwo", "user-2", "corr-2", Map.of("sequence", 2));
        var historicalThree = putMessage("HistoricalThree", "user-3", "corr-3", Map.of("sequence", 3));

        var streamPath = "/api/events/stream?startIndex=" + historicalTwo.id() + "&endIndex=" + historicalThree.id();
        try (var reader = openStream(streamPath)) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");

            assertMessageEvent(readEvent(reader), historicalTwo, 2);
            assertMessageEvent(readEvent(reader), historicalThree, 3);

            var streamEnded = readEvent(reader);
            assertThat(streamEnded.name()).isEqualTo("stream-ended");
            var data = objectMapper.readTree(streamEnded.data());
            assertThat(data.path("lastSentIndex").asLong()).isEqualTo(historicalThree.id());
            assertThat(data.path("endIndex").asLong()).isEqualTo(historicalThree.id());
        }
    }

    @Test
    void aggregateMessageEndpointPersistsMetadataAndStreamsMessage() throws Exception {
        seedRealtimePersister();
        var aggregateUuid = UUID.fromString("11111111-2222-3333-4444-555555555555");

        var aggregateMessage = putAggregateMessage(
                "AggregateChanged",
                "aggregate-user",
                "aggregate-corr",
                aggregateUuid,
                7L,
                Map.of("name", "changed"));

        assertThat(aggregateMessage.aggregateUuid()).isEqualTo(aggregateUuid.toString());
        assertThat(aggregateMessage.aggregateIndex()).isEqualTo(7L);

        var persisted = messageRepository.findById(aggregateMessage.id()).orElseThrow();
        assertThat(persisted.getAggregateUuid()).isEqualTo(aggregateUuid);
        assertThat(persisted.getAggregateIndex()).isEqualTo(7L);

        try (var reader = openStream("/api/events/stream?startIndex=" + aggregateMessage.id())) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");
            assertMessageEvent(readEvent(reader), aggregateMessage, null);
        }
    }

    @Test
    void streamAppliesMessageTypeAndUserFiltersDuringReplayAndLiveRouting() throws Exception {
        seedRealtimePersister();
        putMessage("IgnoredType", "wanted-user", "corr-1", Map.of("sequence", 1));
        putMessage("WantedType", "ignored-user", "corr-2", Map.of("sequence", 2));
        var matchingHistorical = putMessage("WantedType", "wanted-user", "corr-3", Map.of("sequence", 3));

        var streamPath = "/api/events/stream?startIndex=" + matchingHistorical.id()
                + "&messageType=wantedtype&userId=wanted-user";
        try (var reader = openStream(streamPath)) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");
            assertMessageEvent(readEvent(reader), matchingHistorical, 3);

            putMessage("WantedType", "ignored-user", "corr-4", Map.of("sequence", 4));
            var matchingLive = putMessage("WantedType", "wanted-user", "corr-5", Map.of("sequence", 5));

            assertMessageEvent(readEvent(reader), matchingLive, 5);
        }
    }

    @Test
    void bootstrapsWithoutAmqpAndStillAcceptsRestInputForSseStreaming() throws Exception {
        seedRealtimePersister();
        assertThat(applicationContext.getBeanNamesForType(EventstoreInputHandler.class)).isEmpty();
        assertThat(applicationContext.getBeanNamesForType(AmqpInitializer.class)).isEmpty();

        var restMessage = putMessage("NoAmqpMessage", "rest-user", "rest-corr", Map.of("sequence", 1));

        try (var reader = openStream("/api/events/stream?startIndex=" + restMessage.id())) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");
            assertMessageEvent(readEvent(reader), restMessage, 1);
        }
    }

    @Test
    void duplicateRestMessageIsAcceptedButNotPersistedOrStreamedAgain() throws Exception {
        seedRealtimePersister();
        var first = putMessage("DuplicateCandidate", "dup-user", "dup-corr-1", Map.of("same", true));
        long messagesAfterFirstPut = messageRepository.count();

        var duplicateResponse = putMessageResponse("DuplicateCandidate", "dup-user", "dup-corr-2", Map.of("same", true));

        assertThat(duplicateResponse.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);
        assertThat(messageRepository.count()).isEqualTo(messagesAfterFirstPut);

        try (var reader = openStream("/api/events/stream?startIndex=" + first.id() + "&endIndex=" + first.id())) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");
            assertMessageEvent(readEvent(reader), first, null);
            assertThat(readEvent(reader).name()).isEqualTo("stream-ended");
        }
    }

    private BufferedReader openStream(String pathAndQuery) throws Exception {
        var uri = URI.create("http://localhost:" + port + pathAndQuery);
        connection = (HttpURLConnection) uri.toURL().openConnection();
        connection.setRequestMethod("GET");
        connection.setRequestProperty("Accept", "text/event-stream");
        connection.setRequestProperty("Authorization", "Bearer " + DEV_BEARER_TOKEN);
        connection.setConnectTimeout(5_000);
        connection.setReadTimeout(5_000);
        assertThat(connection.getResponseCode()).isEqualTo(HttpStatus.OK.value());
        return new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8));
    }

    private StoredMessage putMessage(String messageType,
                                     String userId,
                                     String correlationId,
                                     Map<String, Object> content) throws Exception {
        var response = putMessageResponse(messageType, userId, correlationId, content);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return storedMessageFrom(response);
    }

    private ResponseEntity<String> putMessageResponse(String messageType,
                                                      String userId,
                                                      String correlationId,
                                                      Map<String, Object> content) {
        return restTemplate.exchange(
                "http://localhost:" + port + "/api/events/message",
                HttpMethod.PUT,
                new HttpEntity<>(message(messageType, userId, correlationId, content), authHeaders()),
                String.class);
    }

    private StoredMessage putAggregateMessage(String messageType,
                                              String userId,
                                              String correlationId,
                                              UUID aggregateUuid,
                                              Long aggregateIndex,
                                              Map<String, Object> content) throws Exception {
        var response = restTemplate.exchange(
                "http://localhost:" + port + "/api/events/aggregate-message",
                HttpMethod.PUT,
                new HttpEntity<>(
                        aggregateMessage(messageType, userId, correlationId, aggregateUuid, aggregateIndex, content),
                        authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        return storedMessageFrom(response);
    }

    private HttpHeaders authHeaders() {
        var headers = new HttpHeaders();
        headers.setBearerAuth(DEV_BEARER_TOKEN);
        return headers;
    }

    private StoredMessage storedMessageFrom(ResponseEntity<String> response) throws Exception {
        var json = objectMapper.readTree(response.getBody());
        return new StoredMessage(
                json.path("id").asLong(),
                json.path("messageType").asString(),
                json.path("userId").asString(),
                json.path("correlationId").asString(),
                optionalText(json.path("aggregateUuid")),
                json.path("aggregateIndex").isMissingNode() || json.path("aggregateIndex").isNull()
                        ? null
                        : json.path("aggregateIndex").asLong());
    }

    private Map<String, Object> message(String messageType,
                                        String userId,
                                        String correlationId,
                                        Map<String, Object> content) {
        return Map.of(
                "messageType", messageType,
                "userId", userId,
                "timestamp", Instant.now().toString(),
                "correlationId", correlationId,
                "content", content);
    }

    private Map<String, Object> aggregateMessage(String messageType,
                                                 String userId,
                                                 String correlationId,
                                                 UUID aggregateUuid,
                                                 Long aggregateIndex,
                                                 Map<String, Object> content) {
        return Map.of(
                "messageType", messageType,
                "userId", userId,
                "timestamp", Instant.now().toString(),
                "correlationId", correlationId,
                "aggregateUuid", aggregateUuid.toString(),
                "aggregateIndex", aggregateIndex,
                "content", content);
    }

    private void assertMessageEvent(SseEvent event, StoredMessage expected, Integer expectedSequence) throws Exception {
        assertThat(event.name()).isEqualTo("message");
        assertThat(event.id()).isEqualTo(Long.toString(expected.id()));
        JsonNode data = objectMapper.readTree(event.data());
        JsonNode message = data.path("message");
        assertThat(message.path("id").asLong()).isEqualTo(expected.id());
        assertThat(message.path("messageType").asString()).isEqualTo(expected.messageType());
        assertThat(message.path("userId").asString()).isEqualTo(expected.userId());
        assertThat(message.path("correlationId").asString()).isEqualTo(expected.correlationId());
        if (expectedSequence != null) {
            assertThat(message.path("body").path("sequence").asInt())
                    .isEqualTo(expectedSequence);
        }
        if (expected.aggregateUuid() != null) {
            assertThat(message.path("aggregateUuid").asString()).isEqualTo(expected.aggregateUuid());
            assertThat(message.path("aggregateIndex").asLong()).isEqualTo(expected.aggregateIndex());
        }
        assertThat(data.path("lastPersistedIndex").asLong()).isGreaterThanOrEqualTo(expected.id());
    }

    private void pauseReplayBeforeLoadingMessages(long startIndex,
                                                  long endIndex,
                                                  CountDownLatch replayStarted,
                                                  CountDownLatch replayCanLoad) {
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            var maybePlan = (Optional<CatchUpPlannerService.Plan>) invocation.callRealMethod();
            return maybePlan.map(plan -> new CatchUpPlannerService.Plan(
                    plan.lastAck(),
                    plan.target(),
                    plan.segments().stream()
                            .map(segment -> new CatchUpPlannerService.Segment(
                                    segment.persistorId(),
                                    segment.fromInclusive(),
                                    segment.toInclusive(),
                                    () -> {
                                        replayStarted.countDown();
                                        awaitReplayRelease(replayCanLoad);
                                        return segment.messages().get();
                                    }))
                            .toList()));
        }).when(catchUpPlannerService).planFromStartInclusiveToEndInclusive(startIndex, endIndex);
    }

    private void awaitReplayRelease(CountDownLatch replayCanLoad) {
        try {
            if (!replayCanLoad.await(5, TimeUnit.SECONDS)) {
                throw new IllegalStateException("Timed out waiting to release replay");
            }
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting to release replay", exception);
        }
    }

    private String optionalText(JsonNode node) {
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        return node.asString();
    }

    private SseEvent readEvent(BufferedReader reader) throws Exception {
        String id = null;
        String name = null;
        var data = new StringBuilder();

        String line;
        while ((line = reader.readLine()) != null) {
            if (line.isEmpty()) {
                if (name != null || !data.isEmpty()) {
                    return new SseEvent(id, name, data.toString());
                }
                continue;
            }
            if (line.startsWith("id:")) {
                id = line.substring("id:".length()).trim();
            } else if (line.startsWith("event:")) {
                name = line.substring("event:".length()).trim();
            } else if (line.startsWith("data:")) {
                if (!data.isEmpty()) {
                    data.append('\n');
                }
                data.append(line.substring("data:".length()).trim());
            }
        }
        throw new IllegalStateException("SSE stream closed before another event arrived");
    }

    private void seedRealtimePersister() {
        var realtime = new PersisterStateEntity();
        realtime.setId("writer-postgres");
        realtime.setStartIndex(1L);
        realtime.setLastIndex(0L);
        realtime.setCanRead(true);
        realtime.setCanWrite(true);
        realtime.setRealtime(true);
        realtime.setPriority(100);
        persisterStateRepository.save(realtime);
    }

    private record StoredMessage(long id,
                                 String messageType,
                                 String userId,
                                 String correlationId,
                                 String aggregateUuid,
                                 Long aggregateIndex) {
    }

    private record SseEvent(String id, String name, String data) {
    }
}
