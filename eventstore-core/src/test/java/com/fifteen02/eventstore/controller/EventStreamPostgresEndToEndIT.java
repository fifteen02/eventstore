package com.fifteen02.eventstore.controller;

import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import com.fifteen02.eventstore.service.GoogleBigQueryClient;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "spring.jpa.hibernate.ddl-auto=create-drop",
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
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class EventStreamPostgresEndToEndIT {

    private static final String DEV_BEARER_TOKEN = "dev-eventstore-token";

    @Container
    private static final PostgreSQLContainer<?> postgresql = new PostgreSQLContainer<>("postgres:16-alpine");

    @DynamicPropertySource
    static void registerPostgresProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgresql::getJdbcUrl);
        registry.add("spring.datasource.username", postgresql::getUsername);
        registry.add("spring.datasource.password", postgresql::getPassword);
    }

    @LocalServerPort
    private int port;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersisterStateRepository persisterStateRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private GoogleBigQueryClient googleBigQueryClient;

    private HttpURLConnection connection;

    @AfterEach
    void disconnectSse() {
        if (connection != null) {
            connection.disconnect();
        }
    }

    @Test
    void postgresBackedStoreReplaysMessageThroughSse() throws Exception {
        seedRealtimePersister();
        var storedMessage = putMessage();

        try (var reader = openStream("/api/events/stream?startIndex=" + storedMessage.id())) {
            assertThat(readEvent(reader).name()).isEqualTo("connected");

            var event = readEvent(reader);
            assertThat(event.name()).isEqualTo("message");
            assertThat(event.id()).isEqualTo(Long.toString(storedMessage.id()));
            JsonNode data = objectMapper.readTree(event.data());
            JsonNode message = data.path("message");
            assertThat(message.path("messageType").asString()).isEqualTo("PostgresSmokeEvent");
            assertThat(message.path("userId").asString()).isEqualTo("postgres-user");
            assertThat(message.path("body").path("db").asString())
                    .isEqualTo("postgres");
        }
    }

    private StoredMessage putMessage() throws Exception {
        var response = restTemplate.exchange(
                "http://localhost:" + port + "/api/events/message",
                HttpMethod.PUT,
                new HttpEntity<>(Map.of(
                        "messageType", "PostgresSmokeEvent",
                        "userId", "postgres-user",
                        "timestamp", Instant.now().toString(),
                        "correlationId", "postgres-corr",
                        "content", Map.of("db", "postgres")), authHeaders()),
                String.class);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.CREATED);
        var json = objectMapper.readTree(response.getBody());
        return new StoredMessage(json.path("id").asLong());
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

    private HttpHeaders authHeaders() {
        var headers = new HttpHeaders();
        headers.setBearerAuth(DEV_BEARER_TOKEN);
        return headers;
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

    private record StoredMessage(long id) {
    }

    private record SseEvent(String id, String name, String data) {
    }
}
