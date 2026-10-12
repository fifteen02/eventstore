package com.fifteen02.eventstore.controller;

import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.JsonNode;
import com.fifteen02.eventstore.pekko.ActorRegistry;
import com.fifteen02.eventstore.service.InvalidMessageException;
import com.fifteen02.eventstore.service.MessageIngestionService;
import com.fifteen02.eventstore.to.MessageTo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(MockitoExtension.class)
class EventInputControllerTest {

    private MockMvc mockMvc;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private MessageIngestionService messageIngestionService;

    @Mock
    private ActorRegistry actorRegistry;

    @BeforeEach
    void setUp() {
        lenient().when(actorRegistry.isReady()).thenReturn(true);
        mockMvc = MockMvcBuilders.standaloneSetup(new EventInputController(messageIngestionService, actorRegistry))
                .build();
    }

    @Test
    void putMessage_returnsServiceUnavailableWithRetryAfterBeforeRouterIsReady() throws Exception {
        when(actorRegistry.isReady()).thenReturn(false);

        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"messageType": "ExternalSystemEvent", "content": {"ok": true}}
                                """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"));

        verify(messageIngestionService, never()).ingest(any());
    }

    @Test
    void putAggregateMessage_returnsServiceUnavailableWithRetryAfterBeforeRouterIsReady() throws Exception {
        when(actorRegistry.isReady()).thenReturn(false);

        mockMvc.perform(put("/api/events/aggregate-message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "AggregateChanged",
                                  "aggregateUuid": "11111111-1111-1111-1111-111111111111",
                                  "aggregateIndex": 7,
                                  "content": {"name": "changed"}
                                }
                                """))
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"));

        verify(messageIngestionService, never()).ingest(any());
    }

    @Test
    void putMessage_mapsInvalidMessageToBadRequestWithTheReason() throws Exception {
        when(messageIngestionService.ingest(any()))
                .thenThrow(new InvalidMessageException("userId must be at most 255 characters"));

        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"messageType": "ExternalSystemEvent", "content": {"ok": true}}
                                """))
                .andExpect(status().isBadRequest())
                .andExpect(status().reason("userId must be at most 255 characters"));
    }

    @Test
    void putAggregateMessage_mapsInvalidMessageToBadRequest() throws Exception {
        when(messageIngestionService.ingest(any()))
                .thenThrow(new InvalidMessageException("content must not contain NUL (\\u0000) characters"));

        mockMvc.perform(put("/api/events/aggregate-message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "AggregateChanged",
                                  "aggregateUuid": "11111111-1111-1111-1111-111111111111",
                                  "aggregateIndex": 7,
                                  "content": {"name": "changed"}
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void putMessage_ingestsPlainMessage() throws Exception {
        var metadata = Map.<String, Object>of(
                "source", "external-system-api",
                "channel", "web");
        when(messageIngestionService.ingest(any()))
                .thenReturn(new MessageTo(42L, "ExternalSystemEvent", json("{\"ok\":true}"),
                        null, null, null, "user-1", "corr-1", Instant.parse("2026-06-03T08:15:00Z"), "hash",
                        metadata));

        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "ExternalSystemEvent",
                                  "userId": "user-1",
                                  "timestamp": "2026-06-03T08:15:00Z",
                                  "correlationId": "corr-1",
                                  "metadata": {"source": "external-system-api", "channel": "web"},
                                  "content": {"ok": true}
                                }
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(42))
                .andExpect(jsonPath("$.messageType").value("ExternalSystemEvent"))
                .andExpect(jsonPath("$.metadata.source").value("external-system-api"))
                .andExpect(jsonPath("$.metadata.channel").value("web"));

        var requestCaptor = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingest(requestCaptor.capture());
        assertThat(requestCaptor.getValue().messageType()).isEqualTo("ExternalSystemEvent");
        assertThat(requestCaptor.getValue().userId()).isEqualTo("user-1");
        assertThat(requestCaptor.getValue().content()).isEqualTo(json("{\"ok\":true}"));
        assertThat(requestCaptor.getValue().metadata()).isEqualTo(metadata);
        assertThat(requestCaptor.getValue().aggregateUuid()).isNull();
        assertThat(requestCaptor.getValue().aggregateIndex()).isNull();
    }

    @Test
    void putAggregateMessage_ingestsAggregateMetadata() throws Exception {
        var aggregateUuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
        when(messageIngestionService.ingest(any()))
                .thenReturn(new MessageTo(43L, "AggregateChanged", json("{\"name\":\"changed\"}"),
                        null, null, null, "user-2", null, Instant.parse("2026-06-03T08:16:00Z"), "hash"));

        mockMvc.perform(put("/api/events/aggregate-message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "AggregateChanged",
                                  "userId": "user-2",
                                  "timestamp": "2026-06-03T08:16:00Z",
                                  "aggregateUuid": "11111111-1111-1111-1111-111111111111",
                                  "aggregateIndex": 7,
                                  "content": {"name": "changed"}
                                }
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(43))
                .andExpect(jsonPath("$.messageType").value("AggregateChanged"));

        var requestCaptor = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingest(requestCaptor.capture());
        assertThat(requestCaptor.getValue().aggregateUuid()).isEqualTo(aggregateUuid);
        assertThat(requestCaptor.getValue().aggregateIndex()).isEqualTo(7L);
        assertThat(requestCaptor.getValue().content()).isEqualTo(json("{\"name\":\"changed\"}"));
    }

    @Test
    void putMessage_rejectsMissingMessageType() throws Exception {
        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"content": {"ok": true}}
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void putMessage_rejectsBlankMessageType() throws Exception {
        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "   ",
                                  "content": {"ok": true}
                                }
                                """))
                .andExpect(status().isBadRequest());

        verify(messageIngestionService, never()).ingest(any());
    }

    @Test
    void putMessage_rejectsMissingContent() throws Exception {
        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"messageType": "ExternalSystemEvent"}
                                """))
                .andExpect(status().isBadRequest());

        verify(messageIngestionService, never()).ingest(any());
    }

    @Test
    void putMessage_rejectsNullContent() throws Exception {
        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "ExternalSystemEvent",
                                  "content": null
                                }
                                """))
                .andExpect(status().isBadRequest());

        verify(messageIngestionService, never()).ingest(any());
    }

    @Test
    void putMessage_acceptsArrayContentAndTraceMetadata() throws Exception {
        when(messageIngestionService.ingest(any()))
                .thenReturn(new MessageTo(44L, "BatchImported", json("[{\"id\":1},{\"id\":2}]"),
                        "trace-parent", "trace-state", "bag", "user-3", "corr-3",
                        Instant.parse("2026-06-03T08:17:00Z"), "hash"));

        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "BatchImported",
                                  "userId": "user-3",
                                  "timestamp": "2026-06-03T08:17:00Z",
                                  "traceParent": "trace-parent",
                                  "traceState": "trace-state",
                                  "baggage": "bag",
                                  "correlationId": "corr-3",
                                  "content": [{"id": 1}, {"id": 2}]
                                }
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").value(44));

        var requestCaptor = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingest(requestCaptor.capture());
        assertThat(requestCaptor.getValue().content()).isEqualTo(json("[{\"id\":1},{\"id\":2}]"));
        assertThat(requestCaptor.getValue().traceParent()).isEqualTo("trace-parent");
        assertThat(requestCaptor.getValue().traceState()).isEqualTo("trace-state");
        assertThat(requestCaptor.getValue().baggage()).isEqualTo("bag");
        assertThat(requestCaptor.getValue().correlationId()).isEqualTo("corr-3");
    }

    @Test
    void putMessage_returnsAcceptedWhenDuplicateSkipped() throws Exception {
        when(messageIngestionService.ingest(any())).thenReturn(null);

        mockMvc.perform(put("/api/events/message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "ExternalSystemEvent",
                                  "content": {"ok": true}
                                }
                                """))
                .andExpect(status().isAccepted());
    }

    @Test
    void putAggregateMessage_rejectsMissingAggregateUuid() throws Exception {
        mockMvc.perform(put("/api/events/aggregate-message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "AggregateChanged",
                                  "aggregateIndex": 7,
                                  "content": {"name": "changed"}
                                }
                                """))
                .andExpect(status().isBadRequest());
    }

    @Test
    void putAggregateMessage_rejectsMissingAggregateIndex() throws Exception {
        mockMvc.perform(put("/api/events/aggregate-message")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {
                                  "messageType": "AggregateChanged",
                                  "aggregateUuid": "11111111-1111-1111-1111-111111111111",
                                  "content": {"name": "changed"}
                                }
                                """))
                .andExpect(status().isBadRequest());

        verify(messageIngestionService, never()).ingest(any());
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
