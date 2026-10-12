package com.fifteen02.eventstore.amqp;

import com.fifteen02.eventstore.constants.MessageHeaders;
import com.fifteen02.eventstore.service.InvalidMessageException;
import com.fifteen02.eventstore.service.MessageIngestionService;
import com.fifteen02.eventstore.to.MessageTo;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.CannotCreateTransactionException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class EventstoreInputHandlerTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private MessageIngestionService messageIngestionService;

    @Mock
    private Channel channel;

    @Test
    void onMessage_acceptsMessageTypeOutsideKnownEnum() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(7L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "ExternalSystemEvent")
                        .setHeader(MessageHeaders.TIMESTAMP, "2026-06-03T08:15:00Z")
                        .setHeader(MessageHeaders.USER_ID, "user-1")
                        .build())
                .build();
        when(messageIngestionService.ingest(any()))
                .thenReturn(new MessageTo(42L, "ExternalSystemEvent", json("{\"ok\":true}"),
                        null, null, null, "user-1", null, null, "hash"));

        handler.onMessage(amqpMessage, channel);

        var input = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingest(input.capture());
        assertThat(input.getValue().messageType()).isEqualTo("ExternalSystemEvent");
        assertThat(input.getValue().content()).isEqualTo(json("{\"ok\":true}"));
        assertThat(input.getValue().userId()).isEqualTo("user-1");
        assertThat(input.getValue().timestamp()).isEqualTo(Instant.parse("2026-06-03T08:15:00Z"));
        verify(channel).basicAck(7L, false);
    }

    @Test
    void onMessage_mapsAggregateAndTraceHeaders() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var aggregateUuid = UUID.fromString("11111111-1111-1111-1111-111111111111");
        var metadata = Map.<String, Object>of(
                "source", "external-system-api",
                "channel", "amqp");
        var amqpMessage = MessageBuilder
                .withBody("{\"name\":\"changed\"}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(8L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "AggregateChanged")
                        .setHeader(MessageHeaders.USER_ID, "user-2")
                        .setHeader("aggregateUuid", aggregateUuid.toString())
                        .setHeader("aggregateIndex", "7")
                        .setHeader(MessageHeaders.TRACE_PARENT, "trace-parent")
                        .setHeader(MessageHeaders.TRACE_STATE, "trace-state")
                        .setHeader(MessageHeaders.BAGGAGE, "bag")
                        .setHeader(MessageHeaders.CORRELATION_ID, "corr-2")
                        .setHeader(MessageHeaders.METADATA, "{\"source\":\"external-system-api\",\"channel\":\"amqp\"}")
                        .build())
                .build();
        when(messageIngestionService.ingest(any()))
                .thenReturn(new MessageTo(43L, "AggregateChanged", json("{\"name\":\"changed\"}"),
                        "trace-parent", "trace-state", "bag", "user-2", "corr-2", null, "hash"));

        handler.onMessage(amqpMessage, channel);

        var input = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingest(input.capture());
        assertThat(input.getValue().aggregateUuid()).isEqualTo(aggregateUuid);
        assertThat(input.getValue().aggregateIndex()).isEqualTo(7L);
        assertThat(input.getValue().traceParent()).isEqualTo("trace-parent");
        assertThat(input.getValue().traceState()).isEqualTo("trace-state");
        assertThat(input.getValue().baggage()).isEqualTo("bag");
        assertThat(input.getValue().correlationId()).isEqualTo("corr-2");
        assertThat(input.getValue().metadata()).isEqualTo(metadata);
        verify(channel).basicAck(8L, false);
    }

    @Test
    void onMessage_parsesNumericTimestampAsEpochMillis() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(9L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "ExternalSystemEvent")
                        .setHeader(MessageHeaders.TIMESTAMP, "1780484100000")
                        .build())
                .build();
        when(messageIngestionService.ingest(any()))
                .thenReturn(new MessageTo(44L, "ExternalSystemEvent", json("{\"ok\":true}"),
                        null, null, null, null, null, null, "hash"));

        handler.onMessage(amqpMessage, channel);

        var input = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingest(input.capture());
        assertThat(input.getValue().timestamp()).isEqualTo(Instant.ofEpochMilli(1780484100000L));
        verify(channel).basicAck(9L, false);
    }

    @Test
    void onMessage_fallsBackToCurrentTimeForInvalidTimestamp() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(10L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "ExternalSystemEvent")
                        .setHeader(MessageHeaders.TIMESTAMP, "not-a-timestamp")
                        .build())
                .build();
        when(messageIngestionService.ingest(any()))
                .thenReturn(new MessageTo(45L, "ExternalSystemEvent", json("{\"ok\":true}"),
                        null, null, null, null, null, null, "hash"));

        var before = Instant.now();
        handler.onMessage(amqpMessage, channel);
        var after = Instant.now();

        var input = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingest(input.capture());
        assertThat(input.getValue().timestamp()).isBetween(before, after);
        verify(channel).basicAck(10L, false);
    }

    @Test
    void onMessage_acknowledgesDuplicateSkippedByIngestionService() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(11L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "ExternalSystemEvent")
                        .build())
                .build();
        when(messageIngestionService.ingest(any())).thenReturn(null);

        handler.onMessage(amqpMessage, channel);

        verify(channel).basicAck(11L, false);
    }

    @Test
    void onMessage_rejectsMissingMessageTypeWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(12L)
                        .build())
                .build();

        handler.onMessage(amqpMessage, channel);

        verify(messageIngestionService, never()).ingest(any());
        verify(channel, never()).basicAck(12L, false);
        verify(channel).basicReject(12L, false);
    }

    @Test
    void onMessage_rejectsBlankMessageTypeWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(13L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "   ")
                        .build())
                .build();

        handler.onMessage(amqpMessage, channel);

        verify(messageIngestionService, never()).ingest(any());
        verify(channel).basicReject(13L, false);
    }

    @Test
    void onMessage_rejectsBodyThatIsNotJsonWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("not json".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(14L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "ExternalSystemEvent")
                        .build())
                .build();

        handler.onMessage(amqpMessage, channel);

        verify(messageIngestionService, never()).ingest(any());
        verify(channel, never()).basicAck(14L, false);
        verify(channel).basicReject(14L, false);
    }

    @Test
    void onMessage_rejectsMetadataThatIsNotAJsonObjectWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(15L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "ExternalSystemEvent")
                        .setHeader(MessageHeaders.METADATA, "[\"not\", \"an object\"]")
                        .build())
                .build();

        handler.onMessage(amqpMessage, channel);

        verify(messageIngestionService, never()).ingest(any());
        verify(channel).basicReject(15L, false);
    }

    @Test
    void onMessage_rejectsMalformedAggregateIndexWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var amqpMessage = MessageBuilder
                .withBody("{\"ok\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(16L)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "AggregateChanged")
                        .setHeader("aggregateIndex", "seven")
                        .build())
                .build();

        handler.onMessage(amqpMessage, channel);

        verify(messageIngestionService, never()).ingest(any());
        verify(channel).basicReject(16L, false);
    }

    @Test
    void onMessage_rejectsJsonNullBodyWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);

        handler.onMessage(message(17L, "null"), channel);
        handler.onMessage(message(18L, ""), channel);

        verify(messageIngestionService, never()).ingest(any());
        verify(channel).basicReject(17L, false);
        verify(channel).basicReject(18L, false);
    }

    @Test
    void onMessage_rejectsInputThatFailsValidationWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        when(messageIngestionService.ingest(any()))
                .thenThrow(new InvalidMessageException("userId must be at most 255 characters"));

        handler.onMessage(message(19L, "{\"ok\":true}"), channel);

        verify(channel).basicReject(19L, false);
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void onMessage_rejectsDataIntegrityViolationWithoutRequeue() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        when(messageIngestionService.ingest(any()))
                .thenThrow(new DataIntegrityViolationException("value too long for type character varying(255)"));

        handler.onMessage(message(20L, "{\"ok\":true}"), channel);

        verify(channel).basicReject(20L, false);
        verify(channel, never()).basicNack(anyLong(), anyBoolean(), anyBoolean());
    }

    @Test
    void onMessage_requeuesAndRethrowsTransientFailures() throws Exception {
        var handler = new EventstoreInputHandler(messageIngestionService);
        var failure = new CannotCreateTransactionException("database unavailable");
        when(messageIngestionService.ingest(any())).thenThrow(failure);

        assertThatThrownBy(() -> handler.onMessage(message(21L, "{\"ok\":true}"), channel)).isSameAs(failure);

        verify(channel).basicNack(21L, false, true);
        verify(channel, never()).basicReject(anyLong(), anyBoolean());
        verify(channel, never()).basicAck(anyLong(), anyBoolean());
    }

    private org.springframework.amqp.core.Message message(long tag, String body) {
        return MessageBuilder
                .withBody(body.getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setDeliveryTag(tag)
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "ExternalSystemEvent")
                        .build())
                .build();
    }

    private JsonNode json(String value) throws Exception {
        return objectMapper.readTree(value);
    }
}
