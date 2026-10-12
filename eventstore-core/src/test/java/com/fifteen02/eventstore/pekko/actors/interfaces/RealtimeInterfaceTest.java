package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.constants.MessageHeaders;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.to.MessageTo;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanContext;
import io.opentelemetry.api.trace.TraceFlags;
import io.opentelemetry.api.trace.TraceState;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import org.apache.pekko.actor.typed.ActorSystem;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RealtimeInterfaceTest {

    @Mock
    private InterfaceStateService interfaceStateService;

    @Mock
    private RabbitTemplate rabbitTemplate;

    @Test
    void routedMessage_serializesMetadataWithSharedObjectMapper() throws Exception {
        var interfaceDefinition = interfaceDefinition("shared-mapper-test");
        var sharedObjectMapper = new TrackingObjectMapper();
        var metadata = Map.<String, Object>of("source", "realtime-test");
        var routedMessage = new MessageTo(123L, "ProfileUpdatedEvent", json(sharedObjectMapper, "{\"ok\":true}"),
                null, null, null, null, null, null, null, metadata);
        var sent = new CountDownLatch(1);

        when(interfaceStateService.isRunning(interfaceDefinition.getName())).thenReturn(true);
        doAnswer(_ -> {
            sent.countDown();
            return null;
        }).when(rabbitTemplate).send(eq(""), eq("shared-mapper-test.sink"), any(Message.class));

        ActorSystem<Void> system = ActorSystem.create(Behaviors.setup(ctx -> {
            var realtimeInterface = ctx.spawn(RealtimeInterface.create(
                    "server-correlation",
                    interfaceDefinition,
                    interfaceStateService,
                    rabbitTemplate,
                    sharedObjectMapper), "realtime-interface");
            realtimeInterface.tell(new Interface.Routed(routedMessage));
            return Behaviors.empty();
        }), "realtime-shared-mapper-test-" + UUID.randomUUID());

        try {
            assertThat(sent.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(sharedObjectMapper.lastSerializedValue()).isSameAs(metadata);

            var message = ArgumentCaptor.forClass(Message.class);
            verify(rabbitTemplate).send(eq(""), eq("shared-mapper-test.sink"), message.capture());
            String metadataHeader = message.getValue().getMessageProperties().getHeader(MessageHeaders.METADATA);
            assertThat(metadataHeader)
                    .isEqualTo("{\"source\":\"realtime-test\"}");
            assertThat(sharedObjectMapper.readTree(message.getValue().getBody()))
                    .isEqualTo(json(sharedObjectMapper, "{\"ok\":true}"));
        } finally {
            system.terminate();
            system.getWhenTerminated().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void traceHeaders_useTheDeliverySpanContextWhenOneIsActive() {
        var deliverySpan = SpanContext.create("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331",
                TraceFlags.getSampled(), TraceState.builder().put("vendor", "delivery").build());
        Map<String, String> carrier = new HashMap<>();
        W3CTraceContextPropagator.getInstance().inject(Context.root().with(Span.wrap(deliverySpan)), carrier, Map::put);

        var headers = RealtimeInterface.traceHeaders(carrier, storedMessage("stored=state"));

        assertThat(headers.traceParent()).isEqualTo("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        assertThat(headers.traceState()).isEqualTo("vendor=delivery");
    }

    @Test
    void traceHeaders_neverPairTheDeliveryTraceparentWithTheStoredTracestate() {
        var deliverySpan = SpanContext.create("0af7651916cd43dd8448eb211c80319c", "b7ad6b7169203331",
                TraceFlags.getSampled(), TraceState.getDefault());
        Map<String, String> carrier = new HashMap<>();
        W3CTraceContextPropagator.getInstance().inject(Context.root().with(Span.wrap(deliverySpan)), carrier, Map::put);

        var headers = RealtimeInterface.traceHeaders(carrier, storedMessage("stored=state"));

        assertThat(headers.traceParent()).isEqualTo("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
        assertThat(headers.traceState()).isNull();
    }

    @Test
    void traceHeaders_fallBackToTheStoredContextWithoutAnActiveSpan() {
        var headers = RealtimeInterface.traceHeaders(new HashMap<>(), storedMessage("stored=state"));

        assertThat(headers.traceParent()).isEqualTo(STORED_TRACEPARENT);
        assertThat(headers.traceState()).isEqualTo("stored=state");
    }

    private static final String STORED_TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    private MessageTo storedMessage(String traceState) {
        return new MessageTo(1L, "OrderShipped", new ObjectMapper().createObjectNode(), STORED_TRACEPARENT, traceState,
                null, null, null, null, null);
    }

    private JsonNode json(ObjectMapper objectMapper, String value) throws Exception {
        return objectMapper.readTree(value);
    }

    private InterfacesConfigProperties.InterfaceDefinition interfaceDefinition(String name) {
        var definition = new InterfacesConfigProperties.InterfaceDefinition();
        definition.setName(name);
        definition.setType(InterfaceType.realtime);
        return definition;
    }

    private static class TrackingObjectMapper extends ObjectMapper {
        private Object lastSerializedValue;

        @Override
        public String writeValueAsString(Object value) throws JacksonException {
            lastSerializedValue = value;
            return super.writeValueAsString(value);
        }

        private Object lastSerializedValue() {
            return lastSerializedValue;
        }
    }
}
