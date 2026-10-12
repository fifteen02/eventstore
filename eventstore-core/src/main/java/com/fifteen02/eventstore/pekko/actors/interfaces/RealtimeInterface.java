package com.fifteen02.eventstore.pekko.actors.interfaces;

import com.fifteen02.eventstore.config.InterfaceType;
import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.constants.MessageHeaders;
import com.fifteen02.eventstore.to.MessageTo;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapSetter;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import tools.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RealtimeInterface extends AbstractBehavior<Interface.Command> implements Interface {
    private static final Logger log = LoggerFactory.getLogger(RealtimeInterface.class);
    private static final String INTERFACE_NAME_ATTRIBUTE = "interface.name";
    private static final String INTERFACE_TYPE_ATTRIBUTE = "interface.type";
    private static final String MESSAGE_ID_ATTRIBUTE = "message.id";
    private static final String MESSAGE_TYPE_ATTRIBUTE = "message.type";
    private static final String SERVER_CORRELATION_ATTRIBUTE = "server.correlation.uuid";

    private static final TextMapSetter<Map<String, String>> MAP_SETTER = Map::put;

    protected final String serverCorrelationUUID;
    protected final InterfacesConfigProperties.InterfaceDefinition interfaceDefinition;
    protected final InterfaceStateService interfaceStateService;
    protected final RabbitTemplate rabbitTemplate;
    protected final ObjectMapper objectMapper;
    protected final InterfaceType type;
    protected final Set<String> include;
    protected final Set<String> exclude;
    protected final Tracer tracer;

    public static Behavior<Command> create(String serverCorrelationUUID,
            InterfacesConfigProperties.InterfaceDefinition def,
            InterfaceStateService interfaceStateService,
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper) {
        return Behaviors.setup(
                ctx -> new RealtimeInterface(ctx, serverCorrelationUUID, def, interfaceStateService, rabbitTemplate,
                        objectMapper));
    }

    protected RealtimeInterface(ActorContext<Command> context,
            String serverCorrelationUUID,
            InterfacesConfigProperties.InterfaceDefinition interfaceDefinition,
            InterfaceStateService interfaceStateService,
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper) {
        super(context);
        this.serverCorrelationUUID = serverCorrelationUUID;
        this.interfaceDefinition = interfaceDefinition;
        this.interfaceStateService = interfaceStateService;
        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.type = interfaceDefinition.getType() == null ? InterfaceType.realtime : interfaceDefinition.getType();
        this.include = normaliseConfiguredTypes(interfaceDefinition.getIncludeTypes());
        this.exclude = normaliseConfiguredTypes(interfaceDefinition.getExcludeTypes());
        this.tracer = GlobalOpenTelemetry.getTracer("com.fifteen02.eventstore");

        log.info("Interface [{}] '{}' started and ready to receive messages", interfaceDefinition.getType(),
                interfaceDefinition.getName());

    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onMessage(Routed.class, this::onRouted)
                .onMessage(ExternalAck.class, this::onExternalAck)
                .build();
    }

    protected Behavior<Command> onRouted(Routed r) {
        if (shouldFilter(r)) {
            return this;
        }
        return doPublish(r);
    }

    /**
     * Check if the message should be filtered (not processed).
     * Returns true if the message should be skipped.
     */
    protected boolean shouldFilter(Routed r) {
        if (!interfaceStateService.isRunning(interfaceDefinition.getName())) {
            return true;
        }
        String messageType = normaliseMessageType(r.messageTo().messageType());
        if (include != null && !include.isEmpty() && !include.contains(messageType)) {
            return true;
        }
        return exclude != null && exclude.contains(messageType);
    }

    protected boolean shouldFilterMessageType(String rawMessageType) {
        String messageType = normaliseMessageType(rawMessageType);
        if (include != null && !include.isEmpty() && !include.contains(messageType)) {
            return true;
        }
        return exclude != null && exclude.contains(messageType);
    }

    protected static Set<String> normaliseConfiguredTypes(Set<String> types) {
        if (types == null) {
            return null;
        }
        return types.stream()
                .map(RealtimeInterface::normaliseMessageType)
                .collect(Collectors.toUnmodifiableSet());
    }

    protected static String normaliseMessageType(String messageType) {
        if (messageType == null) {
            return "";
        }
        return messageType.trim().toLowerCase(Locale.ROOT);
    }

    /**
     * Publish the message with a tracing span.
     */
    protected Behavior<Command> doPublish(Routed r) {
        // Create span with interface name
        Span span = tracer.spanBuilder("Interface " + interfaceDefinition.getName())
                .setSpanKind(SpanKind.CONSUMER)
                .setAttribute(INTERFACE_NAME_ATTRIBUTE, interfaceDefinition.getName())
                .setAttribute(INTERFACE_TYPE_ATTRIBUTE, type.toString())
                .setAttribute(MESSAGE_ID_ATTRIBUTE, r.messageTo().id())
                .setAttribute(MESSAGE_TYPE_ATTRIBUTE, r.messageTo().messageType())
                .setAttribute(SERVER_CORRELATION_ATTRIBUTE, serverCorrelationUUID)
                .startSpan();

        try (Scope scope = span.makeCurrent()) {
            publish(r);
        } finally {
            span.end();
        }

        return this;
    }

    void publish(Routed r) {
        try {
            String queueName = buildSinkQueueName(interfaceDefinition);

            // Capture current OpenTelemetry context into W3C headers
            Map<String, String> carrier = new HashMap<>();
            GlobalOpenTelemetry.getPropagators().getTextMapPropagator().inject(Context.current(), carrier, MAP_SETTER);
            var trace = traceHeaders(carrier, r.messageTo());

            var props = MessagePropertiesBuilder.newInstance()
                    .setContentType("application/json")
                    .setHeader(MessageHeaders.MESSAGE_ID, r.messageTo().id())
                    .setHeader(MessageHeaders.BAGGAGE, r.messageTo().baggage())
                    .setHeader(MessageHeaders.CONTENT_HASH, r.messageTo().contentHash())
                    .setHeader(MessageHeaders.CORRELATION_ID, r.messageTo().correlationId())
                    .setHeader(MessageHeaders.METADATA, metadataHeader(r))
                    .setHeader(MessageHeaders.MESSAGE_TYPE, String.valueOf(r.messageTo().messageType()))
                    .setHeader(MessageHeaders.SERVER_CORRELATION_UUID, serverCorrelationUUID)
                    .setHeader(MessageHeaders.USER_ID, r.messageTo().userId())
                    .setHeader(MessageHeaders.TIMESTAMP, r.messageTo().timestamp())
                    .setHeader(MessageHeaders.TRACE_PARENT, trace.traceParent())
                    .setHeader(MessageHeaders.TRACE_STATE, trace.traceState())
                    .build();

            // Use default direct exchange ("") and route by queue name
            rabbitTemplate.send("", queueName,
                    MessageBuilder.withBody(objectMapper.writeValueAsBytes(r.messageTo().body()))
                            .andProperties(props)
                            .build());
            log.debug("Interface {} published id={} type={}", interfaceDefinition.getName(), r.messageTo().id(),
                    r.messageTo().messageType());
        } catch (Exception e) {
            log.error("Interface {} failed to publish id={}: {}", interfaceDefinition.getName(), r.messageTo().id(),
                    e.getMessage(), e);
            throw e;
        }
    }

    /**
     * Prefer the delivery span's context so consumers continue this trace. Both values come from the same
     * source: a stored tracestate belongs to the stored traceparent, not to the delivery span.
     */
    static TraceHeaders traceHeaders(Map<String, String> carrier, MessageTo messageTo) {
        String traceParent = carrier.get(MessageHeaders.TRACE_PARENT);
        if (traceParent != null) {
            return new TraceHeaders(traceParent, carrier.get(MessageHeaders.TRACE_STATE));
        }
        return new TraceHeaders(messageTo.traceParent(), messageTo.traceState());
    }

    record TraceHeaders(String traceParent, String traceState) {
    }

    private String buildSinkQueueName(InterfacesConfigProperties.InterfaceDefinition interfaceDefinition) {
        return interfaceDefinition.getName() + ".sink";
    }

    private String metadataHeader(Routed routed) {
        if (routed.messageTo().metadata() == null) {
            return null;
        }
        try {
            return objectMapper.writeValueAsString(routed.messageTo().metadata());
        } catch (Exception exception) {
            throw new IllegalArgumentException("Unable to serialize message metadata", exception);
        }
    }

    protected Behavior<Command> onExternalAck(ExternalAck ack) {
        throw new UnsupportedOperationException("Realtime interfaces do not support external acknowledgements");
    }

}
