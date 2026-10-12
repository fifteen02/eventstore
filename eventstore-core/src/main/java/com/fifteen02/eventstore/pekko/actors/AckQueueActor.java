package com.fifteen02.eventstore.pekko.actors;

import tools.jackson.databind.ObjectMapper;
import com.fifteen02.eventstore.pekko.actors.interfaces.Interface;
import com.fifteen02.eventstore.to.AcknowledgeTo;
import com.fifteen02.eventstore.to.Index;
import com.rabbitmq.client.Channel;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PostStop;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;

import java.util.Map;

public class AckQueueActor extends AbstractBehavior<AckQueueActor.Command> {
    private static final Logger log = LoggerFactory.getLogger(AckQueueActor.class);
    private static final String ACK_INDEX_ATTRIBUTE = "ack.index";
    private static final String INTERFACE_NAME_ATTRIBUTE = "interface.name";
    private static final String QUEUE_NAME_ATTRIBUTE = "queue.name";
    private static final String SERVER_CORRELATION_ATTRIBUTE = "server.correlation.uuid";

    public interface Command {
    }

    private static final Tracer tracer = GlobalOpenTelemetry.getTracer("com.fifteen02.eventstore");

    private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<Map<String, String>>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier.get(key);
        }
    };

    private final SimpleMessageListenerContainer container;
    private final ObjectMapper objectMapper;

    public static Behavior<Command> create(String serverCorrelationUUID,
            String interfaceName,
            int prefetchCount,
            ConnectionFactory connectionFactory,
            ActorRef<Interface.Command> parentInterfaceActor) {
        return Behaviors.setup(ctx -> new AckQueueActor(ctx, serverCorrelationUUID, prefetchCount, interfaceName,
                connectionFactory, parentInterfaceActor));
    }

    private AckQueueActor(ActorContext<Command> context,
                          String serverCorrelationUUID,
                          int prefetchCount,
                          String interfaceName,
                          ConnectionFactory connectionFactory,
                          ActorRef<Interface.Command> parentInterfaceActor) {
        super(context);

        this.objectMapper = new ObjectMapper();
        String queueName = interfaceName + ".ack";
        this.container = new SimpleMessageListenerContainer(connectionFactory);
        this.container.setQueueNames(queueName);
        this.container.setConcurrentConsumers(1);
        this.container.setPrefetchCount(prefetchCount);
        this.container.setAcknowledgeMode(org.springframework.amqp.core.AcknowledgeMode.MANUAL);
        this.container.setAutoStartup(true);
        this.container.setMessageListener((org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener) (Message message, Channel channel) -> {
            long tag = message.getMessageProperties().getDeliveryTag();
            String messageServerCorrelationUUID = (String) message.getMessageProperties().getHeaders().get("serverCorrelationUUID");

            if (!serverCorrelationUUID.equals(messageServerCorrelationUUID)) {
                log.info("Received ack message on {} for different serverCorrelationUUID: {}/{} - ack", queueName, messageServerCorrelationUUID, serverCorrelationUUID);
                channel.basicAck(tag, false);
                return;
            }

            // Extract trace context from message headers
            Map<String, String> traceHeaders = Map.of(
                "traceparent", (String) message.getMessageProperties().getHeaders().getOrDefault("traceparent", ""),
                "tracestate", (String) message.getMessageProperties().getHeaders().getOrDefault("tracestate", ""),
                "baggage", (String) message.getMessageProperties().getHeaders().getOrDefault("baggage", "")
            );

            // Extract context from headers
            Context otelContext = Context.current();
            TextMapPropagator propagator = GlobalOpenTelemetry.getPropagators().getTextMapPropagator();
            otelContext = propagator.extract(otelContext, traceHeaders, MAP_GETTER);

            // Create span for acknowledgment processing
            Span span = tracer.spanBuilder("Ack Eventstore " + interfaceName)
                    .setSpanKind(SpanKind.CONSUMER)
                    .setParent(otelContext)
                    .startSpan();

            try (Scope scope = span.makeCurrent()) {
                String body = new String(message.getBody());
                log.debug("Received ack message on {}: {}", queueName, body);

                AcknowledgeTo ack = objectMapper.readValue(body, AcknowledgeTo.class);

                if (ack.index() == null) {
                    log.warn("Ack message missing index on queue {} - rejecting", queueName);
                    span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, "Missing index");
                    channel.basicNack(tag, false, false);
                    return;
                }

                // Add span attributes using standardized keys
                span.setAttribute(INTERFACE_NAME_ATTRIBUTE, interfaceName);
                span.setAttribute(ACK_INDEX_ATTRIBUTE, ack.index());
                span.setAttribute(SERVER_CORRELATION_ATTRIBUTE, serverCorrelationUUID);
                span.setAttribute(QUEUE_NAME_ATTRIBUTE, queueName);
                log.info("Sent Ack message with index to parent actor: {} for interface: {}", ack.index(), interfaceName);
                Interface.ExternalAck externalAck = new Interface.ExternalAck(
                        new Index(ack.index()),
                        () -> {
                            try {
                                channel.basicAck(tag, false);
                            } catch (Exception e) {
                            }
                            return null;
                        }
                );

                parentInterfaceActor.tell(externalAck);
            } catch (Exception e) {
                span.recordException(e);
                span.setStatus(io.opentelemetry.api.trace.StatusCode.ERROR, e.getMessage());
                try {
                    channel.basicNack(tag, false, true);
                } catch (Exception ignored) {
                }
                log.error("Failed handling ack message on {}: {}", queueName, e.getMessage(), e);
            } finally {
                span.end();
            }
        });

        this.container.start();
        log.info("AckQueueActor started consuming from {}", queueName);
    }

    @Override
    public Receive<Command> createReceive() {
        return newReceiveBuilder()
                .onSignal(PostStop.class, _ -> onPostStop())
                .build();
    }

    private Behavior<Command> onPostStop() {
        try {
            container.stop();
            log.info("AckQueueActor stopped container for queue");
        } catch (Exception e) {
            log.warn("Error stopping container during actor shutdown: {}", e.getMessage());
        }
        return Behaviors.stopped();
    }
}
