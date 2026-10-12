package com.fifteen02.eventstore.samples.producer;

import com.fifteen02.eventstore.constants.MessageHeaders;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Producer: publishes order events to EventStore's AMQP input queue instead of calling REST.
 *
 * <p>The AMQP body is the message content (JSON); everything else travels as headers.
 * {@code messageType} is required, the rest optional. Messages are stored exactly as if they had
 * been sent to {@code PUT /api/events/message}, but the producer gets no reply: to see the
 * assigned indices, watch the log with {@code samples/01-quickstart/stream.sh}.
 *
 * <p>Set {@code ORDER_COUNT} to change how many orders are published (default 3).
 */
public final class AmqpPublisher {

    private static final String INPUT_QUEUE = env("EVENTSTORE_INPUT_QUEUE", "eventstore.input");
    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        int orders = Integer.parseInt(env("ORDER_COUNT", "3"));
        // A per-run suffix keeps bodies unique. EventStore drops a message whose body hash it has
        // recently stored, so re-sending identical bodies would be skipped as duplicates.
        String run = UUID.randomUUID().toString().substring(0, 8);

        try (var connection = connectionFactory().newConnection("eventstore-sample-publisher");
             var channel = connection.createChannel()) {
            // Publisher confirms: the broker tells us once it has taken responsibility for each message.
            channel.confirmSelect();
            for (int i = 1; i <= orders; i++) {
                String orderId = "order-" + run + "-" + i;
                String customer = "customer-" + (i % 3);
                publish(channel, "OrderPlaced", customer, Map.of("orderId", orderId, "total", 10 * i));
                publish(channel, "OrderShipped", customer, Map.of("orderId", orderId, "carrier", "DHL"));
                if (i % 3 == 0) {
                    publish(channel, "OrderCancelled", customer, Map.of("orderId", orderId, "reason", "changed mind"));
                }
            }
            channel.waitForConfirmsOrDie(5_000);
            System.out.println("all messages confirmed by the broker");
        }
    }

    private static void publish(Channel channel, String messageType, String userId, Map<String, Object> content)
            throws Exception {
        Map<String, Object> headers = new HashMap<>();
        headers.put(MessageHeaders.MESSAGE_TYPE, messageType);              // required: messages without it are rejected
        headers.put(MessageHeaders.USER_ID, userId);                        // used by SSE user filters and sharding
        headers.put(MessageHeaders.CORRELATION_ID, UUID.randomUUID().toString());
        headers.put(MessageHeaders.TIMESTAMP, Instant.now().toString());    // ISO-8601 or epoch millis; defaults to now
        headers.put(MessageHeaders.METADATA, JSON.writeValueAsString(      // a JSON object, encoded as a string
                Map.of("source", "amqp-publisher-sample")));

        var properties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .deliveryMode(2) // persistent
                .headers(headers)
                .build();

        // Default exchange (""), routed by queue name.
        channel.basicPublish("", INPUT_QUEUE, properties, JSON.writeValueAsBytes(content));
        System.out.printf("published %-14s user=%s %s%n", messageType, userId, content);
    }

    private static ConnectionFactory connectionFactory() {
        var factory = new ConnectionFactory();
        factory.setHost(env("AMQP_HOST", "localhost"));
        factory.setPort(Integer.parseInt(env("AMQP_PORT", "5672")));
        factory.setUsername(env("AMQP_USERNAME", "guest"));
        factory.setPassword(env("AMQP_PASSWORD", "guest"));
        factory.setVirtualHost(env("AMQP_VHOST", "/"));
        return factory;
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
