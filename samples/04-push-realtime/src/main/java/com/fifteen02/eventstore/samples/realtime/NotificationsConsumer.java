package com.fifteen02.eventstore.samples.realtime;

import com.fifteen02.eventstore.constants.MessageHeaders;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * Push consumer for the {@code notifications} <b>realtime</b> interface (see this sample's
 * {@code eventstore.yml}).
 *
 * <p>A realtime interface is fire-and-forget. EventStore publishes each matching message to
 * {@code notifications.sink} once, as it is ingested. There is no {@code .ack} queue, no saved
 * position and no replay: a realtime interface never sees history, only what happens after
 * EventStore starts. The only acknowledgement is the ordinary broker ack.
 *
 * <p>Use it where a missed message is acceptable: push notifications, cache invalidation, live
 * dashboards.
 */
public final class NotificationsConsumer {

    private static final String SINK_QUEUE = env("INTERFACE_NAME", "notifications") + ".sink";

    public static void main(String[] args) throws Exception {
        try (var connection = connectionFactory().newConnection("eventstore-sample-notifications");
             var channel = connection.createChannel()) {
            channel.basicConsume(SINK_QUEUE, false, (tag, delivery) -> {
                var headers = delivery.getProperties().getHeaders();
                System.out.printf("notifications: #%s %-14s user=%-10s %s%n",
                        header(headers, MessageHeaders.MESSAGE_ID),
                        header(headers, MessageHeaders.MESSAGE_TYPE),
                        header(headers, MessageHeaders.USER_ID),
                        new String(delivery.getBody(), StandardCharsets.UTF_8));
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }, tag -> { });
            System.out.printf("notifications: consuming %s (Ctrl+C to stop)%n", SINK_QUEUE);
            new CountDownLatch(1).await();
        }
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

    private static String header(Map<String, Object> headers, String name) {
        var value = headers == null ? null : headers.get(name);
        return value == null ? null : value.toString();
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
