package com.fifteen02.eventstore.samples.replayable;

import com.fifteen02.eventstore.constants.MessageHeaders;
import com.fifteen02.eventstore.to.AcknowledgeTo;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Delivery;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Push consumer for the {@code billing} <b>replayable</b> interface (see this sample's {@code eventstore.yml}).
 *
 * <p>EventStore pushes each matching message to {@code billing.sink}. For every message the consumer:
 * <ol>
 *   <li>processes it, at most once, using the global index ({@code messageId} header) as the
 *       idempotency key. A small on-disk ledger survives consumer restarts;</li>
 *   <li>acknowledges it to <b>EventStore</b> on {@code billing.ack}, which moves the interface's
 *       replay position;</li>
 *   <li>acknowledges the delivery to the <b>broker</b>, which removes it from {@code billing.sink}.</li>
 * </ol>
 *
 * <p>Delivery is at-least-once. After an EventStore restart, messages processed since the last saved
 * ack batch are pushed again; the ledger turns those into "duplicate, skipped".
 */
public final class BillingConsumer {

    private static final String INTERFACE = env("INTERFACE_NAME", "billing");
    private static final String SINK_QUEUE = INTERFACE + ".sink";
    private static final String ACK_QUEUE = INTERFACE + ".ack";
    private static final long SLOW_MS = Long.parseLong(env("SLOW_MS", "0"));
    private static final ObjectMapper JSON = new ObjectMapper();

    private final Path ledgerFile;
    private final Set<Long> processed = new HashSet<>();

    private BillingConsumer(Path ledgerFile) throws IOException {
        this.ledgerFile = ledgerFile;
        if (Files.exists(ledgerFile)) {
            Files.readAllLines(ledgerFile).stream().filter(l -> !l.isBlank()).map(Long::parseLong).forEach(processed::add);
        }
    }

    public static void main(String[] args) throws Exception {
        var classes = Path.of(BillingConsumer.class.getProtectionDomain().getCodeSource().getLocation().toURI());
        var consumer = new BillingConsumer(classes.getParent().resolve("billing.ledger"));

        try (var connection = connectionFactory().newConnection("eventstore-sample-billing");
             var channel = connection.createChannel()) {
            channel.basicQos(50);
            channel.basicConsume(SINK_QUEUE, false, (tag, delivery) -> consumer.handle(channel, delivery), tag -> { });
            System.out.printf("billing: consuming %s, acknowledging on %s, %d already processed (Ctrl+C to stop)%n",
                    SINK_QUEUE, ACK_QUEUE, consumer.processed.size());
            new CountDownLatch(1).await();
        }
    }

    private void handle(Channel channel, Delivery delivery) throws IOException {
        Map<String, Object> headers = delivery.getProperties().getHeaders();
        long index = Long.parseLong(header(headers, MessageHeaders.MESSAGE_ID));
        String serverCorrelationUuid = header(headers, MessageHeaders.SERVER_CORRELATION_UUID);

        // 1. Process, idempotently.
        if (processed.contains(index)) {
            System.out.printf("billing: #%d duplicate, skipped%n", index);
        } else {
            charge(index, header(headers, MessageHeaders.MESSAGE_TYPE), header(headers, MessageHeaders.USER_ID),
                    new String(delivery.getBody(), StandardCharsets.UTF_8));
            remember(index);
        }

        // 2. Acknowledge to EventStore, even for duplicates, so its position keeps moving.
        //    serverCorrelationUUID is copied from the sink message into the ack's *header*:
        //    EventStore discards acks whose header doesn't match its current run.
        var ackProperties = new AMQP.BasicProperties.Builder()
                .contentType("application/json")
                .headers(Map.of(MessageHeaders.SERVER_CORRELATION_UUID, serverCorrelationUuid))
                .build();
        channel.basicPublish("", ACK_QUEUE, ackProperties,
                JSON.writeValueAsBytes(new AcknowledgeTo(index, serverCorrelationUuid)));

        // 3. Remove the delivery from billing.sink.
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
    }

    /** The business logic. */
    private void charge(long index, String messageType, String userId, String body) {
        System.out.printf("billing: #%d %-14s user=%-10s %s%n", index, messageType, userId, body);
        if (SLOW_MS > 0) {
            try {
                Thread.sleep(SLOW_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private void remember(long index) throws IOException {
        processed.add(index);
        Files.writeString(ledgerFile, index + "\n", StandardOpenOption.CREATE, StandardOpenOption.APPEND);
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

    /** AMQP string headers arrive as {@code LongString}; toString() gives the value. */
    private static String header(Map<String, Object> headers, String name) {
        var value = headers == null ? null : headers.get(name);
        return value == null ? null : value.toString();
    }

    private static String env(String name, String defaultValue) {
        var value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }
}
