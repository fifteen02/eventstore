package com.fifteen02.eventstore.amqp;

import com.fifteen02.eventstore.constants.MessageHeaders;
import com.fifteen02.eventstore.repo.InterfaceStateRepository;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.to.AcknowledgeTo;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessagePropertiesBuilder;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.datasource.driver-class-name=org.h2.Driver",
                "spring.datasource.url=jdbc:h2:mem:amqp-e2e;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;INIT=CREATE DOMAIN IF NOT EXISTS JSONB AS JSON",
                "spring.datasource.username=sa",
                "spring.datasource.password=",
                "spring.jpa.hibernate.ddl-auto=create-drop",
                "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
                "spring.jpa.properties.hibernate.format_sql=false",
                "spring.jpa.show-sql=false",
                "eventstore.auth.bearer-token=dev-eventstore-token",
                "eventstore.amqp.enabled=true",
                "eventstore.input.queue=eventstore.input.it",
                "eventstore.interfaces[0].name=amqp-it-persister",
                "eventstore.interfaces[0].type=persister",
                "eventstore.interfaces[0].sink.amqp.virtual-host=/",
                "eventstore.interfaces[0].ack.amqp.virtual-host=/",
                "eventstore.interfaces[0].ack.batch=1",
                "eventstore.interfaces[0].ack.prefetch=1",
                "eventstore.big-query.enabled=false",
                "eventstore.realtime-retention.enabled=false",
                "eventstore.realtime-retention.cron=-",
                "eventstore.sse.heartbeat-interval-seconds=60"
        }
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class AmqpEndToEndIT {

    private static final String INPUT_QUEUE = "eventstore.input.it";
    private static final String INTERFACE_NAME = "amqp-it-persister";
    private static final String SINK_QUEUE = INTERFACE_NAME + ".sink";
    private static final String ACK_QUEUE = INTERFACE_NAME + ".ack";
    private static final DockerImageName LAVINMQ_IMAGE = DockerImageName.parse("cloudamqp/lavinmq:2.1.0");

    @Container
    private static final GenericContainer<?> lavinmq = new GenericContainer<>(LAVINMQ_IMAGE)
            .withExposedPorts(5672);

    @DynamicPropertySource
    static void registerAmqpProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", lavinmq::getHost);
        registry.add("spring.rabbitmq.port", () -> lavinmq.getMappedPort(5672));
        registry.add("spring.rabbitmq.username", () -> "guest");
        registry.add("spring.rabbitmq.password", () -> "guest");
        registry.add("spring.rabbitmq.virtual-host", () -> "/");
    }

    @Autowired
    private RabbitTemplate rabbitTemplate;

    @Autowired
    private AmqpAdmin amqpAdmin;

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private InterfaceStateRepository interfaceStateRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Test
    void amqpInputPublishesSinkMessageAndProcessesAcknowledgement() throws Exception {
        String serverCorrelationUuid = acknowledgeStartupCatchUp();

        publishInputMessage();

        Message sinkMessage = awaitSinkMessage();
        Long eventstoreIndex = sinkMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_ID);
        serverCorrelationUuid = sinkMessage.getMessageProperties().getHeader(MessageHeaders.SERVER_CORRELATION_UUID);
        String metadata = sinkMessage.getMessageProperties().getHeader(MessageHeaders.METADATA);

        assertThat(eventstoreIndex).isNotNull();
        assertThat(serverCorrelationUuid).isNotBlank();
        assertThat(new String(sinkMessage.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"ok\":true}");
        assertThat(objectMapper.readTree(metadata).path("source").asString()).isEqualTo("amqp-it");
        assertThat(messageRepository.findById(eventstoreIndex))
                .isPresent()
                .get()
                .satisfies(message -> {
                    assertThat(message.getType()).isEqualTo("AmqpIntegrationEvent");
                    assertThat(message.getUserId()).isEqualTo("amqp-user");
                    assertThat(message.getCorrelationId()).isEqualTo("amqp-corr");
                    assertThat(message.getMetadata()).containsEntry("source", "amqp-it");
                });

        awaitQueueDepth(INPUT_QUEUE, 0);

        publishAck(eventstoreIndex, serverCorrelationUuid);

        awaitCondition(() -> interfaceStateRepository.findById(INTERFACE_NAME)
                .filter(state -> state.getLastAcknowledgedId() == eventstoreIndex)
                .isPresent());
        awaitQueueDepth(ACK_QUEUE, 0);
    }

    @Test
    void repeatedAmqpInputMessageIsPersistedAndPublishedOnce() throws Exception {
        String serverCorrelationUuid = acknowledgeStartupCatchUp();

        publishInputMessage("{\"sequence\":1}", "duplicate-corr", "duplicate-user",
                "{\"source\":\"duplicate-scenario\"}");
        publishInputMessage("{\"sequence\":1}", "duplicate-corr", "duplicate-user",
                "{\"source\":\"duplicate-scenario\"}");
        publishInputMessage("{\"sequence\":1}", "duplicate-corr", "duplicate-user",
                "{\"source\":\"duplicate-scenario\"}");

        Message sinkMessage = awaitSinkMessage();
        Long eventstoreIndex = sinkMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_ID);

        assertThat(eventstoreIndex).isEqualTo(2L);
        assertThat(new String(sinkMessage.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"sequence\":1}");
        awaitCondition(() -> messageRepository.count() == 2L);
        awaitQueueDepth(INPUT_QUEUE, 0);
        assertNoSinkMessage();

        publishAck(eventstoreIndex, serverCorrelationUuid);
        awaitAcknowledged(eventstoreIndex);
    }

    @Test
    void outOfOrderAcknowledgementsWaitForMissingIndexBeforeAdvancing() throws Exception {
        String serverCorrelationUuid = acknowledgeStartupCatchUp();

        publishInputMessage("{\"sequence\":1}", "ordered-corr-1", "ordered-user",
                "{\"source\":\"ordered-scenario\"}");
        publishInputMessage("{\"sequence\":2}", "ordered-corr-2", "ordered-user",
                "{\"source\":\"ordered-scenario\"}");

        Message firstSinkMessage = awaitSinkMessage();
        Message secondSinkMessage = awaitSinkMessage();
        Long firstIndex = firstSinkMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_ID);
        Long secondIndex = secondSinkMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_ID);

        assertThat(firstIndex).isEqualTo(2L);
        assertThat(secondIndex).isEqualTo(3L);
        assertThat(new String(firstSinkMessage.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"sequence\":1}");
        assertThat(new String(secondSinkMessage.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"sequence\":2}");

        publishAck(secondIndex, serverCorrelationUuid);
        awaitQueueDepth(ACK_QUEUE, 0);
        assertAcknowledged(1L);

        publishAck(firstIndex, serverCorrelationUuid);
        awaitAcknowledged(secondIndex);
    }

    @Test
    void staleAndForeignAcknowledgementsAreDrainedWithoutMovingStateBackward() throws Exception {
        String serverCorrelationUuid = acknowledgeStartupCatchUp();

        publishInputMessage();
        Message sinkMessage = awaitSinkMessage();
        Long eventstoreIndex = sinkMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_ID);

        publishAck(eventstoreIndex, "foreign-server-correlation");
        awaitQueueDepth(ACK_QUEUE, 0);
        assertAcknowledged(1L);

        publishAck(eventstoreIndex, serverCorrelationUuid);
        awaitAcknowledged(eventstoreIndex);

        publishAck(eventstoreIndex, serverCorrelationUuid);
        publishAck(1L, serverCorrelationUuid);
        awaitQueueDepth(ACK_QUEUE, 0);
        assertAcknowledged(eventstoreIndex);
    }

    @Test
    void inputWithoutMessageTypeIsRejectedAndIngestionContinues() throws Exception {
        String serverCorrelationUuid = acknowledgeStartupCatchUp();

        rabbitTemplate.send("", INPUT_QUEUE, MessageBuilder
                .withBody("{\"untyped\":true}".getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setContentType("application/json")
                        .build())
                .build());
        publishInputMessage("{\"afterUntyped\":true}", "after-untyped-corr", "after-untyped-user",
                "{\"source\":\"untyped-scenario\"}");

        Message sinkMessage = awaitSinkMessage();
        Long eventstoreIndex = sinkMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_ID);

        assertThat(eventstoreIndex).isEqualTo(2L);
        assertThat(new String(sinkMessage.getBody(), StandardCharsets.UTF_8)).isEqualTo("{\"afterUntyped\":true}");
        awaitQueueDepth(INPUT_QUEUE, 0);
        assertThat(messageRepository.count()).isEqualTo(2L);

        publishAck(eventstoreIndex, serverCorrelationUuid);
        awaitAcknowledged(eventstoreIndex);
    }

    private String acknowledgeStartupCatchUp() throws Exception {
        awaitCondition(() -> interfaceStateRepository.findById(INTERFACE_NAME)
                .filter(state -> state.getLastAcknowledgedId() == 0L)
                .isPresent());

        Message catchUpMessage = awaitSinkMessage();
        Long catchUpIndex = catchUpMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_ID);
        String serverCorrelationUuid = catchUpMessage.getMessageProperties().getHeader(MessageHeaders.SERVER_CORRELATION_UUID);
        String catchUpType = catchUpMessage.getMessageProperties().getHeader(MessageHeaders.MESSAGE_TYPE);

        assertThat(catchUpIndex).isEqualTo(1L);
        assertThat(catchUpType).isEqualTo("StartMarker");
        assertThat(new String(catchUpMessage.getBody(), StandardCharsets.UTF_8)).isEqualTo("{}");
        publishAck(catchUpIndex, serverCorrelationUuid);
        awaitAcknowledged(catchUpIndex);
        return serverCorrelationUuid;
    }

    private void publishInputMessage() {
        publishInputMessage("{\"ok\":true}", "amqp-corr", "amqp-user",
                "{\"source\":\"amqp-it\",\"flow\":\"input-to-sink\"}");
    }

    private void publishInputMessage(String body, String correlationId, String userId, String metadata) {
        rabbitTemplate.send("", INPUT_QUEUE, MessageBuilder
                .withBody(body.getBytes(StandardCharsets.UTF_8))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setContentType("application/json")
                        .setHeader(MessageHeaders.MESSAGE_TYPE, "AmqpIntegrationEvent")
                        .setHeader(MessageHeaders.USER_ID, userId)
                        .setHeader(MessageHeaders.CORRELATION_ID, correlationId)
                        .setHeader(MessageHeaders.TIMESTAMP, Instant.parse("2026-06-04T07:25:00Z").toString())
                        .setHeader(MessageHeaders.METADATA, metadata)
                        .build())
                .build());
    }

    private Message awaitSinkMessage() {
        return awaitValue(() -> rabbitTemplate.receive(SINK_QUEUE, 250));
    }

    private void publishAck(Long index, String serverCorrelationUuid) throws Exception {
        rabbitTemplate.send("", ACK_QUEUE, MessageBuilder
                .withBody(objectMapper.writeValueAsBytes(new AcknowledgeTo(index, serverCorrelationUuid)))
                .andProperties(MessagePropertiesBuilder.newInstance()
                        .setContentType("application/json")
                        .setHeader(MessageHeaders.SERVER_CORRELATION_UUID, serverCorrelationUuid)
                        .build())
                .build());
    }

    private void awaitQueueDepth(String queue, int expectedDepth) {
        awaitCondition(() -> queueDepth(queue) == expectedDepth);
    }

    private void awaitAcknowledged(Long expectedIndex) {
        awaitCondition(() -> interfaceStateRepository.findById(INTERFACE_NAME)
                .filter(state -> state.getLastAcknowledgedId() == expectedIndex)
                .isPresent());
    }

    private void assertAcknowledged(Long expectedIndex) {
        assertThat(interfaceStateRepository.findById(INTERFACE_NAME))
                .isPresent()
                .get()
                .satisfies(state -> assertThat(state.getLastAcknowledgedId()).isEqualTo(expectedIndex));
    }

    private void assertNoSinkMessage() {
        assertThat(rabbitTemplate.receive(SINK_QUEUE, 750)).isNull();
    }

    private int queueDepth(String queue) {
        Properties properties = amqpAdmin.getQueueProperties(queue);
        if (properties == null) {
            return -1;
        }
        Object value = properties.get(RabbitAdmin.QUEUE_MESSAGE_COUNT);
        return value instanceof Number count ? count.intValue() : -1;
    }

    private <T> T awaitValue(ValueSupplier<T> supplier) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            T value = supplier.get();
            if (value != null) {
                return value;
            }
            sleep();
        }
        throw new AssertionError("Timed out waiting for AMQP message");
    }

    private void awaitCondition(BooleanSupplier condition) {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            sleep();
        }
        throw new AssertionError("Timed out waiting for AMQP condition");
    }

    private void sleep() {
        try {
            Thread.sleep(100);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for AMQP condition", exception);
        }
    }

    @FunctionalInterface
    private interface ValueSupplier<T> {
        T get();
    }
}
