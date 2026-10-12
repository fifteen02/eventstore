package com.fifteen02.eventstore.repo;

import com.fifteen02.eventstore.entity.Message;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.domain.PageRequest;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest(showSql = false, properties = {
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.url=jdbc:h2:mem:message-repository;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;INIT=CREATE DOMAIN IF NOT EXISTS JSONB AS JSON",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.jpa.properties.hibernate.format_sql=false"
})
@Import(MessageRepositoryTest.TestApplication.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
class MessageRepositoryTest {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private MessageRepository messageRepository;

    @Test
    void findTopNByOrderByIdDesc_respectsPageableLimit() {
        for (long id = 1; id <= 10; id++) {
            messageRepository.save(buildMessage(id));
        }

        List<Message> recentMessages = messageRepository.findTopNByOrderByIdDesc(PageRequest.of(0, 3));

        assertThat(recentMessages)
                .hasSize(3)
                .extracting(Message::getId)
                .containsExactly(10L, 9L, 8L);
    }

    @Test
    void save_acceptsMessageTypeOutsideKnownEnum() {
        Message message = buildMessage(11L);
        message.setType("ExternalSystemNotificationCreated");

        messageRepository.save(message);

        assertThat(messageRepository.findById(11L))
                .isPresent()
                .get()
                .extracting(Message::getType)
                .isEqualTo("ExternalSystemNotificationCreated");
    }

    @Test
    void save_preservesMetadata() {
        var metadata = Map.<String, Object>of(
                "source", "external-system-api",
                "attribution", "campaign-42");
        Message message = buildMessage(12L);
        message.setMetadata(metadata);

        messageRepository.save(message);

        assertThat(messageRepository.findById(12L))
                .isPresent()
                .get()
                .extracting(Message::getMetadata)
                .isEqualTo(metadata);
    }

    @Test
    void save_preservesJsonContent() {
        Message message = buildMessage(13L);

        messageRepository.save(message);

        assertThat(messageRepository.findById(13L))
                .isPresent()
                .get()
                .extracting(Message::getContent)
                .isEqualTo(json("{\"contentId\":13}"));
    }

    private static Message buildMessage(long id) {
        Message message = new Message();
        message.setId(id);
        message.setContent(json("{\"contentId\":" + id + "}"));
        message.setTimestamp(Instant.now());
        message.setType("ProfileUpdatedEvent");
        return message;
    }

    private static JsonNode json(String value) {
        try {
            return OBJECT_MAPPER.readTree(value);
        } catch (Exception exception) {
            throw new IllegalArgumentException("Invalid test JSON", exception);
        }
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EntityScan(basePackageClasses = Message.class)
    @EnableJpaRepositories(basePackageClasses = MessageRepository.class)
    static class TestApplication {
    }
}
