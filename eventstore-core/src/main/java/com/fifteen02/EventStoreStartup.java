package com.fifteen02;

import com.fifteen02.eventstore.service.CorrelationIdService;
import com.fifteen02.eventstore.service.MessageIngestionService;
import com.fifteen02.eventstore.service.PersistorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;
import tools.jackson.databind.ObjectMapper;

/**
 * Persists a {@code StartMarker} message (user {@code system}, empty body) every time EventStore
 * starts, and makes sure the realtime (PostgreSQL) persister state exists.
 */
@Component
@Order(2)
public class EventStoreStartup implements ApplicationRunner {

  private static final Logger log = LoggerFactory.getLogger(EventStoreStartup.class);

  private final MessageIngestionService messageIngestionService;
  private final CorrelationIdService correlationIdService;
  private final PersistorService persistorService;
  private final ObjectMapper objectMapper;

  public EventStoreStartup(MessageIngestionService messageIngestionService, CorrelationIdService correlationIdService,
      PersistorService persistorService, ObjectMapper objectMapper) {
    this.messageIngestionService = messageIngestionService;
    this.correlationIdService = correlationIdService;
    this.persistorService = persistorService;
    this.objectMapper = objectMapper;
  }

  // Not @Transactional: the ingestion path commits its own transaction before it publishes the new id.
  // The web server already takes input, so the marker goes through the same lock as client messages.
  @Override
  public void run(ApplicationArguments args) {
    messageIngestionService.ingestSystemMessage(new MessageIngestionService.MessageInput(
        "StartMarker", objectMapper.createObjectNode(), "system", null, null, null, null, null, null, null));
    persistorService.ensureRealtimePersister();
    log.info("Persisted StartMarker with correlationId={}", correlationIdService.getCorrelationId());
  }
}
