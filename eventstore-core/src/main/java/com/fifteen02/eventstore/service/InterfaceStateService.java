package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.InterfaceStateEntity;
import com.fifteen02.eventstore.repo.InterfaceStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.LocalDateTime;

@Service
public class InterfaceStateService {

  private static final Logger logger = LoggerFactory.getLogger(InterfaceStateService.class);
  private final InterfaceStateRepository repo;

  public InterfaceStateService(InterfaceStateRepository repo) {
    this.repo = repo;
  }

  @Transactional
  public InterfaceStateEntity ensure(String name) {
    return repo.findById(name).orElseGet(() -> {
      InterfaceStateEntity e = new InterfaceStateEntity();
      e.setName(name);
      e.setLastAcknowledgedId(0L);
      e.setRunning(true);
      return repo.save(e);
    });
  }

  @Transactional
  public void updateAcknowledged(String name, long lastIndex) {
    InterfaceStateEntity e = ensure(name);
    long newVal = Math.max(e.getLastAcknowledgedId(), lastIndex);
    e.setLastAcknowledgedId(newVal);
    repo.saveAndFlush(e);
  }

  public boolean isRunning(String name) {
    return repo.findById(name).map(InterfaceStateEntity::isRunning).orElse(true);
  }

  public long getLastAck(String name) {
    return repo.findById(name).map(InterfaceStateEntity::getLastAcknowledgedId).orElse(0L);
  }

  /**
   * Updates the last processed index for a specific consumer service and message
   * type.
   * This method is called when processing acknowledgments from consumer services.
   *
   * @param consumerService     the name of the consumer service
   * @param messageType         the type of message that was processed
   * @param index               the eventstore index that was processed
   * @param acknowledgementDate the date when the message was acknowledged
   */
  @Transactional
  public void updateLastProcessedIndex(String consumerService, String messageType, Long index,
      LocalDateTime acknowledgementDate) {
    String interfaceName = consumerService + ":" + messageType;

    logger.debug("Updating last processed index for interface: {} to index: {}", interfaceName, index);

    InterfaceStateEntity entity = ensure(interfaceName);

    // Only update if the new index is greater than the current one
    if (index > entity.getLastAcknowledgedId()) {
      entity.setLastAcknowledgedId(index);
      repo.save(entity);

      logger.debug("Updated last processed index for interface: {} to index: {} (acknowledged at: {})",
          interfaceName, index, acknowledgementDate);
    } else {
      logger.debug("Skipping update for interface: {} - index {} is not greater than current {}",
          interfaceName, index, entity.getLastAcknowledgedId());
    }
  }

  /**
   * Gets the last processed index for a specific consumer service and message
   * type.
   * This can be used to determine replay starting points.
   *
   * @param consumerService the name of the consumer service
   * @param messageType     the type of message
   * @return the last processed index, or 0 if none found
   */
  public long getLastProcessedIndex(String consumerService, String messageType) {
    String interfaceName = consumerService + ":" + messageType;
    return getLastAck(interfaceName);
  }
}
