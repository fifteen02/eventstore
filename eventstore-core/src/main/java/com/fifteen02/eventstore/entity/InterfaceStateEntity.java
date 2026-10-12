package com.fifteen02.eventstore.entity;

import jakarta.persistence.*;
import java.time.Instant;

@Entity
@Table(name = "interface_state")
public class InterfaceStateEntity {

  @Id
  @Column(name = "name", nullable = false)
  private String name;

  @Column(name = "last_acknowledged_id", nullable = false)
  private Long lastAcknowledgedId = 0L;

  @Column(name = "is_running", nullable = false)
  private boolean running = true;

  @Column(name = "updated_at", nullable = false)
  private Instant updatedAt = Instant.now();

  @PrePersist
  @PreUpdate
  public void touch() {
    this.updatedAt = Instant.now();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Long getLastAcknowledgedId() {
    return lastAcknowledgedId;
  }

  public void setLastAcknowledgedId(Long lastAcknowledgedId) {
    this.lastAcknowledgedId = lastAcknowledgedId;
  }

  public boolean isRunning() {
    return running;
  }

  public void setRunning(boolean running) {
    this.running = running;
  }

  public Instant getUpdatedAt() {
    return updatedAt;
  }

  public void setUpdatedAt(Instant updatedAt) {
    this.updatedAt = updatedAt;
  }
}
