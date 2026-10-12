package com.fifteen02.eventstore.service;

import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Provides the current ES instance correlationId, generated on each restart.
 */
@Service
public class CorrelationIdService {

  private final String correlationId = UUID.randomUUID().toString();

  public String getCorrelationId() {
    return correlationId;
  }
}
