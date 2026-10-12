package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Service
public class PersistorService {

  private static final int REALTIME_PRIORITY = Integer.MAX_VALUE;

  private final PersisterStateRepository repo;

  public PersistorService(PersisterStateRepository repo) {
    this.repo = repo;
  }

  @Transactional
  public PersisterStateEntity ensurePersister(String id, boolean realtime, int priority) {
    Optional<PersisterStateEntity> existingWriter = repo.findById(id);
    return existingWriter.orElseGet(() -> {
      PersisterStateEntity e = new PersisterStateEntity();
      e.setId(id);
      e.setStartIndex(0L);
      e.setLastIndex(0L);
      e.setCanRead(true);
      e.setCanWrite(true);
      e.setRealtime(realtime);
      e.setPriority(priority);
      return repo.save(e);
    });
  }

  @Transactional
  public PersisterStateEntity ensureRealtimePersister() {
    return repo.findById(RealtimeMessageFetcher.NAME).orElseGet(() -> {
      PersisterStateEntity entity = new PersisterStateEntity();
      entity.setId(RealtimeMessageFetcher.NAME);
      entity.setStartIndex(1L);
      entity.setLastIndex(null);
      entity.setCanRead(true);
      entity.setCanWrite(true);
      entity.setRealtime(true);
      entity.setPriority(REALTIME_PRIORITY);
      return repo.save(entity);
    });
  }

  @Transactional
  public void updatePersister(String id, long lastIndex) {
    repo.findById(id).ifPresent(e -> {
      e.setLastIndex(lastIndex);
      repo.save(e);
    });
  }
}
