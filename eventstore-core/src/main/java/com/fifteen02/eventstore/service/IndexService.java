package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.to.Index;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.atomic.AtomicLong;

@Service
public class IndexService {
    private final MessageRepository messageRepository;
    private final AtomicLong committedId = new AtomicLong();

    public IndexService(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    @PostConstruct
    @Transactional(readOnly = true)
    public void init() {
        Long maxId = messageRepository.findMaxId();
        long initialValue = maxId == null ? 0L : maxId;
        committedId.set(initialValue);
    }

    public Index current() {
        return new Index(committedId.get());
    }

    public Index nextCandidate() {
        return new Index(committedId.get() + 1L);
    }

    public void markCommitted(long id) {
        committedId.updateAndGet(current -> Math.max(current, id));
    }
}
