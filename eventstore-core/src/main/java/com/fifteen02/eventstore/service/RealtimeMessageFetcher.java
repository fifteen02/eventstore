package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.repo.MessageRepository;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class RealtimeMessageFetcher implements MessageFetcher {

    public static final String NAME = "writer-postgres";

    private final MessageRepository messageRepository;

    public RealtimeMessageFetcher(MessageRepository messageRepository) {
        this.messageRepository = messageRepository;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<Message> getMessages(long fromInclusive, long toInclusive) {
        return messageRepository.findRange(fromInclusive, toInclusive);
    }

}
