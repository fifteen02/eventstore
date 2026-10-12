package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;

import java.util.List;

public interface MessageFetcher {

    String name();

    List<Message> getMessages(long fromInclusive, long toInclusive);
}
