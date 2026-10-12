package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;

import java.util.List;

public interface GoogleBigQueryClient {

    List<Message> load(long fromInclusive, long toInclusive) throws InterruptedException;

}
