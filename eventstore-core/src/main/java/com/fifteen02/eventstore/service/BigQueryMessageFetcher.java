package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

@Component
@ConditionalOnProperty(name = "eventstore.big-query.enabled", havingValue = "true")
public class BigQueryMessageFetcher implements MessageFetcher {

    private static final Logger LOG =
            LoggerFactory.getLogger(BigQueryMessageFetcher.class);

    private final GoogleBigQueryClient googleBigQueryClient;

    public BigQueryMessageFetcher(GoogleBigQueryClient googleBigQueryClient) {
        this.googleBigQueryClient = googleBigQueryClient;
    }

    @Override
    public String name() {
        return "google-big-query";
    }

    @Override
    public List<Message> getMessages(long fromInclusive, long toInclusive) {
        try {
            List<Message> messages = googleBigQueryClient.load(fromInclusive, toInclusive);
            Set<Long> actualIds = messages.stream()
                    .map(Message::getId)
                    .collect(Collectors.toSet());

            Set<Long> expectedIds = LongStream
                    .rangeClosed(fromInclusive, toInclusive)
                    .boxed()
                    .collect(Collectors.toSet());

            if (!actualIds.equals(expectedIds)) {
                throw new IllegalStateException(
                        "Returned IDs do not match expected range [" + fromInclusive + ", " + toInclusive + "]"
                );
            }
            if (messages.size() != expectedIds.size()) {
                throw new IllegalStateException("Some multiple messages detected!");
            }
            LOG.info(
                    "Successfully fetched messages [{} - {}]",
                    fromInclusive,
                    toInclusive
            );
            return messages;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
