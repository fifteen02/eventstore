package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.controller.StreamRequestParameters;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The HTTP streams ({@code /api/events/stream}) that are open right now, and how far through the log
 * each one has got. The server keeps no position for a stream once it closes; this only lets the
 * debug console show who is connected.
 */
@Component
public class OpenStreams {

    private final Map<String, Stream> streams = new ConcurrentHashMap<>();

    public Stream open(String id, StreamRequestParameters parameters, String client, String remoteAddress) {
        long start = parameters.startIndex() == null ? 1L : parameters.startIndex();
        var stream = new Stream(id, parameters, client, remoteAddress, Instant.now(), new AtomicLong(start - 1L));
        streams.put(id, stream);
        return stream;
    }

    public void close(String id) {
        streams.remove(id);
    }

    public List<Stream> list() {
        return streams.values().stream()
                .sorted(Comparator.comparing(Stream::openedAt))
                .toList();
    }

    /**
     * @param position the last index the stream has passed, whether or not its filters let the message
     *                 through
     */
    public record Stream(String id, StreamRequestParameters parameters, String client, String remoteAddress,
                         Instant openedAt, AtomicLong position) {

        public void observed(long index) {
            position.accumulateAndGet(index, Math::max);
        }
    }
}
