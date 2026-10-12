package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.InterfaceStateRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

@Service
public class CatchUpPlannerService {

    private static final Logger LOG = LoggerFactory.getLogger(CatchUpPlannerService.class);

    private final InterfaceStateRepository interfaceStateRepository;
    private final PersisterStateRepository persistorState;
    private final IndexService indexService;
    private final List<MessageFetcher> fetchers;
    private final int maxSegmentSize;

    public CatchUpPlannerService(
            InterfaceStateRepository interfaceStateRepository,
            PersisterStateRepository persistorState,
            IndexService indexService,
            List<MessageFetcher> fetchers,
            @Value("${eventstore.catchup.max-segment-size:1000}") int maxSegmentSize
    ) {
        this.interfaceStateRepository = interfaceStateRepository;
        this.persistorState = persistorState;
        this.indexService = indexService;
        this.fetchers = fetchers;
        this.maxSegmentSize = maxSegmentSize;
    }

    public record Segment(String persistorId, long fromInclusive, long toInclusive, Supplier<List<Message>> messages) {
    }

    public record Plan(long lastAck, long target, List<Segment> segments) {
    }

    /**
     * Build a catch-up plan based on priority:
     * - left bound = interface last_acknowledged_id + 1
     * - right bound = realtime.index (storage HWM)
     * - cover [left.right] using readable persistors ordered by priority desc
     * - clip each segment by [startIndex..index]
     * - if nothing but realtime exists, it will be used naturally
     */
    @Transactional(readOnly = true)
    public Optional<Plan> plan(String interfaceName) {
        var interfaceState = interfaceStateRepository.findById(interfaceName)
                .orElseThrow(() -> new IllegalStateException("Missing interface state: " + interfaceName));

        Long rtLast = indexService.current().value();
        if (rtLast == null || rtLast <= interfaceState.getLastAcknowledgedId()) {
            return Optional.empty(); // no lag
        }

        long left = interfaceState.getLastAcknowledgedId() + 1;
        long target = rtLast;
        return buildPlan(interfaceState.getLastAcknowledgedId(), left, target);
    }

    /**
     * Build a catch-up plan from an explicit index, without requiring interface state.
     * Intended for request-scoped streaming interfaces.
     */
    @Transactional(readOnly = true)
    public Optional<Plan> planFromStartInclusive(long fromInclusive) {
        long left = Math.max(1L, fromInclusive);
        Long rtLast = indexService.current().value();
        if (rtLast == null || rtLast < left) {
            return Optional.empty();
        }
        return buildPlan(left - 1L, left, rtLast);
    }

    /**
     * Build a catch-up plan from an explicit start and end index.
     * Intended for bounded request-scoped streaming interfaces.
     */
    @Transactional(readOnly = true)
    public Optional<Plan> planFromStartInclusiveToEndInclusive(long fromInclusive, long endInclusive) {
        long left = Math.max(1L, fromInclusive);
        Long rtLast = indexService.current().value();
        if (rtLast == null) {
            return Optional.empty();
        }

        long target = Math.min(rtLast, endInclusive);
        if (target < left) {
            return Optional.empty();
        }

        return buildPlan(left - 1L, left, target);
    }

    private Optional<Plan> buildPlan(long lastAck, long left, long target) {
        // Gather all readable persistors (archives + realtimeState), priority-desc
        var all = persistorState.findAllReadableOrderByPriorityDesc().stream().peek(x -> {
            if (x.isRealtime()) {
                x.setLastIndex(target);
            }
        }).toList();


        // Build coverage by walking priority order and taking what we need from each
        var segments = new ArrayList<Segment>();
        long cursor = left;

        while (cursor <= target) {
            // Create final variable for lambda expressions
            final long currentCursor = cursor;

            // Find the highest-priority persistor that can cover 'cursor'
            var covering = all.stream()
                    .filter(p -> p.getStartIndex() != null && p.getLastIndex() != null)
                    .filter(p -> p.getStartIndex() <= currentCursor && p.getLastIndex() >= currentCursor)
                    .findFirst();

            if (covering.isEmpty()) {
                long firstGapEnd = all.stream()
                        .map(PersisterStateEntity::getStartIndex)
                        .filter(Objects::nonNull)
                        .filter(s -> s > currentCursor)
                        .min(Long::compareTo)
                        .map(nextStart -> Math.min(nextStart - 1, target))
                        .orElse(target);
                throw new IllegalStateException("Missing coverage from " + currentCursor + " to " + firstGapEnd);
            }

            var p = covering.get();
            long from = cursor;
            long maxByPersistor = p.getLastIndex();
            long maxByBatchSize = from + maxSegmentSize - 1L;
            long to = Math.min(target, Math.min(maxByPersistor, maxByBatchSize));
            segments.add(new Segment(p.getId(), from, to, () -> {
                LOG.info("Loading messages from {} [{}..{}]", p.getId(), from, to);
                return getById(p.getId()).getMessages(from, to);
            }));
            cursor = to + 1;
        }

        if (segments.isEmpty()) return Optional.empty();
        return Optional.of(new Plan(lastAck, target, segments));
    }

    private MessageFetcher getById(String id) {
        return fetchers.stream()
                .filter(f -> f.name().equals(id))
                .findAny()
                .orElseThrow(() -> new IllegalStateException("No MessageFetcher found by ID: " + id));
    }
}
