package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Comparator;
import java.util.List;

@Service
@ConditionalOnProperty(name = "eventstore.big-query.enabled", havingValue = "true")
public class RealtimeRetentionJob {

    private static final Logger logger = LoggerFactory.getLogger(RealtimeRetentionJob.class);

    private final PersisterStateRepository persisterStateRepository;
    private final MessageRepository messageRepository;
    private final Clock clock;
    private final long deleteBatchSize;
    private final long retentionDays;
    private final boolean retentionEnabled;

    public RealtimeRetentionJob(PersisterStateRepository persisterStateRepository,
                                MessageRepository messageRepository,
                                Clock clock,
                                @Value("${eventstore.realtime-retention.delete-batch-size}") long deleteBatchSize,
                                @Value("${eventstore.realtime-retention.retention-days}") long retentionDays,
                                @Value("${eventstore.realtime-retention.enabled}") boolean retentionEnabled) {
        this.persisterStateRepository = persisterStateRepository;
        this.messageRepository = messageRepository;
        this.clock = clock;
        this.deleteBatchSize = deleteBatchSize;
        this.retentionDays = retentionDays;
        this.retentionEnabled = retentionEnabled;
    }

    @Transactional
    @Scheduled(cron = "${eventstore.realtime-retention.cron}")
    public void trimRealtime() {
        if (!retentionEnabled) {
            logger.info("Realtime retention is disabled; skipping retention trim.");
            return;
        }
        if (retentionDays <= 0) {
            logger.warn("Realtime retention days {} is not positive; skipping retention trim.", retentionDays);
            return;
        }

        var realtimeOpt = persisterStateRepository.findRealtime();
        if (realtimeOpt.isEmpty()) {
            logger.debug("No realtime persister_state found; skipping retention trim.");
            return;
        }

        var realtime = realtimeOpt.get();
        Long startIndex = realtime.getStartIndex();
        if (startIndex == null) {
            logger.warn("Realtime persister_state has null start_index; skipping retention trim.");
            return;
        }

        Instant cutoff = floorToUtcMidnight(Instant.now(clock)).minus(Duration.ofDays(retentionDays));
        Long newStartIndex = messageRepository.findMinIdFromTimestamp(cutoff);
        if (newStartIndex == null || newStartIndex <= 0L) {
            logger.debug("No messages found at or after cutoff {}; skipping retention trim.", cutoff);
            return;
        }

        long deleteEnd = newStartIndex - 1;
        if (deleteEnd < startIndex) {
            logger.debug("Realtime start_index {} already satisfies retention cutoff {}.", startIndex, cutoff);
            return;
        }

        List<PersisterStateEntity> others = persisterStateRepository.findAllNonRealtime();
        if (!isRangeCovered(startIndex, deleteEnd, others)) {
            logger.warn("Delete range {}..{} is not fully covered by other persister_state entries; skipping delete.",
                    startIndex, deleteEnd);
            return;
        }

        int deleted = deleteInBatches(startIndex, deleteEnd);
        realtime.setStartIndex(newStartIndex);
        persisterStateRepository.save(realtime);

        logger.info("Trimmed realtime messages before {} (deleted {} rows). New start_index is {}.",
                cutoff, deleted, newStartIndex);
    }

    /**
     * Deletes by id, never by timestamp: producers may send backdated timestamps, so rows older than
     * the cutoff can sit above {@code deleteEnd}, outside the range checked for archive coverage.
     */
    private int deleteInBatches(long fromInclusive, long toInclusive) {
        if (deleteBatchSize <= 0) {
            logger.warn("Delete batch size {} is not positive; skipping deletion.", deleteBatchSize);
            return 0;
        }

        int deleted = 0;
        int pageSize = (int) Math.min(deleteBatchSize, Integer.MAX_VALUE);
        long cursor = fromInclusive;
        List<Long> ids = messageRepository.findIdsInRange(cursor, toInclusive, PageRequest.of(0, pageSize));
        while (!ids.isEmpty()) {
            deleted += messageRepository.deleteByIdIn(ids);
            cursor = ids.getLast() + 1;
            if (cursor > toInclusive) {
                break;
            }
            ids = messageRepository.findIdsInRange(cursor, toInclusive, PageRequest.of(0, pageSize));
        }
        return deleted;
    }

    private boolean isRangeCovered(long deleteStart, long deleteEnd, List<PersisterStateEntity> others) {
        List<Range> ranges = others.stream()
                .filter(p -> p.getStartIndex() != null && p.getLastIndex() != null)
                .map(p -> new Range(p.getStartIndex(), p.getLastIndex()))
                .filter(r -> r.end() >= deleteStart && r.start() <= deleteEnd)
                .sorted(Comparator.comparingLong(Range::start))
                .toList();

        if (ranges.isEmpty()) {
            return false;
        }

        long cursor = deleteStart;
        for (Range range : ranges) {
            if (range.end() < cursor) {
                continue;
            }
            if (range.start() > cursor) {
                return false;
            }
            cursor = Math.max(cursor, range.end() + 1);
            if (cursor > deleteEnd) {
                return true;
            }
        }
        return cursor > deleteEnd;
    }

    record Range(long start, long end) {
    }

    private Instant floorToUtcMidnight(Instant instant) {
        return ZonedDateTime.ofInstant(instant, ZoneOffset.UTC)
                .toLocalDate()
                .atStartOfDay(ZoneOffset.UTC)
                .toInstant();
    }
}
