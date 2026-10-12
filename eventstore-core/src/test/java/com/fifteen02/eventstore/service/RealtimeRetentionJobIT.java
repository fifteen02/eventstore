package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import com.fifteen02test.eventstore.RealtimeRetentionJobTestApplication;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase;
import org.springframework.boot.jdbc.test.autoconfigure.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.data.jpa.test.autoconfigure.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@DataJpaTest(showSql = false, properties = {
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.url=jdbc:h2:mem:realtime-retention;MODE=PostgreSQL;DB_CLOSE_DELAY=-1;DATABASE_TO_LOWER=TRUE;INIT=CREATE DOMAIN IF NOT EXISTS JSONB AS JSON",
        "spring.datasource.username=sa",
        "spring.datasource.password=",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.jpa.properties.hibernate.format_sql=false",
        "spring.jpa.show-sql=false",
        "eventstore.realtime-retention.delete-batch-size=123",
        "eventstore.realtime-retention.retention-days=7",
        "eventstore.realtime-retention.enabled=true"
})
@ContextConfiguration(classes = RealtimeRetentionJobTestApplication.class)
@TestExecutionListeners(
        listeners = {
                DependencyInjectionTestExecutionListener.class,
                DirtiesContextTestExecutionListener.class,
                TransactionalTestExecutionListener.class
        },
        mergeMode = TestExecutionListeners.MergeMode.REPLACE_DEFAULTS
)
@AutoConfigureTestDatabase(replace = Replace.NONE)
class RealtimeRetentionJobIT {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Autowired
    private RealtimeRetentionJob service;

    @Autowired
    private MessageRepository messageRepository;

    @Autowired
    private PersisterStateRepository persisterStateRepository;

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - realtime persister starts at 1 and ends at 6
     * - archive persisters fully cover the safe delete prefix
     * - messages 1..6 span both sides of the midnight-flattened cutoff
     * <p>
     * Expectations:
     * - messages older than the midnight cutoff are deleted
     * - the realtime start index advances to the first retained message id
     * - the realtime last index remains unchanged
     */
    @Test
    void deletesSafePrefixAndAdvancesRealtimeStartIndex() {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 6L),
                archive("archive-1", 1L, 3L),
                archive("archive-2", 4L, 6L)
        ));

        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-22T08:00:00Z")),
                message(2L, Instant.parse("2026-03-23T08:00:00Z")),
                message(3L, Instant.parse("2026-03-24T00:00:00Z")),
                message(4L, Instant.parse("2026-03-24T07:59:59Z")),
                message(5L, Instant.parse("2026-03-25T08:00:00Z")),
                message(6L, Instant.parse("2026-03-26T08:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(3L, 4L, 5L, 6L);
        assertPersisterStates(
                realtime(3L, 6L),
                List.of(
                        archive("archive-1", 1L, 3L),
                        archive("archive-2", 4L, 6L)
                )
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - realtime persister already starts at the first retained message
     * - all messages are at or after the floored cutoff
     * <p>
     * Expectations:
     * - no messages are deleted
     * - the realtime state remains unchanged
     */
    @Test
    void skipsWhenAllMessagesAreAlreadyWithinRetentionWindow() {
        persisterStateRepository.saveAll(List.of(
                realtime(4L, 6L),
                archive("archive-1", 4L, 6L)
        ));

        messageRepository.saveAll(List.of(
                message(4L, Instant.parse("2026-03-24T08:00:00Z")),
                message(5L, Instant.parse("2026-03-25T08:00:00Z")),
                message(6L, Instant.parse("2026-03-26T08:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(4L, 5L, 6L);
        assertPersisterStates(
                realtime(4L, 6L),
                List.of(archive("archive-1", 4L, 6L))
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - realtime persister starts at 1 and ends at 6
     * - archive persisters leave a gap after the safe delete prefix
     * - messages 1..6 still span both sides of the midnight cutoff
     * <p>
     * Expectations:
     * - the safe prefix is deleted
     * - the realtime state advances to the first retained id
     */
    @Test
    void deletesOnlySafePrefixWhenCoverageStopsAfterIt() {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 6L),
                archive("archive-1", 1L, 2L),
                archive("archive-2", 4L, 6L)
        ));

        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-22T08:00:00Z")),
                message(2L, Instant.parse("2026-03-23T08:00:00Z")),
                message(3L, Instant.parse("2026-03-24T00:00:00Z")),
                message(4L, Instant.parse("2026-03-24T07:59:59Z")),
                message(5L, Instant.parse("2026-03-25T08:00:00Z")),
                message(6L, Instant.parse("2026-03-26T08:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(3L, 4L, 5L, 6L);
        assertPersisterStates(
                realtime(3L, 6L),
                List.of(
                        archive("archive-1", 1L, 2L),
                        archive("archive-2", 4L, 6L)
                )
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - realtime persister starts before the retained window
     * - no archive persisters exist
     * - messages exist on both sides of the cutoff
     * <p>
     * Expectations:
     * - the job deletes nothing because there is no safe coverage
     * - the realtime state remains unchanged
     */
    @Test
    void skipsWhenNoArchivesExist() {
        persisterStateRepository.save(realtime(1L, 3L));

        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-22T08:00:00Z")),
                message(2L, Instant.parse("2026-03-23T08:00:00Z")),
                message(3L, Instant.parse("2026-03-24T00:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(1L, 2L, 3L);
        assertPersisterStates(
                realtime(1L, 3L),
                List.of()
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - realtime persister exists but there are no messages
     * <p>
     * Expectations:
     * - the job skips without deleting
     * - the realtime state remains unchanged
     */
    @Test
    void skipsWhenNoMessagesExist() {
        persisterStateRepository.save(realtime(1L, 3L));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).isEmpty();
        assertPersisterStates(
                realtime(1L, 3L),
                List.of()
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - all messages are already at or after the cutoff
     * - archive coverage exists, but no prefix needs trimming
     * <p>
     * Expectations:
     * - the job skips because there is nothing to delete
     * - the realtime state remains unchanged
     */
    @Test
    void skipsWhenCutoffFallsBeforeAllMessages() {
        persisterStateRepository.saveAll(List.of(
                realtime(4L, 6L),
                archive("archive-1", 4L, 6L)
        ));

        messageRepository.saveAll(List.of(
                message(4L, Instant.parse("2026-03-24T00:00:00Z")),
                message(5L, Instant.parse("2026-03-25T08:00:00Z")),
                message(6L, Instant.parse("2026-03-26T08:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(4L, 5L, 6L);
        assertPersisterStates(
                realtime(4L, 6L),
                List.of(archive("archive-1", 4L, 6L))
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - message 1 lands just before midnight by one millisecond
     * - message 2 lands exactly on the midnight cutoff
     * - archive coverage fully covers the deletable prefix
     * <p>
     * Expectations:
     * - the pre-midnight message is deleted
     * - the message at exactly midnight is retained
     * - the realtime state advances to the retained id
     */
    @Test
    void retainsMessagesExactlyAtMidnightCutoff() {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 2L),
                archive("archive-1", 1L, 2L)
        ));

        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-23T23:59:59.999Z")),
                message(2L, Instant.parse("2026-03-24T00:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(2L);
        assertPersisterStates(
                realtime(2L, 2L),
                List.of(archive("archive-1", 1L, 2L))
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - delete batch size is 2
     * - realtime persister starts at 1 and ends at 7
     * - archive coverage fully covers the delete range
     * - four messages fall before the cutoff and one message is retained
     * <p>
     * Expectations:
     * - the job deletes the old rows in two batches, not one oversized batch
     * - the realtime state advances to the first retained id
     * - the retained messages remain in the database
     */
    @Test
    void deletesMultipleBatchesWhenDeleteRangeExceedsBatchSize() {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 7L),
                archive("archive-1", 1L, 2L),
                archive("archive-2", 3L, 4L)
        ));

        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-20T08:00:00Z")),
                message(2L, Instant.parse("2026-03-21T08:00:00Z")),
                message(3L, Instant.parse("2026-03-22T08:00:00Z")),
                message(4L, Instant.parse("2026-03-23T08:00:00Z")),
                message(5L, Instant.parse("2026-03-24T00:00:00Z")),
                message(6L, Instant.parse("2026-03-25T08:00:00Z")),
                message(7L, Instant.parse("2026-03-26T08:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(5L, 6L, 7L);
        assertPersisterStates(
                realtime(5L, 7L),
                List.of(
                        archive("archive-1", 1L, 2L),
                        archive("archive-2", 3L, 4L)
                )
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - delete batch size is 2
     * - realtime persister starts at 1 and ends at 10_000
     * - one archive spans the entire deletable prefix
     * - 10_000 messages exist, with ids 1..5_000 before the cutoff and 5_001..10_000 at or after it
     * <p>
     * Expectations:
     * - the job deletes 5_000 old rows in repeated batches
     * - the realtime state advances to the first retained id
     * - the retained half of the dataset remains untouched
     */
    @Test
    void deletesTenThousandMessagesInRepeatedBatches() {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 10_000L),
                archive("archive-1", 1L, 5_000L)
        ));

        messageRepository.saveAll(generateMessages(10_000));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactlyElementsOf(longRange(5_001L, 10_000L));
        assertPersisterStates(
                realtime(5_001L, 10_000L),
                List.of(archive("archive-1", 1L, 5_000L))
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - archive ranges overlap and together cover the delete prefix
     * - messages 1..6 span both sides of the cutoff
     * <p>
     * Expectations:
     * - the safe prefix is deleted
     * - the realtime state advances to the first retained id
     */
    @Test
    void deletesSafePrefixWithOverlappingArchives() {
        seedRealtimeAndMessages(List.of(
                archive("archive-1", 1L, 4L),
                archive("archive-2", 3L, 6L)
        ));

        service.trimRealtime();

        assertTrimmedTo(
                List.of(3L, 4L, 5L, 6L),
                realtime(3L, 6L),
                List.of(
                        archive("archive-1", 1L, 4L),
                        archive("archive-2", 3L, 6L)
                )
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - archive ranges are unsorted in the database input
     * - messages 1..6 span both sides of the cutoff
     * <p>
     * Expectations:
     * - the safe prefix is deleted
     * - archive ordering does not affect the result
     */
    @Test
    void deletesSafePrefixWithUnsortedArchives() {
        seedRealtimeAndMessages(List.of(
                archive("archive-2", 4L, 6L),
                archive("archive-1", 1L, 3L)
        ));

        service.trimRealtime();

        assertTrimmedTo(
                List.of(3L, 4L, 5L, 6L),
                realtime(3L, 6L),
                List.of(
                        archive("archive-2", 4L, 6L),
                        archive("archive-1", 1L, 3L)
                )
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - archive ranges are duplicated
     * - messages 1..6 span both sides of the cutoff
     * <p>
     * Expectations:
     * - the safe prefix is deleted
     * - duplicate coverage does not change the result
     */
    @Test
    void deletesSafePrefixWithDuplicateArchives() {
        seedRealtimeAndMessages(List.of(
                archive("archive-1", 1L, 3L),
                archive("archive-2", 1L, 3L)
        ));

        service.trimRealtime();

        assertTrimmedTo(
                List.of(3L, 4L, 5L, 6L),
                realtime(3L, 6L),
                List.of(
                        archive("archive-1", 1L, 3L),
                        archive("archive-2", 1L, 3L)
                )
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - archive coverage has a gap exactly at the boundary that would need to be deleted
     * - messages 1..6 span both sides of the cutoff
     * <p>
     * Expectations:
     * - no deletion happens because the delete prefix is not fully covered
     * - the realtime state remains unchanged
     */
    @Test
    void skipsWhenArchiveGapHitsDeleteBoundary() {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 6L),
                archive("archive-1", 1L, 2L),
                archive("archive-2", 3L, 3L),
                archive("archive-3", 5L, 6L)
        ));

        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-22T08:00:00Z")),
                message(2L, Instant.parse("2026-03-23T08:00:00Z")),
                message(3L, Instant.parse("2026-03-23T12:00:00Z")),
                message(4L, Instant.parse("2026-03-23T23:59:59Z")),
                message(5L, Instant.parse("2026-03-24T08:00:00Z")),
                message(6L, Instant.parse("2026-03-25T08:00:00Z"))
        ));

        service.trimRealtime();

        assertTrimmedTo(
                List.of(1L, 2L, 3L, 4L, 5L, 6L),
                realtime(1L, 6L),
                List.of(
                        archive("archive-1", 1L, 2L),
                        archive("archive-2", 3L, 3L),
                        archive("archive-3", 5L, 6L)
                )
        );
    }

    /**
     * Setup:
     * - fixed clock is 2026-03-31T08:00:00Z
     * - retention-days is 7, so the floored cutoff is 2026-03-24T00:00:00Z
     * - archive coverage stops at id 2
     * - message 5 has a producer-supplied timestamp before the cutoff, although its id is above the first
     *   retained id 3
     * <p>
     * Expectations:
     * - only ids 1..2 are deleted; the backdated row 5 is kept because nothing archived it
     * - the realtime state advances to the first retained id
     */
    @Test
    void keepsBackdatedMessagesAboveTheDeleteRange() {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 6L),
                archive("archive-1", 1L, 2L)
        ));

        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-22T08:00:00Z")),
                message(2L, Instant.parse("2026-03-23T08:00:00Z")),
                message(3L, Instant.parse("2026-03-24T00:00:00Z")),
                message(4L, Instant.parse("2026-03-25T08:00:00Z")),
                message(5L, Instant.parse("2026-03-01T08:00:00Z")),
                message(6L, Instant.parse("2026-03-26T08:00:00Z"))
        ));

        service.trimRealtime();

        assertThat(messageRepository.findAllIdsOrdered()).containsExactly(3L, 4L, 5L, 6L);
        assertPersisterStates(
                realtime(3L, 6L),
                List.of(archive("archive-1", 1L, 2L))
        );
    }

    private void seedRealtimeAndMessages(List<PersisterStateEntity> archives) {
        persisterStateRepository.saveAll(List.of(
                realtime(1L, 6L)
        ));
        persisterStateRepository.saveAll(archives);
        messageRepository.saveAll(List.of(
                message(1L, Instant.parse("2026-03-22T08:00:00Z")),
                message(2L, Instant.parse("2026-03-23T08:00:00Z")),
                message(3L, Instant.parse("2026-03-24T00:00:00Z")),
                message(4L, Instant.parse("2026-03-24T07:59:59Z")),
                message(5L, Instant.parse("2026-03-25T08:00:00Z")),
                message(6L, Instant.parse("2026-03-26T08:00:00Z"))
        ));
    }

    private void assertTrimmedTo(List<Long> expectedIds, PersisterStateEntity expectedRealtime, List<PersisterStateEntity> expectedArchives) {
        assertThat(messageRepository.findAllIdsOrdered()).containsExactlyElementsOf(expectedIds);
        assertPersisterStates(expectedRealtime, expectedArchives);
    }

    private void assertPersisterStates(PersisterStateEntity expectedRealtime, List<PersisterStateEntity> expectedArchives) {
        PersisterStateEntity realtime = persisterStateRepository.findRealtime().orElseThrow();
        assertThat(realtime.getStartIndex()).isEqualTo(expectedRealtime.getStartIndex());
        assertThat(realtime.getLastIndex()).isEqualTo(expectedRealtime.getLastIndex());

        List<PersisterStateEntity> archives = persisterStateRepository.findAllNonRealtime();
        assertThat(archives).hasSize(expectedArchives.size());
        assertThat(archives)
                .extracting(PersisterStateEntity::getId, PersisterStateEntity::getStartIndex, PersisterStateEntity::getLastIndex)
                .containsExactlyInAnyOrderElementsOf(expectedArchives.stream()
                        .map(p -> Tuple.tuple(p.getId(), p.getStartIndex(), p.getLastIndex()))
                        .toList());
    }

    private static List<Message> generateMessages(long count) {
        return longRange(1L, count).stream()
                .map(id -> message(id, id <= 5_000L
                        ? Instant.parse("2026-03-20T08:00:00Z").plusSeconds(id)
                        : Instant.parse("2026-03-24T00:00:00Z").plusSeconds(id - 5_001L)))
                .toList();
    }

    private static List<Long> longRange(long fromInclusive, long toInclusive) {
        return LongStream.rangeClosed(fromInclusive, toInclusive)
                .boxed()
                .toList();
    }

    private static Message message(long id, Instant timestamp) {
        Message message = new Message();
        message.setId(id);
        message.setContent(OBJECT_MAPPER.createObjectNode().put("contentId", id));
        message.setTimestamp(timestamp);
        message.setType("ProfileUpdatedEvent");
        return message;
    }

    private static PersisterStateEntity realtime(Long startIndex, Long lastIndex) {
        PersisterStateEntity entity = new PersisterStateEntity();
        entity.setId("realtime");
        entity.setStartIndex(startIndex);
        entity.setLastIndex(lastIndex);
        entity.setRealtime(true);
        entity.setCanRead(true);
        entity.setCanWrite(true);
        entity.setPriority(100);
        return entity;
    }

    private static PersisterStateEntity archive(String id, long startIndex, long lastIndex) {
        PersisterStateEntity entity = new PersisterStateEntity();
        entity.setId(id);
        entity.setStartIndex(startIndex);
        entity.setLastIndex(lastIndex);
        entity.setRealtime(false);
        entity.setCanRead(true);
        entity.setCanWrite(false);
        entity.setPriority(10);
        return entity;
    }
}
