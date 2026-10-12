package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.MessageRepository;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@Execution(ExecutionMode.CONCURRENT)
class RealtimeRetentionJobTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2026-03-31T08:00:00Z"), ZoneOffset.UTC);
    private static final PageRequest PAGE = PageRequest.of(0, 200_000);

    @Mock
    private PersisterStateRepository persisterStateRepository;

    @Mock
    private MessageRepository messageRepository;

    private RealtimeRetentionJob service;

    @BeforeEach
    void setUp() {
        service = new RealtimeRetentionJob(persisterStateRepository, messageRepository, CLOCK, 200_000L, 7L, true);
    }

    @Test
    void skipsWhenRetentionDisabled() {
        service = new RealtimeRetentionJob(persisterStateRepository, messageRepository, CLOCK, 200_000L, 7L, false);

        service.trimRealtime();

        verifyNoInteractions(persisterStateRepository, messageRepository);
    }

    @Test
    void skipsWhenNoRealtimePersister() {
        service.trimRealtime();

        verify(persisterStateRepository, times(1)).findRealtime();
        verify(messageRepository, never()).findMinIdFromTimestamp(any());
        verify(messageRepository, never()).deleteByIdIn(anyList());
    }

    @Test
    void skipsWhenRealtimeStartIndexIsNull() {
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(null, 123L)));

        service.trimRealtime();

        verify(messageRepository, never()).findMinIdFromTimestamp(any());
        verify(messageRepository, never()).deleteByIdIn(anyList());
    }

    @Test
    void skipsWhenRetentionWindowContainsNoMessages() {
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(10L, 200L)));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(0L);

        service.trimRealtime();

        verify(messageRepository, times(1)).findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"));
        verify(messageRepository, never()).deleteByIdIn(anyList());
    }

    @Test
    void skipsWhenDeleteRangeWithinExistingStartIndex() {
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(10L, 200L)));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(9L);

        service.trimRealtime();

        verify(messageRepository, never()).deleteByIdIn(anyList());
    }

    @Test
    void skipsWhenNoOtherPersisterCoverageExists() {
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(0L, 2_000_000L)));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(1_000_000L);

        service.trimRealtime();

        verify(messageRepository, never()).deleteByIdIn(anyList());
    }

    @Test
    void deletesInBatchesAndUpdatesRealtimeStateUsingFirstRetainedId() {
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(0L, 1_500_000L)));
        when(persisterStateRepository.findAllNonRealtime()).thenReturn(List.of(
                Fixtures.archive("archive-1", 0L, 249_999L),
                Fixtures.archive("archive-2", 250_000L, 499_999L)
        ));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(500_000L);
        when(messageRepository.findIdsInRange(0L, 499_999L, PAGE)).thenReturn(List.of(1L, 2L));
        when(messageRepository.findIdsInRange(3L, 499_999L, PAGE)).thenReturn(List.of());

        service.trimRealtime();

        ArgumentCaptor<List<Long>> deletedIds = ArgumentCaptor.forClass((Class) List.class);
        verify(messageRepository, times(1)).deleteByIdIn(deletedIds.capture());
        assertThat(deletedIds.getValue()).containsExactly(1L, 2L);
        ArgumentCaptor<PersisterStateEntity> stateCaptor = ArgumentCaptor.forClass(PersisterStateEntity.class);
        verify(persisterStateRepository, times(1)).save(stateCaptor.capture());
        assertThat(stateCaptor.getValue().getStartIndex()).isEqualTo(500_000L);
        assertThat(stateCaptor.getValue().getLastIndex()).isEqualTo(1_500_000L);
    }

    @Test
    void updatesStateEvenWhenDeleteBatchSizeIsNonPositive() {
        service = new RealtimeRetentionJob(persisterStateRepository, messageRepository, CLOCK, 0L, 7L, true);
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(0L, 1_100_000L)));
        when(persisterStateRepository.findAllNonRealtime()).thenReturn(List.of(Fixtures.archive("archive-1", 0L, 200_000L)));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(100_000L);

        service.trimRealtime();

        verify(messageRepository, never()).deleteByIdIn(anyList());
        ArgumentCaptor<PersisterStateEntity> stateCaptor = ArgumentCaptor.forClass(PersisterStateEntity.class);
        verify(persisterStateRepository, times(1)).save(stateCaptor.capture());
        assertThat(stateCaptor.getValue().getStartIndex()).isEqualTo(100_000L);
        assertThat(stateCaptor.getValue().getLastIndex()).isEqualTo(1_100_000L);
    }

    @Test
    void retainsMessagesFromConfiguredCutoffDate() {
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(0L, 1_500_000L)));
        when(persisterStateRepository.findAllNonRealtime()).thenReturn(List.of(Fixtures.archive("archive-1", 0L, 300_000L)));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(300_000L);
        when(messageRepository.findIdsInRange(0L, 299_999L, PAGE)).thenReturn(List.of(1L));
        when(messageRepository.findIdsInRange(2L, 299_999L, PAGE)).thenReturn(List.of());

        service.trimRealtime();

        ArgumentCaptor<PersisterStateEntity> stateCaptor = ArgumentCaptor.forClass(PersisterStateEntity.class);
        verify(persisterStateRepository, times(1)).save(stateCaptor.capture());
        assertThat(stateCaptor.getValue().getStartIndex()).isEqualTo(300_000L);
    }

    @Test
    void floorsCurrentTimeToMidnightBeforeApplyingRetentionDays() {
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(0L, 1_500_000L)));
        when(persisterStateRepository.findAllNonRealtime()).thenReturn(List.of(Fixtures.archive("archive-1", 0L, 500_000L)));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(500_000L);
        when(messageRepository.findIdsInRange(0L, 499_999L, PAGE)).thenReturn(List.of(1L));
        when(messageRepository.findIdsInRange(2L, 499_999L, PAGE)).thenReturn(List.of());

        service.trimRealtime();

        verify(messageRepository).findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"));
        ArgumentCaptor<PersisterStateEntity> stateCaptor = ArgumentCaptor.forClass(PersisterStateEntity.class);
        verify(persisterStateRepository, times(1)).save(stateCaptor.capture());
        assertThat(stateCaptor.getValue().getStartIndex()).isEqualTo(500_000L);
    }

    @Test
    void deletesOnlyTheCoveredIdRangeAndNeverSelectsByTimestamp() {
        // Ids above deleteEnd may carry backdated timestamps; they are outside the coverage check and must stay.
        when(persisterStateRepository.findRealtime()).thenReturn(Optional.of(Fixtures.realtime(1L, 10L)));
        when(persisterStateRepository.findAllNonRealtime()).thenReturn(List.of(Fixtures.archive("archive-1", 1L, 4L)));
        when(messageRepository.findMinIdFromTimestamp(Instant.parse("2026-03-24T00:00:00Z"))).thenReturn(5L);
        when(messageRepository.findIdsInRange(1L, 4L, PAGE)).thenReturn(List.of(1L, 2L, 3L, 4L));

        service.trimRealtime();

        ArgumentCaptor<List<Long>> deletedIds = ArgumentCaptor.forClass((Class) List.class);
        verify(messageRepository, times(1)).deleteByIdIn(deletedIds.capture());
        assertThat(deletedIds.getValue()).containsExactly(1L, 2L, 3L, 4L);
        // The last page reached deleteEnd, so there is no further lookup.
        verify(messageRepository, times(1)).findIdsInRange(org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyLong(), any());
    }

    private static final class Fixtures {
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

}
