package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.entity.PersisterStateEntity;
import com.fifteen02.eventstore.repo.PersisterStateRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PersistorServiceTest {

    @Mock
    private PersisterStateRepository persisterStateRepository;

    @Test
    void ensureRealtimePersisterCreatesWriterPostgresState() {
        when(persisterStateRepository.findById(RealtimeMessageFetcher.NAME)).thenReturn(Optional.empty());

        var service = new PersistorService(persisterStateRepository);
        service.ensureRealtimePersister();

        var state = ArgumentCaptor.forClass(PersisterStateEntity.class);
        verify(persisterStateRepository).save(state.capture());
        assertThat(state.getValue().getId()).isEqualTo(RealtimeMessageFetcher.NAME);
        assertThat(state.getValue().getStartIndex()).isEqualTo(1L);
        assertThat(state.getValue().getLastIndex()).isNull();
        assertThat(state.getValue().isCanRead()).isTrue();
        assertThat(state.getValue().isCanWrite()).isTrue();
        assertThat(state.getValue().isRealtime()).isTrue();
        assertThat(state.getValue().getPriority()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void ensureRealtimePersisterDoesNotMutateExistingWriterPostgresState() {
        var existing = new PersisterStateEntity();
        existing.setId(RealtimeMessageFetcher.NAME);
        existing.setStartIndex(1L);
        existing.setLastIndex(5L);
        existing.setCanRead(false);
        existing.setCanWrite(false);
        existing.setRealtime(false);
        existing.setPriority(0);
        when(persisterStateRepository.findById(RealtimeMessageFetcher.NAME)).thenReturn(Optional.of(existing));

        var service = new PersistorService(persisterStateRepository);
        var result = service.ensureRealtimePersister();

        assertThat(result).isSameAs(existing);
        verify(persisterStateRepository, never()).save(existing);
    }
}
