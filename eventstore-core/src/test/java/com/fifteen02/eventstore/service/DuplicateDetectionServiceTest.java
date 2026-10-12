package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.entity.Message;
import com.fifteen02.eventstore.repo.MessageRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class DuplicateDetectionServiceTest {

    @Mock
    private MessageRepository messageRepository;

    @Test
    void loadsRecentHashesFromRepositoryUsingConfiguredLookback() {
        when(messageRepository.findTopNByOrderByIdDesc(any(Pageable.class)))
                .thenReturn(List.of(messageWithHash("existing-hash")));

        var service = serviceWithLookback(3);

        assertThat(service.isDuplicate("existing-hash")).isTrue();

        var pageable = ArgumentCaptor.forClass(Pageable.class);
        verify(messageRepository).findTopNByOrderByIdDesc(pageable.capture());
        assertThat(pageable.getValue()).isEqualTo(PageRequest.of(0, 3));
    }

    @Test
    void ignoresBlankHashesFromRepositoryAndInput() {
        when(messageRepository.findTopNByOrderByIdDesc(any(Pageable.class)))
                .thenReturn(List.of(
                        messageWithHash(null),
                        messageWithHash(""),
                        messageWithHash("   ")));

        var service = serviceWithLookback(3);

        assertThat(service.isDuplicate(null)).isFalse();
        assertThat(service.isDuplicate("")).isFalse();
        assertThat(service.isDuplicate("   ")).isFalse();
    }

    @Test
    void recordsProcessedHashAsDuplicateCandidate() {
        when(messageRepository.findTopNByOrderByIdDesc(any(Pageable.class))).thenReturn(List.of());

        var service = serviceWithLookback(3);

        service.recordProcessedHash("new-hash");

        assertThat(service.isDuplicate("new-hash")).isTrue();
    }

    @Test
    void evictsOldestHashWhenCacheExceedsLookback() {
        when(messageRepository.findTopNByOrderByIdDesc(any(Pageable.class)))
                .thenReturn(List.of(messageWithHash("first"), messageWithHash("second")));

        var service = serviceWithLookback(2);

        service.recordProcessedHash("third");

        assertThat(service.isDuplicate("first")).isFalse();
        assertThat(service.isDuplicate("second")).isTrue();
        assertThat(service.isDuplicate("third")).isTrue();
    }

    @Test
    void stopsCheckingDuplicatesAfterLookbackMessagesProcessedSinceStartup() {
        when(messageRepository.findTopNByOrderByIdDesc(any(Pageable.class)))
                .thenReturn(List.of(messageWithHash("existing-hash")));

        var service = serviceWithLookback(2);

        service.incrementMessageCount();
        assertThat(service.isDuplicate("existing-hash")).isTrue();

        service.incrementMessageCount();
        assertThat(service.isDuplicate("existing-hash")).isFalse();
    }

    @Test
    void doesNotRecordHashWhenLookbackIsDisabled() {
        var service = serviceWithLookback(0);

        service.recordProcessedHash("new-hash");

        assertThat(service.isDuplicate("new-hash")).isFalse();
        verify(messageRepository, never()).findTopNByOrderByIdDesc(any(Pageable.class));
    }

    @Test
    void treatsRepositoryLoadFailureAsEmptyCache() {
        when(messageRepository.findTopNByOrderByIdDesc(any(Pageable.class)))
                .thenThrow(new IllegalStateException("repository unavailable"));

        var service = serviceWithLookback(3);

        assertThat(service.isDuplicate("existing-hash")).isFalse();
    }

    @Test
    void skipsRepositoryLookupWhenInputHashIsBlank() {
        when(messageRepository.findTopNByOrderByIdDesc(any(Pageable.class))).thenReturn(List.of());

        var service = serviceWithLookback(3);

        assertThat(service.isDuplicate(" ")).isFalse();

        verify(messageRepository).findTopNByOrderByIdDesc(any(Pageable.class));
        verify(messageRepository, never()).findById(any());
    }

    private DuplicateDetectionService serviceWithLookback(int lookback) {
        var properties = new InterfacesConfigProperties();
        properties.setLookback(lookback);
        return new DuplicateDetectionService(messageRepository, properties);
    }

    private Message messageWithHash(String contentHash) {
        var message = new Message();
        message.setContentHash(contentHash);
        return message;
    }
}
