package com.fifteen02;

import com.fifteen02.eventstore.service.CorrelationIdService;
import com.fifteen02.eventstore.service.MessageIngestionService;
import com.fifteen02.eventstore.service.PersistorService;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.DefaultApplicationArguments;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class EventStoreStartupTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Mock
    private MessageIngestionService messageIngestionService;

    @Mock
    private CorrelationIdService correlationIdService;

    @Mock
    private PersistorService persistorService;

    @Test
    void runStoresStartMarkerThroughTheLockedSystemPathAndEnsuresRealtimePersisterState() {
        var startup = new EventStoreStartup(messageIngestionService, correlationIdService, persistorService, objectMapper);
        startup.run(new DefaultApplicationArguments());

        var input = ArgumentCaptor.forClass(MessageIngestionService.MessageInput.class);
        verify(messageIngestionService).ingestSystemMessage(input.capture());
        // The client path would apply duplicate suppression; the marker must never be deduped or dedupe others.
        verify(messageIngestionService, never()).ingest(any());
        assertThat(input.getValue().messageType()).isEqualTo("StartMarker");
        assertThat(input.getValue().userId()).isEqualTo("system");
        assertThat(input.getValue().content()).isEqualTo(objectMapper.createObjectNode());
        verify(persistorService).ensureRealtimePersister();
    }
}
