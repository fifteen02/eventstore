package com.fifteen02.eventstore.pekko.actors.ack;

import com.fifteen02.eventstore.pekko.actors.interfaces.Interface.ExternalAck;
import com.fifteen02.eventstore.service.InterfaceStateService;
import com.fifteen02.eventstore.to.Index;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchAcknowledgerTest {

    private static final String INTERFACE_NAME = "test-interface";

    @Mock
    private InterfaceStateService interfaceStateService;

    private BatchAcknowledger acknowledger;

    @BeforeEach
    void setUp() {
        when(interfaceStateService.getLastAck(INTERFACE_NAME)).thenReturn(100L);
        acknowledger = new BatchAcknowledger(INTERFACE_NAME, interfaceStateService, 1);
    }

    @Test
    void ack_shouldAlwaysCallAcknowledgeOnQueueImmediately() {
        AtomicInteger ackCount = new AtomicInteger(0);
        ExternalAck ack = createAck(101L, ackCount::incrementAndGet);

        acknowledger.ack(ack);

        assertThat(ackCount.get()).isEqualTo(1);
    }

    @Test
    void ack_shouldAckOnQueueEvenForAlreadyAcknowledgedIndices() {
        AtomicInteger ackCount = new AtomicInteger(0);
        // Index 50 is below currentIndex (100), should still ack on queue
        ExternalAck ack = createAck(50L, ackCount::incrementAndGet);

        acknowledger.ack(ack);

        assertThat(ackCount.get()).isEqualTo(1);
        // Should NOT persist to database for old indices
        verify(interfaceStateService, never()).updateAcknowledged(any(), anyLong());
    }

    @Test
    void ack_shouldPersistToDatabaseWhenBatchSizeIsMet() {
        AtomicInteger ackCount = new AtomicInteger(0);
        ExternalAck ack = createAck(101L, ackCount::incrementAndGet);

        acknowledger.ack(ack);

        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 101L);
    }

    @Test
    void ack_shouldWaitForConsecutiveIndicesBeforePersisting() {
        when(interfaceStateService.getLastAck(INTERFACE_NAME)).thenReturn(100L);
        acknowledger = new BatchAcknowledger(INTERFACE_NAME, interfaceStateService, 1);

        AtomicInteger ackCount = new AtomicInteger(0);
        // Send index 103 first (gap - missing 101, 102)
        acknowledger.ack(createAck(103L, ackCount::incrementAndGet));

        // Should ack on RabbitMQ but NOT persist (waiting for 101)
        assertThat(ackCount.get()).isEqualTo(1);
        verify(interfaceStateService, never()).updateAcknowledged(any(), anyLong());

        // Now send 101 - should persist 101
        acknowledger.ack(createAck(101L, ackCount::incrementAndGet));
        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 101L);

        // Now send 102 - should persist 102 and 103
        acknowledger.ack(createAck(102L, ackCount::incrementAndGet));
        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 103L);
    }

    @Test
    void ack_shouldProcessMultipleBatchesAtOnce() {
        when(interfaceStateService.getLastAck(INTERFACE_NAME)).thenReturn(100L);
        // Batch size of 2
        acknowledger = new BatchAcknowledger(INTERFACE_NAME, interfaceStateService, 2);

        AtomicInteger ackCount = new AtomicInteger(0);

        // Send 101, 102, 103, 104 (4 indices = 2 full batches)
        acknowledger.ack(createAck(101L, ackCount::incrementAndGet));
        acknowledger.ack(createAck(102L, ackCount::incrementAndGet));
        acknowledger.ack(createAck(103L, ackCount::incrementAndGet));
        acknowledger.ack(createAck(104L, ackCount::incrementAndGet));

        // Should persist up to 104 (2 batches of size 2)
        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 104L);
    }

    @Test
    void ack_shouldNotPersistPartialBatch() {
        when(interfaceStateService.getLastAck(INTERFACE_NAME)).thenReturn(100L);
        // Batch size of 3
        acknowledger = new BatchAcknowledger(INTERFACE_NAME, interfaceStateService, 3);

        AtomicInteger ackCount = new AtomicInteger(0);

        // Send only 2 indices (less than batch size)
        acknowledger.ack(createAck(101L, ackCount::incrementAndGet));
        acknowledger.ack(createAck(102L, ackCount::incrementAndGet));

        // Should ack on RabbitMQ but NOT persist (waiting for full batch)
        assertThat(ackCount.get()).isEqualTo(2);
        verify(interfaceStateService, never()).updateAcknowledged(any(), anyLong());

        // Now complete the batch
        acknowledger.ack(createAck(103L, ackCount::incrementAndGet));
        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 103L);
    }

    @Test
    void ack_shouldHandleOutOfOrderAcks() {
        when(interfaceStateService.getLastAck(INTERFACE_NAME)).thenReturn(100L);
        acknowledger = new BatchAcknowledger(INTERFACE_NAME, interfaceStateService, 1);

        AtomicInteger ackCount = new AtomicInteger(0);

        // Send out of order: 103, 101, 102
        acknowledger.ack(createAck(103L, ackCount::incrementAndGet));
        verify(interfaceStateService, never()).updateAcknowledged(any(), anyLong());

        acknowledger.ack(createAck(101L, ackCount::incrementAndGet));
        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 101L);

        acknowledger.ack(createAck(102L, ackCount::incrementAndGet));
        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 103L);

        // All 3 should have been acked on RabbitMQ
        assertThat(ackCount.get()).isEqualTo(3);
    }

    @Test
    void ack_shouldHandleDuplicateIndices() {
        AtomicInteger ackCount = new AtomicInteger(0);

        // Send 101
        acknowledger.ack(createAck(101L, ackCount::incrementAndGet));
        verify(interfaceStateService).updateAcknowledged(INTERFACE_NAME, 101L);

        // Send 101 again (duplicate)
        acknowledger.ack(createAck(101L, ackCount::incrementAndGet));

        // Should still ack on RabbitMQ but not persist again
        assertThat(ackCount.get()).isEqualTo(2);
        verify(interfaceStateService, times(1)).updateAcknowledged(INTERFACE_NAME, 101L);
    }

    private ExternalAck createAck(long index, Runnable onAck) {
        return new ExternalAck(new Index(index), () -> {
            onAck.run();
            return null;
        });
    }
}
