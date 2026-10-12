package com.fifteen02.eventstore.pekko.actors.ack;

import com.fifteen02.eventstore.pekko.actors.interfaces.Interface;
import com.fifteen02.eventstore.service.InterfaceStateService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;

public class BatchAcknowledger implements Acknowledger {

    private static final Logger log = LoggerFactory.getLogger(BatchAcknowledger.class);

    protected final InterfaceStateService interfaceStateService;
    protected final String interfaceName;
    protected final TreeSet<Long> pendingIndices = new TreeSet<>();
    protected final long batchSize;
    protected long currentIndex;

    public BatchAcknowledger(String interfaceName, InterfaceStateService interfaceStateService, long batch) {
        this.interfaceStateService = interfaceStateService;
        this.interfaceName = interfaceName;
        this.batchSize = Math.max(1L, batch);
        this.currentIndex = interfaceStateService.getLastAck(interfaceName);
    }

    @Override
    public void ack(Interface.ExternalAck message) {
        long messageIndex = message.index().value();

        // Always ack on RabbitMQ immediately - we don't need to hold onto these
        message.acknowledgeOnQueue().get();

        // If the message index is at or below the current acknowledged index,
        // don't process it (already acknowledged or duplicate)
        if (messageIndex <= currentIndex) {
            return;
        }

        // Add index to buffer
        pendingIndices.add(messageIndex);

        // Process buffered indices in sequence
        processBufferedIndices();
    }

    private void processBufferedIndices() {
        // Only persist when we have at least one full batch of consecutive indices
        while (!pendingIndices.isEmpty()) {
            long expectedNextIndex = currentIndex + 1;

            // If the head is not the next expected index, we have a gap
            long headIndex = pendingIndices.first();
            if (headIndex != expectedNextIndex) {
                log.warn("Waiting for missing index [interface: {}] [currentIndex: {}] [expectedNext: {}] [headIndex: {}] [gap: {}] [pendingCount: {}]",
                        interfaceName, currentIndex, expectedNextIndex, headIndex, headIndex - expectedNextIndex, pendingIndices.size());
                return;
            }

            // Count how many consecutive indices we currently have from the head
            long consecutiveCount = 0L;
            long probeIndex = expectedNextIndex;

            for (Long index : pendingIndices) {
                if (index == probeIndex) {
                    consecutiveCount++;
                    probeIndex++;
                } else {
                    break; // gap encountered
                }
            }

            if (consecutiveCount < batchSize) {
                log.debug("Waiting for full batch [interface: {}] [currentIndex: {}] [consecutiveCount: {}] [batchSize: {}] [pendingCount: {}]",
                        interfaceName, currentIndex, consecutiveCount, batchSize, pendingIndices.size());
                return;
            }

            // We may have multiple full batches buffered; determine how many
            long fullBatches = consecutiveCount / batchSize;
            long lastIndexToPersist = currentIndex + (fullBatches * batchSize);

            updateAcknowledged(lastIndexToPersist);
            log.debug("Persisted acknowledgment to database [interface: {}] [previousIndex: {}] [newIndex: {}] [batchCount: {}]",
                    interfaceName, currentIndex, lastIndexToPersist, fullBatches);

            // Remove all entries up to the lastIndexToPersist
            while (!pendingIndices.isEmpty() && pendingIndices.first() <= lastIndexToPersist) {
                pendingIndices.pollFirst();
            }

            // Advance current index
            currentIndex = lastIndexToPersist;
        }
    }

    void updateAcknowledged(long lastIndexToPersist) {
        // Persist once with the last index of the accumulated full batches
        interfaceStateService.updateAcknowledged(interfaceName, lastIndexToPersist);
    }
}
