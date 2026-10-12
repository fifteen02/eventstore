package com.fifteen02.eventstore.service;

import com.fifteen02.eventstore.config.InterfacesConfigProperties;
import com.fifteen02.eventstore.repo.MessageRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.LinkedHashSet;
import java.util.Set;

@Service
public class DuplicateDetectionService {

    private static final Logger log = LoggerFactory.getLogger(DuplicateDetectionService.class);

    private final MessageRepository messageRepository;
    private final Set<String> recentHashes;
    private final int lookbackSize;
    private int messagesProcessedSinceStartup = 0;

    public DuplicateDetectionService(MessageRepository messageRepository,
            InterfacesConfigProperties interfacesConfigProperties) {
        this.messageRepository = messageRepository;
        this.lookbackSize = interfacesConfigProperties.getLookback();
        this.recentHashes = new LinkedHashSet<>(lookbackSize);

        // Load initial hashes from the database
        loadRecentHashes();
    }

    /**
     * Check if a message is a duplicate based on its content hash
     * 
     * @param contentHash The MD5 hash of the message content
     * @return true if the message is a duplicate, false otherwise
     */
    public boolean isDuplicate(String contentHash) {
        if (contentHash == null || contentHash.isBlank()) {
            return false;
        }

        // After processing lookbackSize messages since startup, we're safe from
        // duplicates
        if (messagesProcessedSinceStartup >= lookbackSize) {
            log.debug("Skipping duplicate check - past lookback window (processed: {})", messagesProcessedSinceStartup);
            return false;
        }

        // Check against the loaded recent hashes
        boolean isDuplicate = recentHashes.contains(contentHash);
        if (isDuplicate) {
            log.info("Duplicate message detected with hash: {} (processed: {})", contentHash,
                    messagesProcessedSinceStartup);
        }

        return isDuplicate;
    }

    /**
     * Increment the message counter (called after processing each message)
     */
    public void incrementMessageCount() {
        messagesProcessedSinceStartup++;
        log.debug("Processed message count: {}", messagesProcessedSinceStartup);
    }

    public void recordProcessedHash(String contentHash) {
        if (lookbackSize <= 0 || contentHash == null || contentHash.isBlank()) {
            return;
        }
        if (recentHashes.size() >= lookbackSize) {
            recentHashes.remove(recentHashes.iterator().next());
        }
        recentHashes.add(contentHash);
    }

    /**
     * Load recent message hashes from the database
     */
    private void loadRecentHashes() {
        if (lookbackSize <= 0) {
            return;
        }
        try {
            // Get the most recent messages up to lookback size
            var recentMessages = messageRepository.findTopNByOrderByIdDesc(PageRequest.of(0, lookbackSize));

            for (var message : recentMessages) {
                if (message.getContentHash() != null && !message.getContentHash().isBlank()) {
                    recentHashes.add(message.getContentHash());
                }
            }

            log.info("Loaded {} recent message hashes for duplicate detection (lookback: {})",
                    recentHashes.size(), lookbackSize);
        } catch (Exception e) {
            log.warn("Failed to load recent message hashes: {}", e.getMessage());
        }
    }
}
