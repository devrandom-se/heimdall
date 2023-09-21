/*
 * Heimdall - Salesforce Backup Solution
 * Copyright (C) 2025 Johan Karlsteen
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package se.devrandom.heimdall.salesforce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.Date;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

/**
 * Sliding window progress tracker for ContentVersion downloads.
 *
 * Tracks which files have been downloaded and maintains a checkpoint of the "safe" position
 * where all files up to that point are confirmed complete. Similar to TCP sliding window.
 *
 * Thread-safe for concurrent downloads.
 *
 * Example:
 * Records: [001✓] [002✓] [003⏳] [004✓] [005⏳] [006-] [007-]
 *   ✓ = Completed
 *   ⏳ = In Progress
 *   - = Not started
 *
 * LastCommittedId = 002 (all files up to 002 are confirmed complete)
 */
public class ContentVersionProgressTracker {
    private static final Logger log = LoggerFactory.getLogger(ContentVersionProgressTracker.class);

    private final ConcurrentSkipListMap<String, FileStatus> fileProgress = new ConcurrentSkipListMap<>();
    private final AtomicInteger completedCount = new AtomicInteger(0);
    private final int checkpointInterval;
    private final Consumer<CheckpointData> checkpointCallback;
    private final long runId; // Database run ID for tracking

    private volatile String lastCommittedId = null;
    private volatile Date lastCommittedSystemModstamp = null;

    public enum FileStatus {
        IN_PROGRESS,
        COMPLETED,
        FAILED
    }

    /**
     * Checkpoint data containing the safe position where all prior records are complete
     */
    public static class CheckpointData {
        public final String lastId;
        public final Date lastSystemModstamp;
        public final int completedCount;

        public CheckpointData(String lastId, Date lastSystemModstamp, int completedCount) {
            this.lastId = lastId;
            this.lastSystemModstamp = lastSystemModstamp;
            this.completedCount = completedCount;
        }
    }

    /**
     * Create a progress tracker
     *
     * @param runId Database run ID for this backup run
     * @param checkpointInterval How often to persist checkpoint (every N completed files)
     * @param checkpointCallback Called when checkpoint should be persisted
     */
    public ContentVersionProgressTracker(long runId, int checkpointInterval, Consumer<CheckpointData> checkpointCallback) {
        this.runId = runId;
        this.checkpointInterval = checkpointInterval;
        this.checkpointCallback = checkpointCallback;
    }

    /**
     * Get the run ID for this tracker
     */
    public long getRunId() {
        return runId;
    }

    /**
     * Mark a file as started (before download begins)
     * This reserves the slot in the sliding window
     *
     * @param recordId ContentVersion Id
     * @param systemModstamp SystemModstamp from record
     */
    public synchronized void markStarted(String recordId, Date systemModstamp) {
        fileProgress.put(recordId, FileStatus.IN_PROGRESS);
        log.debug("Marked {} as IN_PROGRESS", recordId);
    }

    /**
     * Mark a file as completed (successful download or skip)
     * Updates the sliding window and triggers checkpoint if needed
     *
     * @param recordId ContentVersion Id
     * @param systemModstamp SystemModstamp from record
     */
    public synchronized void markCompleted(String recordId, Date systemModstamp) {
        fileProgress.put(recordId, FileStatus.COMPLETED);
        int completed = completedCount.incrementAndGet();

        log.debug("Marked {} as COMPLETED (total completed: {})", recordId, completed);

        // Update sliding window
        updateSlidingWindow(systemModstamp);

        // Checkpoint every N files
        if (completed % checkpointInterval == 0) {
            persistCheckpoint();
        }
    }

    /**
     * Mark a file as failed (after all retries exhausted)
     * Failed files block the sliding window - checkpoint won't advance past them
     *
     * @param recordId ContentVersion Id
     */
    public synchronized void markFailed(String recordId) {
        fileProgress.put(recordId, FileStatus.FAILED);
        log.warn("Marked {} as FAILED - this will block checkpoint advancement", recordId);
    }

    /**
     * Update the sliding window to find the last contiguous completed position
     * The checkpoint can only advance to a position where ALL prior files are completed
     */
    private void updateSlidingWindow(Date systemModstamp) {
        for (var entry : fileProgress.entrySet()) {
            FileStatus status = entry.getValue();

            // Stop at first non-completed file (IN_PROGRESS or FAILED)
            if (status != FileStatus.COMPLETED) {
                log.debug("Sliding window stopped at {} (status: {})", entry.getKey(), status);
                return;
            }

            // This file and all before it are completed - advance checkpoint
            lastCommittedId = entry.getKey();
            lastCommittedSystemModstamp = systemModstamp;
        }

        log.debug("Sliding window advanced to lastId={}", lastCommittedId);
    }

    /**
     * Persist the current checkpoint via callback
     */
    private void persistCheckpoint() {
        if (lastCommittedId != null) {
            CheckpointData checkpoint = new CheckpointData(
                lastCommittedId,
                lastCommittedSystemModstamp,
                completedCount.get()
            );

            log.info("Checkpointing at Id={}, completed={}", lastCommittedId, completedCount.get());

            try {
                checkpointCallback.accept(checkpoint);
            } catch (Exception e) {
                log.error("Failed to persist checkpoint: {}", e.getMessage(), e);
                // Continue processing - checkpoint failure shouldn't stop the backup
            }
        } else {
            log.debug("No checkpoint to persist yet (no completed files)");
        }
    }

    /**
     * Force a final checkpoint (call at end of batch)
     */
    public synchronized void finalCheckpoint() {
        updateSlidingWindow(lastCommittedSystemModstamp);
        persistCheckpoint();
        log.info("Final checkpoint: lastId={}, total completed={}", lastCommittedId, completedCount.get());
    }

    /**
     * Get the current safe checkpoint position
     * This is the last Id where all prior files are confirmed complete
     */
    public synchronized CheckpointData getCurrentCheckpoint() {
        return new CheckpointData(lastCommittedId, lastCommittedSystemModstamp, completedCount.get());
    }

    /**
     * Get total count of completed files
     */
    public int getCompletedCount() {
        return completedCount.get();
    }

    /**
     * Get current progress map size (for debugging)
     */
    public int getTrackedFileCount() {
        return fileProgress.size();
    }
}
