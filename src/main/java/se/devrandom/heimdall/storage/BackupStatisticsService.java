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
package se.devrandom.heimdall.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

/**
 * Thread-safe statistics service for tracking backup job metrics.
 * Uses lock-free atomic operations for high-performance concurrent updates.
 */
@Service
@ConditionalOnProperty(name = "spring.batch.job.enabled", havingValue = "true", matchIfMissing = true)
public class BackupStatisticsService {
    private static final Logger log = LoggerFactory.getLogger(BackupStatisticsService.class);
    private static final int MAX_FAILED_IDS = 1000;

    // ContentVersion statistics
    private final AtomicLong contentVersionTotal = new AtomicLong(0);
    private final AtomicLong contentVersionSkipped = new AtomicLong(0);
    private final AtomicLong contentVersionDownloaded = new AtomicLong(0);
    private final AtomicLong contentVersionFailed = new AtomicLong(0);
    private final AtomicLong contentVersionBytesTransferred = new AtomicLong(0);
    private final AtomicInteger contentVersionRetries = new AtomicInteger(0);

    // Deduplication statistics
    private final AtomicLong contentVersionSkippedDuplicate = new AtomicLong(0);
    private final AtomicLong contentVersionSkippedSize = new AtomicLong(0);
    private final AtomicLong contentVersionBytesDeduped = new AtomicLong(0);

    // CSV/Object backup statistics
    private final AtomicInteger csvFilesUploaded = new AtomicInteger(0);
    private final AtomicLong totalRecordsProcessed = new AtomicLong(0);
    private final AtomicLong deletedRecordsProcessed = new AtomicLong(0);

    // Archive statistics
    private final AtomicInteger objectsWithArchiveEnabled = new AtomicInteger(0);
    private final AtomicInteger objectsArchived = new AtomicInteger(0);
    private final AtomicLong totalRecordsArchived = new AtomicLong(0);
    private final AtomicLong contentDocumentLinksArchived = new AtomicLong(0);

    // Timing
    private final Instant jobStartTime = Instant.now();
    private volatile Instant jobEndTime;

    // Failed ContentVersion IDs for debugging (bounded to prevent memory issues)
    private final ConcurrentLinkedQueue<String> failedContentVersionIds = new ConcurrentLinkedQueue<>();

    // ===== ContentVersion Statistics Methods =====

    public void incrementContentVersionTotal() {
        contentVersionTotal.incrementAndGet();
    }

    public void incrementContentVersionSkipped() {
        contentVersionSkipped.incrementAndGet();
    }

    public void incrementContentVersionDownloaded() {
        contentVersionDownloaded.incrementAndGet();
    }

    public void incrementContentVersionFailed(String contentVersionId) {
        contentVersionFailed.incrementAndGet();

        // Add to failed list if not too large
        if (failedContentVersionIds.size() < MAX_FAILED_IDS) {
            failedContentVersionIds.add(contentVersionId);
        }
    }

    public void addBytesTransferred(long bytes) {
        contentVersionBytesTransferred.addAndGet(bytes);
    }

    public void incrementRetry() {
        contentVersionRetries.incrementAndGet();
    }

    public void incrementContentVersionSkippedDuplicate() {
        contentVersionSkippedDuplicate.incrementAndGet();
    }

    public void incrementContentVersionSkippedSize() {
        contentVersionSkippedSize.incrementAndGet();
    }

    public void addBytesDeduped(long bytes) {
        contentVersionBytesDeduped.addAndGet(bytes);
    }

    // ===== CSV/Object Backup Statistics Methods =====

    public void recordCsvUpload(String objectName, int recordCount, boolean isDeleted) {
        csvFilesUploaded.incrementAndGet();

        if (isDeleted) {
            deletedRecordsProcessed.addAndGet(recordCount);
        } else {
            totalRecordsProcessed.addAndGet(recordCount);
        }

        log.debug("Recorded CSV upload: {} ({} records, deleted: {})",
            objectName, recordCount, isDeleted);
    }

    // ===== Archive Statistics Methods =====

    public void incrementObjectsWithArchiveEnabled() {
        objectsWithArchiveEnabled.incrementAndGet();
    }

    public void recordArchive(String objectName, long recordCount) {
        objectsArchived.incrementAndGet();
        totalRecordsArchived.addAndGet(recordCount);
        log.debug("Recorded archive: {} ({} records)", objectName, recordCount);
    }

    public void addContentDocumentLinksArchived(long count) {
        contentDocumentLinksArchived.addAndGet(count);
    }

    public int getObjectsWithArchiveEnabled() {
        return objectsWithArchiveEnabled.get();
    }

    public int getObjectsArchived() {
        return objectsArchived.get();
    }

    public long getTotalRecordsArchived() {
        return totalRecordsArchived.get();
    }

    public long getContentDocumentLinksArchived() {
        return contentDocumentLinksArchived.get();
    }

    // ===== Getters for Current Values =====

    public long getContentVersionTotal() {
        return contentVersionTotal.get();
    }

    public long getContentVersionSkipped() {
        return contentVersionSkipped.get();
    }

    public long getContentVersionDownloaded() {
        return contentVersionDownloaded.get();
    }

    public long getContentVersionFailed() {
        return contentVersionFailed.get();
    }

    public long getBytesTransferred() {
        return contentVersionBytesTransferred.get();
    }

    public int getRetryCount() {
        return contentVersionRetries.get();
    }

    public int getCsvFilesUploaded() {
        return csvFilesUploaded.get();
    }

    public long getTotalRecordsProcessed() {
        return totalRecordsProcessed.get();
    }

    public long getDeletedRecordsProcessed() {
        return deletedRecordsProcessed.get();
    }

    // ===== Summary Report Generation =====

    /**
     * Marks the job as complete and sets the end time.
     */
    public void markJobComplete() {
        this.jobEndTime = Instant.now();
    }

    /**
     * Generates a comprehensive summary report of the backup job.
     *
     * @return Formatted summary report string
     */
    public String generateSummaryReport() {
        if (jobEndTime == null) {
            markJobComplete();
        }

        Duration duration = Duration.between(jobStartTime, jobEndTime);
        long minutes = duration.toMinutes();
        long seconds = duration.getSeconds() % 60;

        StringBuilder report = new StringBuilder();
        report.append("\n");

        // Duration
        report.append(String.format("Duration: %dm %ds%n", minutes, seconds));
        report.append("\n");

        // CSV Files Statistics
        report.append("CSV Files Processed:\n");
        report.append(String.format("  - Total CSV files uploaded: %,d%n", csvFilesUploaded.get()));
        report.append(String.format("  - Total records processed: %,d%n", totalRecordsProcessed.get()));
        report.append(String.format("  - Deleted records processed: %,d%n", deletedRecordsProcessed.get()));
        report.append("\n");

        // Archive Statistics
        int archiveEnabled = objectsWithArchiveEnabled.get();
        if (archiveEnabled > 0) {
            report.append("Archive:\n");
            report.append(String.format("  - Objects with archive enabled: %,d%n", archiveEnabled));
            report.append(String.format("  - Objects actually archived: %,d%n", objectsArchived.get()));
            report.append(String.format("  - Total records archived: %,d%n", totalRecordsArchived.get()));
            long cdlArchived = contentDocumentLinksArchived.get();
            if (cdlArchived > 0) {
                report.append(String.format("  - ContentDocumentLinks archived: %,d%n", cdlArchived));
            }
            report.append("\n");
        }

        // ContentVersion Statistics
        long cvTotal = contentVersionTotal.get();
        if (cvTotal > 0) {
            report.append("ContentVersion Files:\n");
            report.append(String.format("  - Total ContentVersions found: %,d%n", cvTotal));
            report.append(String.format("  - Downloaded and uploaded: %,d%n", contentVersionDownloaded.get()));

            // Breakdown of skipped files
            long totalSkipped = contentVersionSkipped.get();
            long skippedDupe = contentVersionSkippedDuplicate.get();
            long skippedSize = contentVersionSkippedSize.get();
            long skippedOld = totalSkipped - skippedDupe - skippedSize;

            report.append(String.format("  - Skipped (total): %,d%n", totalSkipped));
            if (skippedOld > 0) {
                report.append(String.format("    • Already in S3 (previous period): %,d%n", skippedOld));
            }
            if (skippedDupe > 0) {
                report.append(String.format("    • Deduplicated (same checksum): %,d%n", skippedDupe));
            }
            if (skippedSize > 0) {
                report.append(String.format("    • Too small (size filter): %,d%n", skippedSize));
            }

            report.append(String.format("  - Failed: %,d%n", contentVersionFailed.get()));

            // Bytes transferred
            long bytes = contentVersionBytesTransferred.get();
            String bytesFormatted = formatBytes(bytes);
            report.append(String.format("  - Total bytes transferred: %s%n", bytesFormatted));

            // Storage saved via deduplication
            long bytesDeduped = contentVersionBytesDeduped.get();
            if (bytesDeduped > 0) {
                String dedupedFormatted = formatBytes(bytesDeduped);
                double savingsPercent = bytes > 0 ?  (double) bytesDeduped / (bytes + bytesDeduped) * 100 : 0.0;
                report.append(String.format("  - Storage saved via deduplication: %s (%.1f%%)%n",
                    dedupedFormatted, savingsPercent));
            }

            // Retry count
            int retries = contentVersionRetries.get();
            report.append(String.format("  - Retry attempts: %d%n", retries));

            // Throughput
            if (duration.getSeconds() > 0 && bytes > 0) {
                double mbPerSecond = (bytes / 1024.0 / 1024.0) / duration.getSeconds();
                report.append(String.format("  - Average throughput: %.2f MB/s%n", mbPerSecond));
            }

            report.append("\n");

            // Failed ContentVersions (first 10)
            if (!failedContentVersionIds.isEmpty()) {
                report.append("Failed ContentVersions (first 10):\n");
                int count = 0;
                for (String id : failedContentVersionIds) {
                    report.append(String.format("  - %s%n", id));
                    if (++count >= 10) break;
                }
                if (failedContentVersionIds.size() > 10) {
                    report.append(String.format("  ... and %d more%n",
                        failedContentVersionIds.size() - 10));
                }
                report.append("\n");
            }
        }

        // Overall Status
        long failedCount = contentVersionFailed.get();
        long processedCount = contentVersionDownloaded.get() + contentVersionSkipped.get();
        double failureRate = processedCount > 0 ? (double) failedCount / processedCount : 0.0;

        String status;
        if (failureRate > 0.5) {
            status = "FAILED (>50% failure rate)";
        } else if (failureRate > 0.1) {
            status = "WARNING (>10% failure rate)";
        } else {
            status = "SUCCESS";
        }

        report.append(String.format("Overall Status: %s", status));

        return report.toString();
    }

    /**
     * Formats bytes into human-readable format (B, KB, MB, GB, TB).
     */
    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + " B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2f KB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2f MB", bytes / 1024.0 / 1024.0);
        } else if (bytes < 1024L * 1024 * 1024 * 1024) {
            return String.format("%.2f GB", bytes / 1024.0 / 1024.0 / 1024.0);
        } else {
            return String.format("%.2f TB", bytes / 1024.0 / 1024.0 / 1024.0 / 1024.0);
        }
    }

    /**
     * Resets all statistics. Useful for testing or starting a new job.
     */
    public void reset() {
        contentVersionTotal.set(0);
        contentVersionSkipped.set(0);
        contentVersionDownloaded.set(0);
        contentVersionFailed.set(0);
        contentVersionBytesTransferred.set(0);
        contentVersionRetries.set(0);
        contentVersionSkippedDuplicate.set(0);
        contentVersionSkippedSize.set(0);
        contentVersionBytesDeduped.set(0);
        csvFilesUploaded.set(0);
        totalRecordsProcessed.set(0);
        deletedRecordsProcessed.set(0);
        objectsWithArchiveEnabled.set(0);
        objectsArchived.set(0);
        totalRecordsArchived.set(0);
        contentDocumentLinksArchived.set(0);
        failedContentVersionIds.clear();
        jobEndTime = null;

        log.info("Statistics reset");
    }
}
