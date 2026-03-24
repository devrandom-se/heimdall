package se.devrandom.heimdall.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Level 1: Pure unit tests for BackupStatisticsService.
 * Tests counters, report generation, and thread-safety.
 */
class BackupStatisticsServiceTest {

    private BackupStatisticsService stats;

    @BeforeEach
    void setUp() {
        stats = new BackupStatisticsService();
        stats.reset();
    }

    // ===== Counter tests =====

    @Test
    void incrementContentVersionCounters() {
        stats.incrementContentVersionTotal();
        stats.incrementContentVersionTotal();
        stats.incrementContentVersionDownloaded();
        stats.incrementContentVersionSkipped();
        stats.incrementContentVersionFailed("CV001");

        assertEquals(2, stats.getContentVersionTotal());
        assertEquals(1, stats.getContentVersionDownloaded());
        assertEquals(1, stats.getContentVersionSkipped());
        assertEquals(1, stats.getContentVersionFailed());
    }

    @Test
    void addBytesTransferred() {
        stats.addBytesTransferred(1024);
        stats.addBytesTransferred(2048);
        assertEquals(3072, stats.getBytesTransferred());
    }

    @Test
    void incrementRetry() {
        stats.incrementRetry();
        stats.incrementRetry();
        assertEquals(2, stats.getRetryCount());
    }

    @Test
    void recordCsvUpload_normalRecords() {
        stats.recordCsvUpload("Account", 100, false);
        assertEquals(1, stats.getCsvFilesUploaded());
        assertEquals(100, stats.getTotalRecordsProcessed());
        assertEquals(0, stats.getDeletedRecordsProcessed());
    }

    @Test
    void recordCsvUpload_deletedRecords() {
        stats.recordCsvUpload("Account", 50, true);
        assertEquals(1, stats.getCsvFilesUploaded());
        assertEquals(0, stats.getTotalRecordsProcessed());
        assertEquals(50, stats.getDeletedRecordsProcessed());
    }

    @Test
    void deduplicationCounters() {
        stats.incrementContentVersionSkippedDuplicate();
        stats.incrementContentVersionSkippedSize();
        stats.addBytesDeduped(4096);

        // Verify via report output (these don't have direct getters)
        stats.incrementContentVersionTotal();
        String report = stats.generateSummaryReport();
        assertTrue(report.contains("Deduplicated"));
    }

    @Test
    void archiveCounters() {
        stats.incrementObjectsWithArchiveEnabled();
        stats.incrementObjectsWithArchiveEnabled();
        stats.recordArchive("Account", 500);
        stats.addContentDocumentLinksArchived(10);

        assertEquals(2, stats.getObjectsWithArchiveEnabled());
        assertEquals(1, stats.getObjectsArchived());
        assertEquals(500, stats.getTotalRecordsArchived());
        assertEquals(10, stats.getContentDocumentLinksArchived());
    }

    // ===== Reset tests =====

    @Test
    void reset_clearsAllCounters() {
        stats.incrementContentVersionTotal();
        stats.incrementContentVersionDownloaded();
        stats.addBytesTransferred(1000);
        stats.recordCsvUpload("Account", 100, false);
        stats.incrementObjectsWithArchiveEnabled();

        stats.reset();

        assertEquals(0, stats.getContentVersionTotal());
        assertEquals(0, stats.getContentVersionDownloaded());
        assertEquals(0, stats.getBytesTransferred());
        assertEquals(0, stats.getCsvFilesUploaded());
        assertEquals(0, stats.getObjectsWithArchiveEnabled());
    }

    // ===== Report generation tests =====

    @Test
    void generateSummaryReport_containsDuration() {
        String report = stats.generateSummaryReport();
        assertTrue(report.contains("Duration:"));
    }

    @Test
    void generateSummaryReport_containsCsvStats() {
        stats.recordCsvUpload("Account", 100, false);
        String report = stats.generateSummaryReport();
        assertTrue(report.contains("CSV Files Processed"));
        assertTrue(report.contains("100"));
    }

    @Test
    void generateSummaryReport_containsArchiveSection() {
        stats.incrementObjectsWithArchiveEnabled();
        stats.recordArchive("Account", 50);
        String report = stats.generateSummaryReport();
        assertTrue(report.contains("Archive:"));
        assertTrue(report.contains("50"));
    }

    @Test
    void generateSummaryReport_containsContentVersionStats() {
        stats.incrementContentVersionTotal();
        stats.incrementContentVersionDownloaded();
        stats.addBytesTransferred(1024 * 1024);
        String report = stats.generateSummaryReport();
        assertTrue(report.contains("ContentVersion Files:"));
        assertTrue(report.contains("Downloaded"));
    }

    @Test
    void generateSummaryReport_overallStatusSuccess() {
        stats.incrementContentVersionDownloaded();
        stats.incrementContentVersionTotal();
        String report = stats.generateSummaryReport();
        assertTrue(report.contains("Overall Status: SUCCESS"));
    }

    @Test
    void generateSummaryReport_failedContentVersionsListed() {
        stats.incrementContentVersionTotal();
        stats.incrementContentVersionFailed("CV_FAIL_001");
        stats.incrementContentVersionFailed("CV_FAIL_002");
        String report = stats.generateSummaryReport();
        assertTrue(report.contains("CV_FAIL_001"));
        assertTrue(report.contains("CV_FAIL_002"));
    }

    // ===== Thread-safety tests =====

    @Test
    void concurrentIncrements() throws InterruptedException {
        int threadCount = 10;
        int incrementsPerThread = 1000;
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);

        for (int t = 0; t < threadCount; t++) {
            executor.submit(() -> {
                for (int i = 0; i < incrementsPerThread; i++) {
                    stats.incrementContentVersionTotal();
                    stats.incrementContentVersionDownloaded();
                    stats.addBytesTransferred(1);
                }
                latch.countDown();
            });
        }

        assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdown();

        assertEquals(threadCount * incrementsPerThread, stats.getContentVersionTotal());
        assertEquals(threadCount * incrementsPerThread, stats.getContentVersionDownloaded());
        assertEquals(threadCount * incrementsPerThread, stats.getBytesTransferred());
    }
}
