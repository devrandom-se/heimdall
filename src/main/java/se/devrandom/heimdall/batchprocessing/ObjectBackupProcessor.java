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
package se.devrandom.heimdall.batchprocessing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import se.devrandom.heimdall.salesforce.SalesforceService;
import se.devrandom.heimdall.salesforce.objects.*;
import se.devrandom.heimdall.storage.BackupStatisticsService;
import se.devrandom.heimdall.storage.PostgresService;

import se.devrandom.heimdall.salesforce.ApiLimitTracker;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;

public class ObjectBackupProcessor
        implements ItemProcessor<Heimdall_Backup_Config__c, DescribeSObjectResult> {

    /**
     * Result from a pipelined batch operation.
     * Contains the download result and optionally a future for the next Bulk Query.
     */
    private static class BatchPipelineResult {
        final SalesforceService.CsvDownloadResult download;
        final CompletableFuture<BulkQueryRequest> nextQueryFuture;

        BatchPipelineResult(SalesforceService.CsvDownloadResult download,
                           CompletableFuture<BulkQueryRequest> nextQueryFuture) {
            this.download = download;
            this.nextQueryFuture = nextQueryFuture;
        }
    }

    @Autowired
    private SalesforceService salesforceService;

    @Autowired
    private BackupStatisticsService statisticsService;

    @Autowired
    private PostgresService postgresService;

    private static final Logger log = LoggerFactory.getLogger(ObjectBackupProcessor.class);

    // Format for SOQL datetime literals
    private static final SimpleDateFormat SOQL_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    static {
        SOQL_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /**
     * Format a timestamp for SOQL WHERE clause
     */
    private String formatForSoql(Timestamp timestamp) {
        if (timestamp == null) {
            return "1970-01-01T00:00:00.000Z";
        }
        return SOQL_DATE_FORMAT.format(timestamp);
    }

    /**
     * Download one batch of records using pipeline optimization.
     * If a preparedQuery is provided, uses that instead of creating a new one.
     * After downloading CSV, starts the next query asynchronously (if more records exist)
     * while processing the current CSV to storage.
     *
     * @param item The SObject description
     * @param queryAll Whether to use queryAll (includes deleted records)
     * @param lastModstamp The last SystemModstamp for pagination
     * @param lastId The last Id for pagination
     * @param initialRecordCount Running total of records processed
     * @param preparedQuery Optional pre-created query future (from previous batch's look-ahead)
     * @return BatchPipelineResult with download result and optional future for next query
     */
    private BatchPipelineResult downloadOneBatchPipelined(
            DescribeSObjectResult item,
            boolean queryAll,
            String lastModstamp,
            String lastId,
            int initialRecordCount,
            CompletableFuture<BulkQueryRequest> preparedQuery) {

        BulkQueryRequest bulkQueryRequest;

        // Use prepared query if available, otherwise create new one
        if (preparedQuery != null) {
            log.info("Using pre-prepared Bulk Query for {} (pipeline optimization)", item.name);
            try {
                bulkQueryRequest = preparedQuery.join(); // Wait for it to complete
            } catch (Exception e) {
                log.warn("Pre-prepared query failed, falling back to sync creation: {}", e.getMessage());
                bulkQueryRequest = null;
            }
        } else {
            bulkQueryRequest = null;
        }

        // Create query synchronously if we don't have one
        if (bulkQueryRequest == null) {
            log.info("Creating Bulk Query request for {} (queryAll: {}, lastModstamp: {}, lastId: {})",
                    item.name, queryAll, lastModstamp, lastId);

            bulkQueryRequest = salesforceService.createBulkQuery(item, queryAll, lastModstamp, lastId);

            try {
                Thread.sleep(10 * 1000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Wait for query to complete
        BulkQueryRequest bulkQueryStatus;
        do {
            bulkQueryStatus = salesforceService.checkBulkQueryStatus(bulkQueryRequest);
            if (!bulkQueryStatus.isFinished()) {
                try {
                    Thread.sleep(2000);  // Poll every 2 seconds
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        } while (!bulkQueryStatus.isFinished());

        if (!bulkQueryStatus.wasSuccessful()) {
            log.error("Query failed for {} (state: {})", item.name, bulkQueryStatus.state);
            // Adjust batch size down on failure (might be too large)
            int currentBatchSize = salesforceService.getBatchSize(item.name);
            salesforceService.adjustBatchSize(item.name, currentBatchSize, 0, 0, true);
            return new BatchPipelineResult(null, null);
        }

        log.info("Successfully ran query for {}", item.name);

        // Download CSV (without processing to storage yet) - with timing for batch size optimization
        int batchSize = salesforceService.getBatchSize(item.name);
        long downloadStartTime = System.currentTimeMillis();
        SalesforceService.CsvDownloadResult download = salesforceService.downloadBulkQueryCsv(bulkQueryStatus, initialRecordCount);
        long downloadDurationSeconds = (System.currentTimeMillis() - downloadStartTime) / 1000;

        // Log batch timing
        log.info("Batch completed for {}: {} records in {}s (batch size: {})",
                item.name, download.recordCount, downloadDurationSeconds, batchSize);

        // Adjust batch size based on timing (only for full batches)
        salesforceService.adjustBatchSize(item.name, batchSize, downloadDurationSeconds,
                download.recordCount, false);

        // Start next query asynchronously if there are more records (pipeline optimization)
        CompletableFuture<BulkQueryRequest> nextQueryFuture = null;
        if (download.hasMoreRecords && download.lastId != null) {
            final String nextLastModstamp = formatForSoql(download.lastModstamp);
            final String nextLastId = download.lastId;

            log.info("Starting next Bulk Query asynchronously (pipeline look-ahead) for {}", item.name);

            nextQueryFuture = CompletableFuture.supplyAsync(() -> {
                try {
                    BulkQueryRequest nextQuery = salesforceService.createBulkQuery(
                            item, queryAll, nextLastModstamp, nextLastId);
                    // Wait 10s for Salesforce to process
                    Thread.sleep(10 * 1000);
                    return nextQuery;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            });
        }

        // Process CSV to storage (S3 + PostgreSQL) - this happens while next query is running
        salesforceService.processCsvToStorage(download);

        return new BatchPipelineResult(download, nextQueryFuture);
    }

    /**
     * Legacy method for backwards compatibility - delegates to pipelined version
     */
    private SalesforceService.BackupBatchResult downloadOneBatch(
            DescribeSObjectResult item,
            boolean queryAll,
            String lastModstamp,
            String lastId,
            int initialRecordCount) {

        BatchPipelineResult pipelineResult = downloadOneBatchPipelined(
                item, queryAll, lastModstamp, lastId, initialRecordCount, null);

        if (pipelineResult.download == null) {
            return null;
        }

        // Convert CsvDownloadResult to BackupBatchResult for compatibility
        SalesforceService.BackupBatchResult result = new SalesforceService.BackupBatchResult();
        result.lastId = pipelineResult.download.lastId;
        result.lastModstamp = pipelineResult.download.lastModstamp;
        result.csvPath = pipelineResult.download.csvPath;
        if (pipelineResult.download.isQueryAll) {
            result.deletedRecords = initialRecordCount + pipelineResult.download.recordCount;
        } else {
            result.totalRecords = initialRecordCount + pipelineResult.download.recordCount;
        }

        return result;
    }

    @Override
    public DescribeSObjectResult process(Heimdall_Backup_Config__c backup) {
        log.trace("Processing Heimdall_Backup_Config__c {} (Status__c: {})",
                backup.getObjectName__c(), backup.getStatus__c());

        // Check API limit before processing this object
        Optional<ApiLimitTracker> tracker = salesforceService.getApiLimitTracker();
        if (tracker.isPresent() && tracker.get().isLimitReached()) {
            log.warn("API limit reached - skipping backup for {}", backup.getObjectName__c());
            return null;
        }

        boolean doBackup = backup.shouldBackup();
        boolean doArchive = backup.shouldArchive();

        // Skip if nothing to do (Not Replicable, No Backup)
        if (!doBackup && !doArchive) {
            log.debug("Skipping {} (Status: {})", backup.getObjectName__c(), backup.getStatus__c());
            return null;
        }

        // Backward compat deprecation warning
        if (Heimdall_Backup_Config__c.BACKUP.equals(backup.getStatus__c()) && Boolean.TRUE.equals(backup.getArchive__c())) {
            log.warn("{} uses deprecated Archive__c checkbox. Update Status to 'Backup + Archive' instead.",
                    backup.getObjectName__c());
        }

        String objectName = backup.getObjectName__c();

        // Wrap entire processing logic in try-catch so one object failing doesn't kill the whole job
        try {
            return processObject(backup, objectName, doBackup, doArchive);
        } catch (Exception e) {
            log.error("Processing FAILED for {} - skipping and continuing with next object: {}", objectName, e.getMessage());
            return null;
        }
    }

    private DescribeSObjectResult processObject(Heimdall_Backup_Config__c backup, String objectName,
            boolean doBackup, boolean doArchive) {

        // Describe the object to get field metadata (cached, no extra API cost)
        DescribeSObjectResult describe = salesforceService.describeSObject(objectName);

        // PHASES 1+2: Backup active and deleted records
        if (doBackup) {
            executeBackup(backup, describe, objectName);
        } else {
            log.info("Skipping backup for {} (Status: {})", objectName, backup.getStatus__c());
        }

        // PHASE 3: Archive old records if configured
        if (doArchive && backup.getArchive_Age_Days__c() != null && !"ContentVersion".equals(objectName)) {
            statisticsService.incrementObjectsWithArchiveEnabled();
            try {
                executeArchive(backup, describe, objectName);
            } catch (Exception e) {
                log.error("Archive FAILED for {}: {}", objectName, e.getMessage(), e);
            }
        }

        // Update statistics so admins can see when the object was last processed
        salesforceService.updateBackupStatistics(backup, 0);

        // Refresh pre-computed object stats for fast dashboard loading
        postgresService.refreshObjectStats(objectName);

        return describe;
    }

    private void executeBackup(Heimdall_Backup_Config__c backup, DescribeSObjectResult describe, String objectName) {

        Optional<ApiLimitTracker> tracker = salesforceService.getApiLimitTracker();

        // Get checkpoint from PostgreSQL
        String lastModstamp = "1970-01-01T00:00:00.000Z";
        String lastId = "";
        String lastModstampDeleted = "1970-01-01T00:00:00.000Z";
        String lastIdDeleted = "";

        try {
            PostgresService.BackupRun existingRun = postgresService.getLatestBackupRun(objectName, false);
            if (existingRun != null && existingRun.lastCheckpointModstamp != null) {
                lastModstamp = formatForSoql(existingRun.lastCheckpointModstamp);
                lastId = existingRun.lastCheckpointId != null ? existingRun.lastCheckpointId : "";
                log.info("Found checkpoint for {}: lastModstamp={}, lastId={}",
                        objectName, lastModstamp, lastId);
            }

            PostgresService.BackupRun existingDeletedRun = postgresService.getLatestBackupRun(objectName, true);
            if (existingDeletedRun != null && existingDeletedRun.lastCheckpointModstamp != null) {
                lastModstampDeleted = formatForSoql(existingDeletedRun.lastCheckpointModstamp);
                lastIdDeleted = existingDeletedRun.lastCheckpointId != null ? existingDeletedRun.lastCheckpointId : "";
            }
        } catch (SQLException e) {
            log.warn("Failed to get checkpoint from PostgreSQL: {}", e.getMessage());
        }

        // Smart COUNT optimization check
        if (salesforceService.shouldCheckCountFirst(backup)) {
            if (lastModstamp.equals("1970-01-01T00:00:00.000Z")) {
                log.info("No previous backup run found for {}, skipping COUNT check and running full backup", objectName);
            } else {
                log.info("Running COUNT check for {} (last modstamp: {})", objectName, lastModstamp);
                int newRecordCount = salesforceService.countNewRecords(objectName, lastModstamp);

                if (newRecordCount == 0) {
                    log.info("COUNT check: {} has no new records, skipping backup", objectName);
                    salesforceService.updateBackupStatistics(backup, 0);
                    return;
                } else if (newRecordCount > 0) {
                    log.info("COUNT check: {} has {} new records, proceeding with backup", objectName, newRecordCount);
                }
            }
        }

        // Create backup run in PostgreSQL (skip for ContentVersion - it manages its own run in downloadContentVersionFiles)
        long runId = -1;
        if (!"ContentVersion".equals(objectName)) {
            try {
                runId = postgresService.createBackupRun(objectName, false);
                log.info("Created PostgreSQL backup run: runId={}", runId);
            } catch (SQLException e) {
                log.error("Failed to create backup run in PostgreSQL: {}", e.getMessage());
                throw new RuntimeException("Cannot track backup progress", e);
            }
        }

        int totalRecords = 0;
        int totalDeletedRecords = 0;
        int batchCounter = 0;

        // Current checkpoint for active records
        String currentLastModstamp = lastModstamp;
        String currentLastId = lastId;

        // Pipeline state: holds the future for the next pre-started query
        CompletableFuture<BulkQueryRequest> nextQueryFuture = null;

        // PHASE 1: Fetch all active records first (with pipeline optimization)
        log.info("Starting PHASE 1: Fetching active records for {} (pipeline-optimized)", objectName);
        while (true) {
            // Check API limit before each batch
            if (tracker.isPresent() && tracker.get().isLimitReached()) {
                log.warn("API limit reached - stopping active records fetch for {} after {} batches", objectName, batchCounter);
                break;
            }

            batchCounter++;
            log.info("Starting active records batch {} for {}", batchCounter, objectName);

            // Use pipelined method - pass the pre-started query future if available
            BatchPipelineResult pipelineResult = downloadOneBatchPipelined(
                    describe, false, currentLastModstamp, currentLastId, totalRecords, nextQueryFuture);

            if (pipelineResult.download == null) {
                log.error("Batch {} failed for {}", batchCounter, objectName);
                break;
            }

            // Save the future for the next iteration (pipeline look-ahead)
            nextQueryFuture = pipelineResult.nextQueryFuture;

            int newRecords = pipelineResult.download.recordCount;

            // Track CSV upload in statistics
            if (newRecords > 0) {
                statisticsService.recordCsvUpload(objectName, newRecords, false);
            }

            // Update checkpoint (skip for ContentVersion - it has its own checkpoint tracking in downloadContentVersionFiles)
            if (pipelineResult.download.lastId != null) {
                currentLastModstamp = formatForSoql(pipelineResult.download.lastModstamp);
                currentLastId = pipelineResult.download.lastId;

                if (!"ContentVersion".equals(objectName)) {
                    try {
                        postgresService.updateBackupRunCheckpoint(runId, pipelineResult.download.lastId,
                                pipelineResult.download.lastModstamp, totalRecords + newRecords);
                    } catch (SQLException e) {
                        log.warn("Failed to save checkpoint: {}", e.getMessage());
                    }
                }
            }

            totalRecords += newRecords;

            log.info("Active records batch {} for {}: {} records fetched (total: {})",
                    batchCounter, objectName, newRecords, totalRecords);

            if (newRecords == 0 || !pipelineResult.download.hasMoreRecords) {
                log.info("{} active records complete after {} batches", objectName, batchCounter);
                break;
            }
        }

        // Complete active records run (skip for ContentVersion - it manages its own run)
        if (!"ContentVersion".equals(objectName)) {
            try {
                postgresService.completeBackupRun(runId, "SUCCESS", totalRecords, 0, 0, null);
            } catch (SQLException e) {
                log.warn("Failed to complete backup run: {}", e.getMessage());
            }
        }

        // PHASE 2: Fetch deleted records (with pipeline optimization)
        if (describe.hasIsDeletedField()) {
            log.info("Starting PHASE 2: Fetching deleted records for {} (pipeline-optimized)", objectName);

            long deletedRunId;
            try {
                deletedRunId = postgresService.createBackupRun(objectName, true);
            } catch (SQLException e) {
                log.error("Failed to create deleted records backup run: {}", e.getMessage());
                deletedRunId = -1;
            }

            String currentDeletedModstamp = lastModstampDeleted;
            String currentDeletedId = lastIdDeleted;
            int deletedBatchCounter = 0;

            // Pipeline state for deleted records
            CompletableFuture<BulkQueryRequest> nextDeletedQueryFuture = null;

            while (true) {
                // Check API limit before each batch
                if (tracker.isPresent() && tracker.get().isLimitReached()) {
                    log.warn("API limit reached - stopping deleted records fetch for {} after {} batches", objectName, deletedBatchCounter);
                    break;
                }

                deletedBatchCounter++;
                log.info("Starting deleted records batch {} for {}", deletedBatchCounter, objectName);

                // Use pipelined method for deleted records too
                BatchPipelineResult pipelineResult = downloadOneBatchPipelined(
                        describe, true, currentDeletedModstamp, currentDeletedId, totalDeletedRecords, nextDeletedQueryFuture);

                if (pipelineResult.download == null) {
                    log.error("Deleted batch {} failed for {}", deletedBatchCounter, objectName);
                    break;
                }

                // Save the future for the next iteration
                nextDeletedQueryFuture = pipelineResult.nextQueryFuture;

                int newDeletedRecords = pipelineResult.download.recordCount;

                if (newDeletedRecords > 0) {
                    statisticsService.recordCsvUpload(objectName, newDeletedRecords, true);
                }

                if (pipelineResult.download.lastId != null) {
                    currentDeletedModstamp = formatForSoql(pipelineResult.download.lastModstamp);
                    currentDeletedId = pipelineResult.download.lastId;

                    if (deletedRunId > 0) {
                        try {
                            postgresService.updateBackupRunCheckpoint(deletedRunId, pipelineResult.download.lastId,
                                    pipelineResult.download.lastModstamp, totalDeletedRecords + newDeletedRecords);
                        } catch (SQLException e) {
                            log.warn("Failed to save deleted checkpoint: {}", e.getMessage());
                        }
                    }
                }

                totalDeletedRecords += newDeletedRecords;

                log.info("Deleted records batch {} for {}: {} records fetched (total: {})",
                        deletedBatchCounter, objectName, newDeletedRecords, totalDeletedRecords);

                if (newDeletedRecords == 0 || !pipelineResult.download.hasMoreRecords) {
                    log.info("{} deleted records complete after {} batches", objectName, deletedBatchCounter);
                    break;
                }
            }

            if (deletedRunId > 0) {
                try {
                    postgresService.completeBackupRun(deletedRunId, "SUCCESS", totalDeletedRecords, 0, 0, null);
                } catch (SQLException e) {
                    log.warn("Failed to complete deleted backup run: {}", e.getMessage());
                }
            }
        }

        // Calculate total
        int grandTotal = totalRecords + totalDeletedRecords;

        // Update statistics
        salesforceService.updateBackupStatistics(backup, grandTotal);

        log.info("Completed backup for {} - Total: {} records ({} active, {} deleted) in {} batches",
                objectName, grandTotal, totalRecords, totalDeletedRecords, batchCounter);

        // Log resource usage after each object
        Runtime rt = Runtime.getRuntime();
        long usedMb = (rt.totalMemory() - rt.freeMemory()) / (1024 * 1024);
        long maxMb = rt.maxMemory() / (1024 * 1024);
        int cpus = rt.availableProcessors();
        log.info("Resource usage: heap {} MB / {} MB ({}%), CPUs: {}",
                usedMb, maxMb, maxMb > 0 ? usedMb * 100 / maxMb : 0, cpus);
    }

    /**
     * Execute archive phase for an object: query Salesforce for records older than Archive_Age_Days__c
     * and store them in archive periods (negative YYMM in PostgreSQL, Archive-yyyyMM in S3).
     */
    private void executeArchive(Heimdall_Backup_Config__c backup, DescribeSObjectResult describe, String objectName) {
        int ageDays = backup.getArchive_Age_Days__c();
        String archiveExpression = backup.getArchive_Expression__c();

        // Calculate CreatedDate threshold
        LocalDateTime threshold = LocalDateTime.now().minusDays(ageDays);
        String createdDateThreshold = threshold.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"));

        log.info("Starting ARCHIVE phase for {} (age > {} days, threshold: {}, expression: {})",
                objectName, ageDays, createdDateThreshold, archiveExpression);

        // Get archive checkpoint (looks across all archive periods)
        String lastModstamp = "1970-01-01T00:00:00.000Z";
        String lastId = "";

        try {
            PostgresService.BackupRun existingRun = postgresService.getLatestArchiveRun(objectName);
            if (existingRun != null && existingRun.lastCheckpointModstamp != null) {
                // Auto-reset check: if archive expression changed, reset checkpoint
                String previousExpression = existingRun.archiveExpression;
                if (!Objects.equals(previousExpression, archiveExpression)) {
                    log.warn("Archive expression changed for {} (was: '{}', now: '{}') - resetting checkpoint for full re-scan",
                            objectName, previousExpression, archiveExpression);
                } else {
                    lastModstamp = formatForSoql(existingRun.lastCheckpointModstamp);
                    lastId = existingRun.lastCheckpointId != null ? existingRun.lastCheckpointId : "";
                    log.info("Found archive checkpoint for {}: lastModstamp={}, lastId={}",
                            objectName, lastModstamp, lastId);
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to get archive checkpoint from PostgreSQL: {}", e.getMessage());
        }

        // Determine archive periods
        int archivePeriodInt = postgresService.getArchivePeriod();
        String archivePeriodS3 = "Archive-" + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));

        // Create archive run
        long runId;
        try {
            runId = postgresService.createArchiveRun(objectName, archiveExpression);
        } catch (SQLException e) {
            log.error("Failed to create archive run in PostgreSQL: {}", e.getMessage());
            throw new RuntimeException("Cannot track archive progress", e);
        }

        int totalArchived = 0;
        int batchCounter = 0;
        String currentLastModstamp = lastModstamp;
        String currentLastId = lastId;

        try {
            while (true) {
                batchCounter++;
                int batchSize = salesforceService.getBatchSize(objectName);

                // Generate archive SOQL
                String soql = describe.generateArchiveSOQLQuery(
                        createdDateThreshold, archiveExpression, currentLastModstamp, currentLastId, batchSize);

                log.info("Archive batch {} for {}: {}", batchCounter, objectName, soql);

                // Submit bulk query
                BulkQueryRequest bulkQueryRequest = salesforceService.createBulkQueryWithSOQL(soql, objectName);

                // Wait for Salesforce to process
                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }

                // Poll for completion
                BulkQueryRequest bulkQueryStatus;
                do {
                    bulkQueryStatus = salesforceService.checkBulkQueryStatus(bulkQueryRequest);
                    if (!bulkQueryStatus.isFinished()) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                } while (!bulkQueryStatus.isFinished());

                if (!bulkQueryStatus.wasSuccessful()) {
                    log.error("Archive query failed for {} (state: {})", objectName, bulkQueryStatus.state);
                    break;
                }

                // Download CSV
                SalesforceService.CsvDownloadResult download =
                        salesforceService.downloadBulkQueryCsv(bulkQueryStatus, totalArchived);

                if (download == null || download.recordCount == 0) {
                    log.info("Archive complete for {} - no more records in batch {}", objectName, batchCounter);
                    break;
                }

                // Store to archive storage
                salesforceService.processArchiveCsvToStorage(download, archivePeriodInt, archivePeriodS3);

                // Update checkpoint
                if (download.lastId != null) {
                    currentLastModstamp = formatForSoql(download.lastModstamp);
                    currentLastId = download.lastId;

                    try {
                        postgresService.updateBackupRunCheckpoint(runId, download.lastId,
                                download.lastModstamp, totalArchived + download.recordCount);
                    } catch (SQLException e) {
                        log.warn("Failed to save archive checkpoint: {}", e.getMessage());
                    }
                }

                totalArchived += download.recordCount;

                log.info("Archive batch {} for {}: {} records (total archived: {})",
                        batchCounter, objectName, download.recordCount, totalArchived);

                if (!download.hasMoreRecords) {
                    log.info("Archive complete for {} after {} batches", objectName, batchCounter);
                    break;
                }
            }

            // Complete archive run
            try {
                postgresService.completeBackupRun(runId, "SUCCESS", totalArchived, 0, 0, null);
            } catch (SQLException e) {
                log.warn("Failed to complete archive run: {}", e.getMessage());
            }

            log.info("Archived {} records for {} in {} batches", totalArchived, objectName, batchCounter);

            // Record archive statistics
            if (totalArchived > 0) {
                statisticsService.recordArchive(objectName, totalArchived);
            }

            // Archive ContentDocumentLink records for this object
            try {
                executeContentDocumentLinkArchive(objectName, createdDateThreshold, archiveExpression,
                        archivePeriodInt, archivePeriodS3);
            } catch (Exception cdlEx) {
                log.warn("ContentDocumentLink archive failed for {} - parent archive is intact: {}",
                        objectName, cdlEx.getMessage(), cdlEx);
            }

        } catch (Exception e) {
            // Mark run as failed
            try {
                postgresService.completeBackupRun(runId, "FAILED", totalArchived, 0, 0, e.getMessage());
            } catch (SQLException ex) {
                log.warn("Failed to mark archive run as failed: {}", ex.getMessage());
            }
            throw e;
        }
    }

    /**
     * Archive ContentDocumentLink records for a parent object.
     * Queries CDL using a subquery that reuses the parent's archive criteria,
     * storing results in the same archive period.
     * Failures are logged but do not affect the parent archive.
     */
    private void executeContentDocumentLinkArchive(String parentObjectName, String createdDateThreshold,
            String archiveExpression, int archivePeriodInt, String archivePeriodS3) {

        String cdlCheckpointName = "ContentDocumentLink:" + parentObjectName;

        log.info("Starting ContentDocumentLink archive for {} (threshold: {}, expression: {})",
                parentObjectName, createdDateThreshold, archiveExpression);

        // Get checkpoint
        String lastModstamp = "1970-01-01T00:00:00.000Z";
        String lastId = "";

        try {
            PostgresService.BackupRun existingRun = postgresService.getLatestArchiveRun(cdlCheckpointName);
            if (existingRun != null && existingRun.lastCheckpointModstamp != null) {
                // Auto-reset check: if archive expression changed, reset checkpoint
                String previousExpression = existingRun.archiveExpression;
                if (!Objects.equals(previousExpression, archiveExpression)) {
                    log.warn("Archive expression changed for {} (was: '{}', now: '{}') - resetting CDL checkpoint",
                            cdlCheckpointName, previousExpression, archiveExpression);
                } else {
                    lastModstamp = formatForSoql(existingRun.lastCheckpointModstamp);
                    lastId = existingRun.lastCheckpointId != null ? existingRun.lastCheckpointId : "";
                    log.info("Found CDL archive checkpoint for {}: lastModstamp={}, lastId={}",
                            cdlCheckpointName, lastModstamp, lastId);
                }
            }
        } catch (SQLException e) {
            log.warn("Failed to get CDL archive checkpoint: {}", e.getMessage());
        }

        // Create archive run
        long runId;
        try {
            runId = postgresService.createArchiveRun(cdlCheckpointName, archiveExpression);
        } catch (SQLException e) {
            log.error("Failed to create CDL archive run: {}", e.getMessage());
            return;
        }

        int totalArchived = 0;
        int batchCounter = 0;
        String currentLastModstamp = lastModstamp;
        String currentLastId = lastId;

        try {
            while (true) {
                batchCounter++;
                int batchSize = salesforceService.getBatchSize(parentObjectName);

                String soql = buildContentDocumentLinkSOQL(parentObjectName, createdDateThreshold,
                        archiveExpression, currentLastModstamp, currentLastId, batchSize);

                log.info("CDL archive batch {} for {}: {}", batchCounter, cdlCheckpointName, soql);

                // Submit bulk query
                BulkQueryRequest bulkQueryRequest = salesforceService.createBulkQueryWithSOQL(soql, cdlCheckpointName);

                try {
                    Thread.sleep(10 * 1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }

                // Poll for completion
                BulkQueryRequest bulkQueryStatus;
                do {
                    bulkQueryStatus = salesforceService.checkBulkQueryStatus(bulkQueryRequest);
                    if (!bulkQueryStatus.isFinished()) {
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new RuntimeException(e);
                        }
                    }
                } while (!bulkQueryStatus.isFinished());

                if (!bulkQueryStatus.wasSuccessful()) {
                    log.error("CDL archive query failed for {} (state: {})", cdlCheckpointName, bulkQueryStatus.state);
                    break;
                }

                // Download CSV
                SalesforceService.CsvDownloadResult download =
                        salesforceService.downloadBulkQueryCsv(bulkQueryStatus, totalArchived);

                if (download == null || download.recordCount == 0) {
                    log.info("CDL archive complete for {} - no more records in batch {}", cdlCheckpointName, batchCounter);
                    break;
                }

                // Store to archive storage
                salesforceService.processArchiveCsvToStorage(download, archivePeriodInt, archivePeriodS3);

                // Update checkpoint
                if (download.lastId != null) {
                    currentLastModstamp = formatForSoql(download.lastModstamp);
                    currentLastId = download.lastId;

                    try {
                        postgresService.updateBackupRunCheckpoint(runId, download.lastId,
                                download.lastModstamp, totalArchived + download.recordCount);
                    } catch (SQLException e) {
                        log.warn("Failed to save CDL archive checkpoint: {}", e.getMessage());
                    }
                }

                totalArchived += download.recordCount;

                log.info("CDL archive batch {} for {}: {} records (total: {})",
                        batchCounter, cdlCheckpointName, download.recordCount, totalArchived);

                if (!download.hasMoreRecords) {
                    log.info("CDL archive complete for {} after {} batches", cdlCheckpointName, batchCounter);
                    break;
                }
            }

            // Complete CDL archive run
            try {
                postgresService.completeBackupRun(runId, "SUCCESS", totalArchived, 0, 0, null);
            } catch (SQLException e) {
                log.warn("Failed to complete CDL archive run: {}", e.getMessage());
            }

            log.info("Archived {} ContentDocumentLink records for {} in {} batches",
                    totalArchived, parentObjectName, batchCounter);

            // Record CDL archive statistics
            if (totalArchived > 0) {
                statisticsService.addContentDocumentLinksArchived(totalArchived);
            }

        } catch (Exception e) {
            try {
                postgresService.completeBackupRun(runId, "FAILED", totalArchived, 0, 0, e.getMessage());
            } catch (SQLException ex) {
                log.warn("Failed to mark CDL archive run as failed: {}", ex.getMessage());
            }
            throw e;
        }
    }

    /**
     * Build SOQL query for ContentDocumentLink archive using a subquery
     * against the parent object's archive criteria.
     */
    private String buildContentDocumentLinkSOQL(String parentObjectName, String createdDateThreshold,
            String archiveExpression, String lastModstamp, String lastId, int batchSize) {

        StringBuilder soql = new StringBuilder();
        soql.append("SELECT Id,ContentDocumentId,LinkedEntityId,ShareType,Visibility,SystemModstamp");
        soql.append(" FROM ContentDocumentLink");
        soql.append(" WHERE LinkedEntityId IN (");
        soql.append(String.format("SELECT Id FROM %s WHERE CreatedDate < %s", parentObjectName, createdDateThreshold));
        if (archiveExpression != null && !archiveExpression.isBlank()) {
            soql.append(String.format(" AND (%s)", archiveExpression));
        }
        soql.append(")");
        soql.append(String.format(" AND ((SystemModstamp = %s AND Id > '%s') OR SystemModstamp > %s)",
                lastModstamp, lastId, lastModstamp));
        soql.append(" ORDER BY SystemModstamp ASC, Id ASC");
        soql.append(String.format(" LIMIT %d", batchSize));
        return soql.toString();
    }
}
