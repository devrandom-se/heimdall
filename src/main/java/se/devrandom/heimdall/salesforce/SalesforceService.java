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

import io.jsonwebtoken.Jwts;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.reactive.function.BodyInserters;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import se.devrandom.heimdall.config.SalesforceCredentials;
import se.devrandom.heimdall.salesforce.objects.*;
import se.devrandom.heimdall.salesforce.requests.DescribeGlobal;
import se.devrandom.heimdall.storage.BackupStatisticsService;
import se.devrandom.heimdall.storage.PostgresService;
import se.devrandom.heimdall.storage.S3Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Component
@ConditionalOnProperty(name = "spring.batch.job.enabled", havingValue = "true", matchIfMissing = true)
public class SalesforceService {
    private static final Logger log = LoggerFactory.getLogger(SalesforceService.class);
    private static final Integer DEFAULT_BATCH_SIZE = 50000;
    private static final Integer MIN_BATCH_SIZE = 5000;
    private static final Integer MAX_BATCH_SIZE = 200000;
    private static final Integer CONTENTVERSION_BASE_LIMIT = 4000;
    private static final Integer CONTENTVERSION_CACHED_LIMIT = 50000;
    private static final Integer MAX_RECORDS = 2000;
    private static final Integer BATCH_FAST_THRESHOLD_SECONDS = 30;

    // Dynamic batch size per object - adjusts during runtime
    private final Map<String, Integer> objectBatchSizes = new ConcurrentHashMap<>();
    private final WebClient webClient;
    private final SalesforceCredentials salesforceCredentials;
    private final SalesforceAccessToken salesforceAccessToken;
    private final DescribeGlobal describeGlobal;
    private final Map<String, Heimdall_Backup_Config__c> objectBackup__cMap;

    private final CustomItemReaderWriter<DescribeGlobalResult> sObjectDescriptionIO;
    private final CustomItemReaderWriter<Heimdall_Backup_Config__c> objectBackupIO;
    private final CustomItemReaderWriter<DescribeSObjectResult> describeSObjectResultIO;

    private final S3Service s3Service;
    private final PostgresService postgresService;
    private final BackupStatisticsService statisticsService;

    private final int skipFilesBelowKb;
    private final Set<String> globalChecksumCache;
    private final Map<String, Set<String>> referenceFieldsCache = new ConcurrentHashMap<>();
    private final Optional<ApiLimitTracker> apiLimitTracker;

    private Map<String, Heimdall_Backup_Config__c> downloadObjectBackupRecords() {
        Map<String, Heimdall_Backup_Config__c> resultMap = new HashMap<>();

        try {
            log.info("Fetching existing Heimdall_Backup_Config__c records from Salesforce...");


            String query =  "SELECT Id, ObjectName__c, Status__c, " +
                            "CheckCountFirst__c, ConsecutiveEmptyRuns__c, LastNonEmptyRun__c, " +
                            "ChangeFrequency__c, AverageRecordsPerRun__c, " +
                            "Archive__c, Archive_Age_Days__c, Archive_Expression__c, Archive_Action__c " +
                            "FROM Heimdall_Backup_Config__c";

            WebClient.RequestHeadersSpec<?> request = createQueryWebClient(query);
            SOQLResultObjectBackup result = request
                    .retrieve()
                    .bodyToMono(SOQLResultObjectBackup.class)
                    .block();

            if (result != null && result.records != null) {
                for (Heimdall_Backup_Config__c record : result.records) {
                    resultMap.put(record.getObjectName__c(), record);
                }
                log.info("Fetched {} Heimdall_Backup_Config__c records", result.records.size());
            } else {
                log.warn("No Heimdall_Backup_Config__c records found in Salesforce.");
            }

        } catch (Exception e) {
            log.error("Error while fetching Heimdall_Backup_Config__c records: {}", e.getMessage());
            throw new RuntimeException("Failed to fetch Heimdall_Backup_Config__c records", e);
        }

        return resultMap;
    }

    @Autowired
    public SalesforceService(WebClient webClient, SalesforceCredentials salesforceCredentials,
                             S3Service s3Service, PostgresService postgresService,
                             BackupStatisticsService statisticsService,
                             @Value("${heimdall.backup.skip-files-below-kb:0}") int skipFilesBelowKb,
                             @Value("${heimdall.metadata.auto-migrate:true}") boolean autoMigrateMetadata,
                             Optional<ApiLimitTracker> apiLimitTracker) {
        this.webClient = webClient;
        this.salesforceCredentials = salesforceCredentials;
        this.s3Service = s3Service;
        this.postgresService = postgresService;
        this.statisticsService = statisticsService;
        this.skipFilesBelowKb = skipFilesBelowKb;
        this.globalChecksumCache = ConcurrentHashMap.newKeySet();
        this.apiLimitTracker = apiLimitTracker;

        salesforceAccessToken = loginToSalesforce();

        // Initialize API limit tracker from /limits endpoint
        apiLimitTracker.ifPresent(this::initializeApiLimits);

        // Auto-migrate schema if enabled
        if (autoMigrateMetadata) {
            SalesforceSchemaManager schemaManager = new SalesforceSchemaManager(
                    webClient, salesforceAccessToken, salesforceCredentials);
            schemaManager.ensureSchema();
        }

        describeGlobal = DescribeGlobal.request(salesforceAccessToken, webClient, salesforceCredentials.getApiVersion());
        sObjectDescriptionIO = new CustomItemReaderWriter<DescribeGlobalResult>(describeGlobal.sobjects, this);
        describeSObjectResultIO = new CustomItemReaderWriter<DescribeSObjectResult>(new ArrayList<DescribeSObjectResult>(), this);
        objectBackup__cMap = downloadObjectBackupRecords();
        objectBackupIO = new CustomItemReaderWriter<Heimdall_Backup_Config__c>(objectBackup__cMap.values().stream().toList(), this);

        // Log deduplication configuration
        log.info("ContentVersion size filter: {} KB threshold", skipFilesBelowKb);
        log.info("Global checksum cache initialized (max ~150 MB for 1.7M checksums)");

        // Preload checksum cache from database for instant deduplication
        preloadChecksumCacheFromDatabase();
    }

    private void initializeApiLimits(ApiLimitTracker tracker) {
        try {
            String uri = String.format("/services/data/%s/limits", salesforceCredentials.getApiVersion());
            String responseBody = webClient.get()
                    .uri(uri)
                    .headers(h -> h.setBearerAuth(salesforceAccessToken.accessToken))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            if (responseBody != null) {
                JSONObject limits = new JSONObject(responseBody);
                JSONObject dailyApi = limits.getJSONObject("DailyApiRequests");
                long max = dailyApi.getLong("Max");
                long remaining = dailyApi.getLong("Remaining");
                long currentUsed = max - remaining;
                tracker.initialize(currentUsed, max);
            }
        } catch (Exception e) {
            log.warn("Failed to fetch API limits from Salesforce: {}. API limit tracking will be disabled.", e.getMessage());
        }
    }

    /**
     * Preload checksum cache from PostgreSQL database.
     * This enables instant deduplication for incremental backups without S3 HEAD requests.
     * Cache is populated with all existing ContentVersion checksums from database.
     */
    private void preloadChecksumCacheFromDatabase() {
        try {
            log.info("Preloading checksum cache from PostgreSQL database...");
            long startTime = System.currentTimeMillis();

            // Load all checksums from database
            Set<String> checksums = postgresService.loadContentVersionChecksums();

            // Add to global cache (thread-safe)
            globalChecksumCache.addAll(checksums);

            long elapsedMs = System.currentTimeMillis() - startTime;
            long estimatedMemoryMb = (globalChecksumCache.size() * 88) / 1024 / 1024;

            log.info("Preloaded {} checksums into global cache in {} ms (~{} MB memory)",
                    globalChecksumCache.size(), elapsedMs, estimatedMemoryMb);
            log.info("Cache hit optimization: Files with existing checksums will skip S3 HEAD requests");

        } catch (Exception e) {
            log.warn("Failed to preload checksum cache from database: {}", e.getMessage());
            log.warn("Cache will be populated progressively during processing (slower for incremental backups)");
            // Continue - cache will be built progressively during processing
        }
    }

    /**
     * Get current batch size for an object. Returns default if not yet adjusted.
     */
    public int getBatchSize(String objectName) {
        if ("ContentVersion".equals(objectName)) {
            // If we have a populated checksum cache, most files will be skipped via cache
            // lookup so we can safely use larger batches
            int cachedLimit = globalChecksumCache.isEmpty()
                    ? CONTENTVERSION_BASE_LIMIT
                    : CONTENTVERSION_CACHED_LIMIT;
            return objectBatchSizes.getOrDefault(objectName, cachedLimit);
        }
        return objectBatchSizes.getOrDefault(objectName, DEFAULT_BATCH_SIZE);
    }

    /**
     * Adjust batch size based on how long the batch took.
     * - If timeout/error: halve the batch size
     * - If fast (<30s) and full batch: increase by 50%
     * - Otherwise: keep current size
     *
     * @param objectName The object name
     * @param currentBatchSize The batch size that was used
     * @param durationSeconds How long the batch took
     * @param recordsReturned How many records were returned
     * @param wasTimeout Whether the batch timed out
     */
    public void adjustBatchSize(String objectName, int currentBatchSize, long durationSeconds,
                                int recordsReturned, boolean wasTimeout) {
        int minBatch = "ContentVersion".equals(objectName) ? CONTENTVERSION_BASE_LIMIT : MIN_BATCH_SIZE;
        int maxBatch = "ContentVersion".equals(objectName) ? CONTENTVERSION_CACHED_LIMIT : MAX_BATCH_SIZE;

        int newBatchSize = currentBatchSize;
        String reason;

        if (wasTimeout) {
            // Timeout - halve the batch size
            newBatchSize = Math.max(minBatch, currentBatchSize / 2);
            reason = "timeout";
        } else {
            boolean wasFullBatch = recordsReturned >= currentBatchSize * 0.8;

            if (wasFullBatch && durationSeconds < BATCH_FAST_THRESHOLD_SECONDS) {
                // Fast full batch - increase by 50%
                newBatchSize = Math.min(maxBatch, (int)(currentBatchSize * 1.5));
                reason = "fast batch (" + durationSeconds + "s)";
            } else {
                // Good pace - keep current
                reason = "optimal";
            }
        }

        if (newBatchSize != currentBatchSize) {
            log.info("Adjusting batch size for {}: {} -> {} (reason: {})",
                    objectName, currentBatchSize, newBatchSize, reason);
            objectBatchSizes.put(objectName, newBatchSize);
        }
    }

    private WebClient.RequestHeadersSpec<? extends WebClient.RequestHeadersSpec<?>> createQueryWebClient(
            String queryParam) {
        log.debug("Executing SOQL query: {}", queryParam);
        return webClient
                .get()
                .uri(uriBuilder -> uriBuilder
                        .path("/services/data/" + salesforceCredentials.getApiVersion() + "/query/")
                        .queryParam("q", queryParam)
                        .build())
                .headers(httpHeaders ->
                        httpHeaders.setBearerAuth(this.salesforceAccessToken.accessToken)
                )
                .accept(MediaType.APPLICATION_JSON);
    }

    public void createOrUpdateObjectBackupRecord(Heimdall_Backup_Config__c objectBackup__c) {
        String objectName = objectBackup__c.getObjectName__c();
        if(objectBackup__cMap.containsKey(objectName) && objectBackup__cMap.get(objectName).equalsOnImportantFields(objectBackup__c)) {
            log.debug(String.format("%s is identical", objectBackup__c.getObjectName__c()));
        } else {
            log.debug(String.format("Preparing %s for batch upsert", objectBackup__c.getObjectName__c()));
            // Just update the map - actual upsert will be done in batch by the writer
            objectBackup__cMap.put(objectName, objectBackup__c);
        }
    }

    public void upsertHeimdall_Backup_Config__c(Heimdall_Backup_Config__c c) {
        // Ensure Name is always set to ObjectName__c
        if (c.getName() == null && c.getObjectName__c() != null) {
            c.setName(c.getObjectName__c());
        }
        String jsonBody = c.generateJsonBody(false);

        // Keep reference to existing record if it exists (has Id)
        Heimdall_Backup_Config__c existing = objectBackup__cMap.get(c.getObjectName__c());

        String result = webClient
                .patch()
                .uri("/services/data/" + salesforceCredentials.getApiVersion() + "/sobjects/Heimdall_Backup_Config__c/ObjectName__c/" + c.getObjectName__c())
                .headers(httpHeaders ->
                        httpHeaders.setBearerAuth(this.salesforceAccessToken.accessToken)
                )
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(jsonBody)
                .accept(MediaType.APPLICATION_JSON)
                .retrieve()
                .toBodilessEntity()
                .onErrorResume(WebClientResponseException.class, e -> Mono.just(ResponseEntity.status(e.getStatusCode()).build()))
                .map(entity -> entity.getStatusCode().isError() ? "error" : "ok")
                .block();
        if(result.equals("ok")) {
            // If we had an existing record, preserve its Id
            if (existing != null && existing.getId() != null) {
                c.setId(existing.getId());
            } else {
                // New record - fetch Id from Salesforce
                log.debug("Fetching Id for new Heimdall_Backup_Config__c: {}", c.getObjectName__c());
                String query = String.format("SELECT Id FROM Heimdall_Backup_Config__c WHERE ObjectName__c = '%s' LIMIT 1",
                    c.getObjectName__c());
                try {
                    SOQLResultObjectBackup queryResult = createQueryWebClient(query)
                        .retrieve()
                        .bodyToMono(SOQLResultObjectBackup.class)
                        .block();

                    if (queryResult != null && queryResult.records != null && !queryResult.records.isEmpty()) {
                        String id = queryResult.records.get(0).getId();
                        c.setId(id);
                        log.debug("Fetched Id for {}: {}", c.getObjectName__c(), id);
                    }
                } catch (Exception e) {
                    log.warn("Failed to fetch Id for {}: {}", c.getObjectName__c(), e.getMessage());
                }
            }
            objectBackup__cMap.put(c.getObjectName__c(), c);
            log.info("Upserted {} (Id: {})", c.getObjectName__c(), c.getId());
        } else {
            throw new RuntimeException("Unable to upsert " + c);
        }
    }

    public void batchUpsertHeimdall_Backup_Config__c(List<Heimdall_Backup_Config__c> configs) {
        if (configs == null || configs.isEmpty()) {
            return;
        }

        log.info("Batch upserting {} Heimdall_Backup_Config__c records", configs.size());

        // Split into batches of 200 (Salesforce Composite API limit)
        List<List<Heimdall_Backup_Config__c>> batches = new ArrayList<>();
        for (int i = 0; i < configs.size(); i += 200) {
            batches.add(configs.subList(i, Math.min(i + 200, configs.size())));
        }

        for (List<Heimdall_Backup_Config__c> batch : batches) {
            // Build composite request body
            Map<String, Object> compositeRequest = new HashMap<>();
            compositeRequest.put("allOrNone", false);
            List<Map<String, Object>> records = new ArrayList<>();

            for (Heimdall_Backup_Config__c config : batch) {
                Map<String, Object> record = new HashMap<>();
                record.put("attributes", Map.of("type", "Heimdall_Backup_Config__c"));
                record.put("ObjectName__c", config.getObjectName__c());
                if (config.getStatus__c() != null) {
                    record.put("Status__c", config.getStatus__c());
                }
                records.add(record);
            }
            compositeRequest.put("records", records);

            try {
                String response = webClient
                        .patch()
                        .uri("/services/data/" + salesforceCredentials.getApiVersion() + "/composite/sobjects/Heimdall_Backup_Config__c/ObjectName__c")
                        .headers(httpHeaders -> httpHeaders.setBearerAuth(this.salesforceAccessToken.accessToken))
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(compositeRequest)
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .bodyToMono(String.class)
                        .block();

                log.info("Batch upsert completed for {} records", batch.size());

                // Collect ObjectNames that need Id lookup
                List<String> objectNamesNeedingIds = new ArrayList<>();
                for (Heimdall_Backup_Config__c config : batch) {
                    Heimdall_Backup_Config__c existing = objectBackup__cMap.get(config.getObjectName__c());
                    if (existing == null || existing.getId() == null) {
                        objectNamesNeedingIds.add(config.getObjectName__c());
                    }
                }

                // Fetch all Ids in a single query
                if (!objectNamesNeedingIds.isEmpty()) {
                    String inClause = "'" + String.join("','", objectNamesNeedingIds) + "'";
                    String query = String.format("SELECT Id, ObjectName__c FROM Heimdall_Backup_Config__c WHERE ObjectName__c IN (%s)", inClause);
                    try {
                        SOQLResultObjectBackup queryResult = createQueryWebClient(query)
                            .retrieve()
                            .bodyToMono(SOQLResultObjectBackup.class)
                            .block();

                        if (queryResult != null && queryResult.records != null) {
                            // Build lookup map from results
                            Map<String, String> objectNameToId = new HashMap<>();
                            for (Heimdall_Backup_Config__c record : queryResult.records) {
                                objectNameToId.put(record.getObjectName__c(), record.getId());
                            }
                            // Apply Ids to configs
                            for (Heimdall_Backup_Config__c config : batch) {
                                String id = objectNameToId.get(config.getObjectName__c());
                                if (id != null) {
                                    config.setId(id);
                                }
                            }
                            log.debug("Fetched {} Ids in single query", queryResult.records.size());
                        }
                    } catch (Exception e) {
                        log.warn("Failed to fetch Ids in bulk: {}", e.getMessage());
                    }
                }

                // Update map with all configs
                for (Heimdall_Backup_Config__c config : batch) {
                    objectBackup__cMap.put(config.getObjectName__c(), config);
                }

            } catch (Exception e) {
                log.error("Batch upsert failed: {}", e.getMessage(), e);
                // Fallback to individual upserts
                log.info("Falling back to individual upserts");
                for (Heimdall_Backup_Config__c config : batch) {
                    try {
                        upsertHeimdall_Backup_Config__c(config);
                    } catch (Exception ex) {
                        log.error("Failed to upsert {}: {}", config.getObjectName__c(), ex.getMessage());
                    }
                }
            }
        }
    }

    public static class SalesforceCreateResponse {
        public String id;
        public boolean success;
        public List<String> errors;
    }

    /**
     * Result from a backup batch operation with checkpoint info
     */
    public static class BackupBatchResult {
        public String lastId;
        public java.sql.Timestamp lastModstamp;
        public int totalRecords;
        public int deletedRecords;
        public Path csvPath;

        public BackupBatchResult() {}
    }

    /**
     * Result from downloading CSV (before S3/PostgreSQL processing).
     * Used for pipeline optimization to allow starting the next Bulk Query
     * while processing the current CSV.
     */
    public static class CsvDownloadResult {
        public Path csvPath;
        public String lastId;
        public java.sql.Timestamp lastModstamp;
        public int recordCount;
        public boolean hasMoreRecords;
        public String objectName;
        public boolean isQueryAll;

        public CsvDownloadResult() {}
    }

    /**
     * Runs a fast COUNT() query to check if there are new records since the last backup.
     * This is much faster than creating a Bulk API job.
     *
     * @param objectName The Salesforce object name
     * @param lastModstamp The timestamp from the last successful backup run (ISO format)
     * @return Number of new/updated records since lastModstamp
     */
    public int countNewRecords(String objectName, String lastModstamp) {
        try {
            String countQuery = String.format(
                "SELECT COUNT() FROM %s WHERE SystemModstamp > %s",
                objectName,
                lastModstamp
            );

            log.debug("COUNT query for {}: {}", objectName, countQuery);

            SOQLCountResult result = createQueryWebClient(countQuery)
                    .retrieve()
                    .bodyToMono(SOQLCountResult.class)
                    .block();

            int count = result != null ? result.totalSize : 0;
            log.info("COUNT result for {}: {} new records", objectName, count);

            return count;
        } catch (Exception e) {
            log.warn("COUNT query failed for {}: {}. Falling back to Bulk API.",
                objectName, e.getMessage());
            // If COUNT fails, return -1 to indicate we should proceed with Bulk API
            return -1;
        }
    }

    /**
     * Determines if a COUNT() query should be run before a Bulk API job.
     * Uses adaptive logic based on historical backup patterns.
     *
     * @param backup The Heimdall_Backup_Config__c record for this object
     * @return true if a COUNT check should be performed first
     */
    public boolean shouldCheckCountFirst(Heimdall_Backup_Config__c backup) {
        // Known "hot" objects that are almost always updated - skip COUNT for these
        Set<String> alwaysActiveObjects = Set.of(
            /*"Account", "Opportunity", "Contact", "Lead",
            "Case", "Task", "Event", "User",
            "ContentVersion", "ContentDocument"*/
        );

        if (alwaysActiveObjects.contains(backup.getObjectName__c())) {
            log.debug("{} is a high-activity object, skipping COUNT check", backup.getObjectName__c());
            return false; // Skip COUNT, go straight to Bulk API
        }

        // Manual override - respect the flag if set
        if (backup.getCheckCountFirst__c() != null && backup.getCheckCountFirst__c()) {
            log.debug("{} has CheckCountFirst__c=true, will run COUNT", backup.getObjectName__c());
            return true;
        }

        if (backup.getCheckCountFirst__c() != null && !backup.getCheckCountFirst__c()) {
            log.debug("{} has CheckCountFirst__c=false, skipping COUNT", backup.getObjectName__c());
            return false;
        }

        // Auto-learn: If 3+ consecutive empty runs, start checking COUNT
        if (backup.getConsecutiveEmptyRuns__c() != null && backup.getConsecutiveEmptyRuns__c() >= 3) {
            log.info("{} has {} consecutive empty runs, enabling COUNT check",
                backup.getObjectName__c(), backup.getConsecutiveEmptyRuns__c());
            return true;
        }

        // Default: don't check COUNT (assume objects have changes)
        return false;
    }

    /**
     * Updates backup statistics based on the result of a backup run.
     * This enables adaptive optimization over time.
     *
     * @param backup The Heimdall_Backup_Config__c record to update
     * @param recordCount Number of records backed up in this run (0 if none)
     */
    public void updateBackupStatistics(Heimdall_Backup_Config__c backup, int recordCount) {
        boolean needsUpdate = false;

        if (recordCount == 0) {
            // Increment consecutive empty runs counter
            int emptyRuns = backup.getConsecutiveEmptyRuns__c() != null
                ? backup.getConsecutiveEmptyRuns__c() + 1
                : 1;
            backup.setConsecutiveEmptyRuns__c(emptyRuns);
            needsUpdate = true;

            // Auto-enable COUNT check after 3 empty runs
            if (emptyRuns >= 3 && (backup.getCheckCountFirst__c() == null || !backup.getCheckCountFirst__c())) {
                log.info("{} has been empty for {} runs, auto-enabling COUNT check",
                    backup.getObjectName__c(), emptyRuns);
                backup.setCheckCountFirst__c(true);
                backup.setChangeFrequency__c("Low");
            }
        } else {
            // Reset consecutive empty runs counter
            if (backup.getConsecutiveEmptyRuns__c() != null && backup.getConsecutiveEmptyRuns__c() > 0) {
                backup.setConsecutiveEmptyRuns__c(0);
                needsUpdate = true;
            }

            // Update last non-empty run date
            backup.setLastNonEmptyRun__c(new Date());
            needsUpdate = true;

            // Update average records per run (simple moving average)
            Double currentAvg = backup.getAverageRecordsPerRun__c();
            if (currentAvg == null) {
                backup.setAverageRecordsPerRun__c((double) recordCount);
            } else {
                // Weighted average: 80% old, 20% new (gives more weight to recent history)
                backup.setAverageRecordsPerRun__c(currentAvg * 0.8 + recordCount * 0.2);
            }

            // If object becomes active again (after being marked for COUNT), consider disabling COUNT check
            if (backup.getCheckCountFirst__c() != null && backup.getCheckCountFirst__c() && recordCount > 100) {
                log.info("{} is active again with {} records, consider disabling COUNT check",
                    backup.getObjectName__c(), recordCount);
                backup.setCheckCountFirst__c(false);
                backup.setChangeFrequency__c("High");
            } else if (recordCount > 1000) {
                backup.setChangeFrequency__c("High");
            } else if (recordCount > 10) {
                backup.setChangeFrequency__c("Medium");
            }
        }

        // Only update if something changed
        if (needsUpdate) {
            try {
                upsertHeimdall_Backup_Config__c(backup);
                log.debug("Updated backup statistics for {}: emptyRuns={}, avgRecords={}, changeFreq={}",
                    backup.getObjectName__c(),
                    backup.getConsecutiveEmptyRuns__c(),
                    backup.getAverageRecordsPerRun__c(),
                    backup.getChangeFrequency__c());
            } catch (Exception e) {
                log.warn("Failed to update backup statistics for {}: {}",
                    backup.getObjectName__c(), e.getMessage());
                // Don't throw - statistics update failure shouldn't stop the backup
            }
        }
    }

    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 5000; // 5 seconds

    public BulkQueryRequest createBulkQuery(DescribeSObjectResult item, Boolean queryAll, String lastModstamp, String lastId) {
        int batchSize = getBatchSize(item.name);
        String soqlQuery = item.generateSOQLQuery(queryAll, lastModstamp, lastId, batchSize);
        String operation = queryAll ? "queryAll" : "query";

        log.info("Creating Bulk Query for {} (queryAll={}, batchSize={}): {}", item.name, queryAll, batchSize, soqlQuery);
        return submitBulkQuery(soqlQuery, operation, item.name);
    }

    /**
     * Create a Bulk Query using a raw SOQL string.
     * Uses "query" operation (not queryAll).
     */
    public BulkQueryRequest createBulkQueryWithSOQL(String soqlQuery, String objectName) {
        log.info("Creating Bulk Query for {} with custom SOQL: {}", objectName, soqlQuery);
        return submitBulkQuery(soqlQuery, "query", objectName);
    }

    private BulkQueryRequest submitBulkQuery(String soqlQuery, String operation, String objectName) {
        Map map = new HashMap();
        map.put("operation", operation);
        map.put("contentType", "CSV");
        map.put("lineEnding", "LF");
        map.put("query", soqlQuery);

        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                BulkQueryRequest bulkQueryRequest = webClient
                        .post()
                        .uri("/services/data/" + salesforceCredentials.getApiVersion() + "/jobs/query")
                        .headers(httpHeaders ->
                                httpHeaders.setBearerAuth(this.salesforceAccessToken.accessToken)
                        )
                        .contentType(MediaType.APPLICATION_JSON)
                        .bodyValue(map)
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .bodyToMono(BulkQueryRequest.class).single().block();
                return bulkQueryRequest;
            } catch (Exception e) {
                lastException = e;
                String errorMsg = e.toString().toLowerCase();
                boolean isTransient = errorMsg.contains("connection reset") ||
                                      errorMsg.contains("connection refused") ||
                                      errorMsg.contains("timeout") ||
                                      errorMsg.contains("temporarily unavailable") ||
                                      errorMsg.contains("503") ||
                                      errorMsg.contains("502");

                if (isTransient && attempt < MAX_RETRY_ATTEMPTS) {
                    long delayMs = INITIAL_RETRY_DELAY_MS * (long) Math.pow(2, attempt - 1);
                    log.warn("Transient error on submitBulkQuery attempt {}/{}: {}. Retrying in {} ms...",
                            attempt, MAX_RETRY_ATTEMPTS, e.getMessage(), delayMs);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry delay", ie);
                    }
                } else if (!isTransient) {
                    log.error("Non-transient error on submitBulkQuery for {}: {}", objectName, e.toString());
                    log.error("Failed SOQL query: {}", soqlQuery);
                    if (e instanceof WebClientResponseException) {
                        String responseBody = ((WebClientResponseException) e).getResponseBodyAsString();
                        log.error("Salesforce error response: {}", responseBody);
                    }
                    throw new RuntimeException(e);
                }
            }
        }

        log.error("All {} retry attempts failed for submitBulkQuery: {}", MAX_RETRY_ATTEMPTS, lastException.toString());
        throw new RuntimeException(lastException);
    }

    public BulkQueryRequest checkBulkQueryStatus(BulkQueryRequest bulkQueryRequest) {
        Exception lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                BulkQueryRequest bulkQueryJobStatus = webClient
                        .get()
                        .uri("/services/data/" + salesforceCredentials.getApiVersion() + "/jobs/query/" + bulkQueryRequest.id)
                        .headers(httpHeaders ->
                                httpHeaders.setBearerAuth(this.salesforceAccessToken.accessToken)
                        )
                        .accept(MediaType.APPLICATION_JSON)
                        .retrieve()
                        .bodyToMono(BulkQueryRequest.class).single().block();
                return bulkQueryJobStatus;
            } catch (Exception e) {
                lastException = e;
                String errorMsg = e.toString().toLowerCase();
                boolean isTransient = errorMsg.contains("connection reset") ||
                                      errorMsg.contains("connection refused") ||
                                      errorMsg.contains("timeout") ||
                                      errorMsg.contains("temporarily unavailable") ||
                                      errorMsg.contains("503") ||
                                      errorMsg.contains("502");

                if (isTransient && attempt < MAX_RETRY_ATTEMPTS) {
                    long delayMs = INITIAL_RETRY_DELAY_MS * (long) Math.pow(2, attempt - 1);
                    log.warn("Transient error on checkBulkQueryStatus attempt {}/{}: {}. Retrying in {} ms...",
                            attempt, MAX_RETRY_ATTEMPTS, e.getMessage(), delayMs);
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted during retry delay", ie);
                    }
                } else if (!isTransient) {
                    log.error("Non-transient error on checkBulkQueryStatus: {}", e.toString());
                    throw new RuntimeException(e);
                }
            }
        }

        log.error("All {} retry attempts failed for checkBulkQueryStatus: {}", MAX_RETRY_ATTEMPTS, lastException.toString());
        throw new RuntimeException(lastException);
    }

    private static class CsvLastRow {
        String lastId;
        java.util.Date lastSystemModstamp;
        int rowCount;
        boolean valid = true;
        String validationError;
    }

    /**
     * Exception thrown when CSV validation fails (truncated data, missing Id, etc.)
     */
    public static class CsvValidationException extends RuntimeException {
        public CsvValidationException(String message) {
            super(message);
        }
    }

    /**
     * Reads a complete CSV record (which may span multiple lines if fields contain newlines)
     */
    private String readCsvRecord(java.io.BufferedReader reader) throws IOException {
        StringBuilder record = new StringBuilder();
        boolean inQuotes = false;
        int c;

        while ((c = reader.read()) != -1) {
            char ch = (char) c;

            if (ch == '"') {
                // Check for escaped quote ("")
                reader.mark(1);
                int next = reader.read();
                if (next == '"' && inQuotes) {
                    // Escaped quote - add both to record
                    record.append(ch).append((char) next);
                } else {
                    // Regular quote - toggle inQuotes
                    record.append(ch);
                    inQuotes = !inQuotes;
                    // Put back the character we read ahead
                    if (next != -1) {
                        reader.reset();
                    }
                }
            } else if (ch == '\n' && !inQuotes) {
                // End of record
                return record.toString();
            } else if (ch == '\r') {
                // Handle \r\n line endings - check if next is \n
                reader.mark(1);
                int next = reader.read();
                if (next == '\n' && !inQuotes) {
                    // End of record (Windows line ending)
                    return record.toString();
                } else {
                    // Not end of record, add \r to record
                    record.append(ch);
                    if (next != -1) {
                        reader.reset();
                    }
                }
            } else {
                record.append(ch);
            }
        }

        // End of file - return what we have if anything
        return record.length() > 0 ? record.toString() : null;
    }

    private CsvLastRow extractLastRowInfo(Path csvPath, boolean isQueryAll) {
        CsvLastRow result = new CsvLastRow();
        result.rowCount = 0;

        try (java.io.BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            // Read header as a complete record
            String headerLine = readCsvRecord(reader);
            if (headerLine == null) {
                return result;
            }

            String[] headers = parseCsvLineSimple(headerLine);
            int idIndex = -1;
            int systemModstampIndex = -1;

            for (int i = 0; i < headers.length; i++) {
                if (headers[i].trim().equalsIgnoreCase("Id")) {
                    idIndex = i;
                }
                if (headers[i].trim().equalsIgnoreCase("SystemModstamp")) {
                    systemModstampIndex = i;
                }
            }

            if (idIndex < 0) {
                log.warn("Id column not found in CSV for {}", csvPath.getFileName());
            }
            if (systemModstampIndex < 0) {
                log.warn("SystemModstamp column not found in CSV for {}", csvPath.getFileName());
            }

            // Read all records to find the last one
            String lastRecord = null;
            String record;
            while ((record = readCsvRecord(reader)) != null) {
                if (!record.trim().isEmpty()) {
                    result.rowCount++;
                    lastRecord = record;
                }
            }

            if (lastRecord != null && idIndex >= 0) {
                String[] values = parseCsvLineSimple(lastRecord);

                // VALIDATION: Id column (now last in SOQL) must exist and not be empty
                // If CSV was truncated, the last column will be missing or empty
                if (idIndex >= values.length) {
                    result.valid = false;
                    result.validationError = String.format(
                        "CSV truncated: Id column index %d >= values length %d. Last row: %s",
                        idIndex, values.length, lastRecord.length() > 100 ? lastRecord.substring(0, 100) + "..." : lastRecord);
                    log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
                    return result;
                }

                result.lastId = values[idIndex].trim();

                // VALIDATION: Id must not be empty (indicates truncated data)
                if (result.lastId.isEmpty()) {
                    result.valid = false;
                    result.validationError = String.format(
                        "CSV truncated: Id column is empty on last row. Last row: %s",
                        lastRecord.length() > 100 ? lastRecord.substring(0, 100) + "..." : lastRecord);
                    log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
                    return result;
                }

                // VALIDATION: Id must look like a Salesforce ID (15 or 18 alphanumeric chars)
                if (!result.lastId.matches("^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$")) {
                    result.valid = false;
                    result.validationError = String.format(
                        "CSV corrupted: Invalid Id format '%s' on last row", result.lastId);
                    log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
                    return result;
                }

                log.debug("Extracted and validated lastId: {}", result.lastId);

                if (systemModstampIndex >= 0 && systemModstampIndex < values.length) {
                    String modstampStr = values[systemModstampIndex].trim();
                    try {
                        // Try multiple date formats
                        java.text.SimpleDateFormat formatter;
                        if (modstampStr.contains("+")) {
                            formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                        } else if (modstampStr.endsWith("Z")) {
                            formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                        } else {
                            formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                        }
                        formatter.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
                        result.lastSystemModstamp = formatter.parse(modstampStr);
                        log.debug("Extracted lastSystemModstamp: {}", result.lastSystemModstamp);
                    } catch (java.text.ParseException e) {
                        log.warn("Failed to parse SystemModstamp '{}': {}", modstampStr, e.getMessage());
                    }
                }
            } else if (result.rowCount > 0 && idIndex >= 0) {
                // We had records but couldn't extract last record - something is wrong
                result.valid = false;
                result.validationError = "Had " + result.rowCount + " records but lastRecord was null";
                log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
            }
        } catch (IOException e) {
            log.error("Error reading CSV file {}: {}", csvPath, e.getMessage());
            result.valid = false;
            result.validationError = "IO error: " + e.getMessage();
        }

        return result;
    }

    /**
     * Simple CSV parser that handles quoted fields and escaped quotes ("")
     */
    /**
     * Read a complete CSV line from reader, handling newlines within quoted fields
     */
    private String readCompleteCsvLine(BufferedReader reader) throws IOException {
        StringBuilder line = new StringBuilder();
        String currentLine;
        boolean inQuotes = false;

        while ((currentLine = reader.readLine()) != null) {
            if (line.length() > 0) {
                // Add newline that was consumed by readLine()
                line.append('\n');
            }
            line.append(currentLine);

            // Count quotes to determine if we're inside a quoted field
            for (int i = 0; i < currentLine.length(); i++) {
                char c = currentLine.charAt(i);
                if (c == '"') {
                    // Check if it's an escaped quote
                    if (i + 1 < currentLine.length() && currentLine.charAt(i + 1) == '"') {
                        i++; // Skip escaped quote
                    } else {
                        inQuotes = !inQuotes;
                    }
                }
            }

            // If we're not inside quotes, the line is complete
            if (!inQuotes) {
                return line.toString();
            }
            // Otherwise, continue reading the next line
        }

        // End of file - return what we have (or null if nothing)
        return line.length() > 0 ? line.toString() : null;
    }

    private String[] parseCsvLineSimple(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // Check for escaped quote ("")
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // This is an escaped quote - add one quote to current value
                    current.append('"');
                    i++; // Skip the next quote
                } else {
                    // This is a field delimiter quote - toggle inQuotes
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                values.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        values.add(current.toString());

        return values.toArray(new String[0]);
    }

    /**
     * Download query results from Bulk API and process them
     * @param bulkQueryRequest The bulk query request
     * @param initialRecordCount Starting record count (for resuming)
     * @return BackupBatchResult with checkpoint info
     */
    public BackupBatchResult getQueryResults(BulkQueryRequest bulkQueryRequest, int initialRecordCount) {
        AtomicReference<String> sforceLocator = new AtomicReference<>("");
        AtomicReference<Boolean> downloading = new AtomicReference<>(true);
        AtomicReference<Integer> sforceNumberOfRecords = new AtomicReference<>(-1);
        int totalRecords = initialRecordCount;
        boolean isQueryAll = "queryAll".equals(bulkQueryRequest.operation);
        String objectName = bulkQueryRequest.object;

        BackupBatchResult result = new BackupBatchResult();
        result.totalRecords = initialRecordCount;

        // Base file path (first file without locator suffix)
        Path baseFilePath = Paths.get("/tmp/" + bulkQueryRequest.id + ".csv");
        boolean isFirstFile = true;
        List<Path> tempFilesToDelete = new ArrayList<>();

        while(downloading.get()) {
            String sforceLocatorString = !sforceLocator.get().isEmpty() ? "&locator=" + sforceLocator.get() : "";
            Flux<DataBuffer> dataBufferFlux = webClient
                    .get()
                    .uri(String.format("/services/data/%s/jobs/query/%s/results?maxRecords=%d%s",
                                salesforceCredentials.getApiVersion(),
                                bulkQueryRequest.id,
                                MAX_RECORDS,
                                sforceLocatorString))
                            .headers(httpHeaders ->
                                    httpHeaders.setBearerAuth(this.salesforceAccessToken.accessToken)
                            )
                            .accept(MediaType.APPLICATION_JSON)
                            .exchangeToFlux(response -> {
                                if (response.statusCode().equals(HttpStatus.OK)) {
                                    final String headerSforceLocator = response.headers().header("Sforce-Locator").get(0);
                                    if (headerSforceLocator.equals("null")) {
                                        downloading.set(false);
                                    } else {
                                        sforceLocator.set(headerSforceLocator);
                                    }
                                    // Capture Sforce-NumberOfRecords header for validation (case-insensitive)
                                    String numRecordsValue = null;
                                    for (String headerName : List.of("Sforce-NumberOfRecords", "sforce-numberofrecords", "SFORCE-NUMBEROFRECORDS")) {
                                        List<String> headerValues = response.headers().header(headerName);
                                        if (!headerValues.isEmpty()) {
                                            numRecordsValue = headerValues.get(0);
                                            break;
                                        }
                                    }
                                    if (numRecordsValue != null) {
                                        try {
                                            sforceNumberOfRecords.set(Integer.parseInt(numRecordsValue));
                                        } catch (NumberFormatException e) {
                                            log.warn("Failed to parse Sforce-NumberOfRecords header: {}", numRecordsValue);
                                        }
                                    }
                                    // Update API limit tracker from response header
                                    apiLimitTracker.ifPresent(tracker -> {
                                        List<String> limitHeader = response.headers().header("Sforce-Limit-Info");
                                        if (!limitHeader.isEmpty()) {
                                            tracker.updateFromHeader(limitHeader.get(0));
                                        }
                                    });
                                    return response.bodyToFlux(DataBuffer.class);
                                } else {
                                    log.error(response.createException().toString());
                                    throw new RuntimeException();
                                }
                            });

            Path currentFilePath;
            if (isFirstFile) {
                currentFilePath = baseFilePath;
                isFirstFile = false;
            } else {
                currentFilePath = Paths.get("/tmp/" + bulkQueryRequest.id + ".csv." + sforceLocator.get());
                tempFilesToDelete.add(currentFilePath);
            }

            log.info("Writing to {}", currentFilePath);
            DataBufferUtils.write(dataBufferFlux, currentFilePath, StandardOpenOption.CREATE).block();

            // Concatenate subsequent files to base file (skip header)
            if (!currentFilePath.equals(baseFilePath)) {
                try {
                    log.info("Concatenating {} to {}", currentFilePath, baseFilePath);
                    concatenateCsvFiles(baseFilePath, currentFilePath);
                } catch (IOException e) {
                    log.error("Failed to concatenate CSV files: {}", e.getMessage(), e);
                    throw new RuntimeException("CSV concatenation failed", e);
                }
            }

            // Extract checkpoint info from last row and validate CSV integrity
            CsvLastRow lastRowInfo = extractLastRowInfo(currentFilePath, isQueryAll);

            // VALIDATION 1: Check that CSV parsing succeeded and Id was valid
            if (!lastRowInfo.valid) {
                throw new CsvValidationException(String.format(
                    "CSV validation failed for %s: %s", objectName, lastRowInfo.validationError));
            }

            // VALIDATION 2: Check that row count matches Sforce-NumberOfRecords header
            int expectedRecords = sforceNumberOfRecords.get();
            if (expectedRecords >= 0 && lastRowInfo.rowCount != expectedRecords) {
                throw new CsvValidationException(String.format(
                    "CSV record count mismatch for %s: expected %d (from Sforce-NumberOfRecords header), got %d. File may be truncated.",
                    objectName, expectedRecords, lastRowInfo.rowCount));
            }

            totalRecords += lastRowInfo.rowCount;
            log.debug("CSV validated: {} records match Sforce-NumberOfRecords header", lastRowInfo.rowCount);

            if (lastRowInfo.lastId != null) {
                result.lastId = lastRowInfo.lastId;
                result.lastModstamp = lastRowInfo.lastSystemModstamp != null
                    ? new java.sql.Timestamp(lastRowInfo.lastSystemModstamp.getTime())
                    : null;
                log.info("Downloaded {} records, lastId={}, lastModstamp={}",
                        totalRecords, lastRowInfo.lastId, lastRowInfo.lastSystemModstamp);
            }

            // Reset for next chunk
            sforceNumberOfRecords.set(-1);
        }

        // Update result counts
        if (isQueryAll) {
            result.deletedRecords = totalRecords;
        } else {
            result.totalRecords = totalRecords;
        }
        result.csvPath = baseFilePath;

        // After loop: Upload consolidated CSV to S3 and process into PostgreSQL
        if (totalRecords > 0 && Files.exists(baseFilePath)) {
            try {
                String s3Key = s3Service.uploadCsvToS3(baseFilePath, objectName, isQueryAll);
                log.info("Uploaded consolidated CSV to S3: {}", s3Key);

                Set<String> refFields = getReferenceFields(objectName);
                int insertedRecords = postgresService.processCsvToPostgres(baseFilePath, objectName, s3Key, isQueryAll, refFields);
                log.info("Inserted {} records into PostgreSQL for {}", insertedRecords, objectName);

                // For ContentVersion: Download files (handled separately now)
                if ("ContentVersion".equals(objectName) && !isQueryAll) {
                    log.info("Processing ContentVersion files...");
                    downloadContentVersionFiles(baseFilePath, objectName);
                }

            } catch (IOException | SQLException e) {
                log.error("Failed to process consolidated CSV for {}: {}", objectName, e.getMessage(), e);
            }
        } else if (totalRecords == 0 && Files.exists(baseFilePath)) {
            try {
                Files.delete(baseFilePath);
                log.info("Deleted empty CSV file for {} (0 records)", objectName);
            } catch (IOException e) {
                log.warn("Failed to delete empty CSV file {}: {}", baseFilePath, e.getMessage());
            }
        }

        // Clean up temporary files
        for (Path tempFile : tempFilesToDelete) {
            try {
                if (Files.exists(tempFile)) {
                    Files.delete(tempFile);
                    log.debug("Deleted temporary file: {}", tempFile);
                }
            } catch (IOException e) {
                log.warn("Failed to delete temporary file {}: {}", tempFile, e.getMessage());
            }
        }

        return result;
    }

    /**
     * Download query results from Bulk API without processing to storage.
     * This method only downloads and concatenates CSV chunks, extracting checkpoint info.
     * Use processCsvToStorage() to upload to S3 and insert into PostgreSQL.
     *
     * @param bulkQueryRequest The bulk query request
     * @param initialRecordCount Starting record count (for resuming)
     * @return CsvDownloadResult with CSV path and checkpoint info
     */
    public CsvDownloadResult downloadBulkQueryCsv(BulkQueryRequest bulkQueryRequest, int initialRecordCount) {
        AtomicReference<String> sforceLocator = new AtomicReference<>("");
        AtomicReference<Boolean> downloading = new AtomicReference<>(true);
        AtomicReference<Integer> sforceNumberOfRecords = new AtomicReference<>(-1);
        int totalRecords = initialRecordCount;
        boolean isQueryAll = "queryAll".equals(bulkQueryRequest.operation);
        String objectName = bulkQueryRequest.object;

        CsvDownloadResult result = new CsvDownloadResult();
        result.objectName = objectName;
        result.isQueryAll = isQueryAll;
        result.recordCount = 0;

        // Base file path (first file without locator suffix)
        Path baseFilePath = Paths.get("/tmp/" + bulkQueryRequest.id + ".csv");
        boolean isFirstFile = true;
        List<Path> tempFilesToDelete = new ArrayList<>();

        while(downloading.get()) {
            String sforceLocatorString = !sforceLocator.get().isEmpty() ? "&locator=" + sforceLocator.get() : "";
            String currentLocator = sforceLocator.get(); // Save for retry

            // Retry loop for chunk download and validation
            CsvLastRow lastRowInfo = null;
            Path currentFilePath = null;
            for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
                try {
                    Flux<DataBuffer> dataBufferFlux = webClient
                            .get()
                            .uri(String.format("/services/data/%s/jobs/query/%s/results?maxRecords=%d%s",
                                        salesforceCredentials.getApiVersion(),
                                        bulkQueryRequest.id,
                                        MAX_RECORDS,
                                        sforceLocatorString))
                                    .headers(httpHeaders ->
                                            httpHeaders.setBearerAuth(this.salesforceAccessToken.accessToken)
                                    )
                                    .accept(MediaType.APPLICATION_JSON)
                                    .exchangeToFlux(response -> {
                                        if (response.statusCode().equals(HttpStatus.OK)) {
                                            final String headerSforceLocator = response.headers().header("Sforce-Locator").get(0);
                                            if (headerSforceLocator.equals("null")) {
                                                downloading.set(false);
                                            } else {
                                                sforceLocator.set(headerSforceLocator);
                                            }
                                            // Capture Sforce-NumberOfRecords header for validation
                                            // Note: Header name case changed with Hyperforce migration, so we check multiple variants
                                            String numRecordsValue = null;
                                            for (String headerName : List.of("Sforce-NumberOfRecords", "sforce-numberofrecords", "SFORCE-NUMBEROFRECORDS")) {
                                                List<String> headerValues = response.headers().header(headerName);
                                                if (!headerValues.isEmpty()) {
                                                    numRecordsValue = headerValues.get(0);
                                                    break;
                                                }
                                            }
                                            if (numRecordsValue != null) {
                                                try {
                                                    sforceNumberOfRecords.set(Integer.parseInt(numRecordsValue));
                                                } catch (NumberFormatException e) {
                                                    log.warn("Failed to parse Sforce-NumberOfRecords header: {}", numRecordsValue);
                                                }
                                            }
                                            // Update API limit tracker from response header
                                            apiLimitTracker.ifPresent(tracker -> {
                                                List<String> limitHeader = response.headers().header("Sforce-Limit-Info");
                                                if (!limitHeader.isEmpty()) {
                                                    tracker.updateFromHeader(limitHeader.get(0));
                                                }
                                            });
                                            return response.bodyToFlux(DataBuffer.class);
                                        } else {
                                            log.error(response.createException().toString());
                                            throw new RuntimeException();
                                        }
                                    });

                    if (isFirstFile) {
                        currentFilePath = baseFilePath;
                        isFirstFile = false;
                    } else {
                        currentFilePath = Paths.get("/tmp/" + bulkQueryRequest.id + ".csv." + sforceLocator.get());
                        tempFilesToDelete.add(currentFilePath);
                    }

                    log.info("Writing to {}", currentFilePath);
                    DataBufferUtils.write(dataBufferFlux, currentFilePath, StandardOpenOption.CREATE).block();

                    // Concatenate subsequent files to base file (skip header)
                    if (!currentFilePath.equals(baseFilePath)) {
                        log.info("Concatenating {} to {}", currentFilePath, baseFilePath);
                        concatenateCsvFiles(baseFilePath, currentFilePath);
                    }

                    // Extract checkpoint info from last row and validate CSV integrity
                    lastRowInfo = extractLastRowInfo(currentFilePath, isQueryAll);

                    // VALIDATION 1: Check that CSV parsing succeeded and Id was valid
                    if (!lastRowInfo.valid) {
                        throw new CsvValidationException(String.format(
                            "CSV validation failed for %s: %s", objectName, lastRowInfo.validationError));
                    }

                    // VALIDATION 2: Check that row count matches Sforce-NumberOfRecords header
                    int expectedRecords = sforceNumberOfRecords.get();
                    if (expectedRecords >= 0 && lastRowInfo.rowCount != expectedRecords) {
                        throw new CsvValidationException(String.format(
                            "CSV record count mismatch for %s: expected %d (from Sforce-NumberOfRecords header), got %d. File may be truncated.",
                            objectName, expectedRecords, lastRowInfo.rowCount));
                    }

                    log.debug("CSV validated: {} records match Sforce-NumberOfRecords header", lastRowInfo.rowCount);
                    break; // Success - exit retry loop

                } catch (CsvValidationException e) {
                    if (attempt == MAX_RETRY_ATTEMPTS) {
                        log.error("CSV validation failed after {} attempts for {}: {}", MAX_RETRY_ATTEMPTS, objectName, e.getMessage());
                        throw e;
                    }
                    log.warn("CSV validation failed for {} (attempt {}/{}), retrying in 5 seconds: {}",
                        objectName, attempt, MAX_RETRY_ATTEMPTS, e.getMessage());
                    // Reset locator to retry same chunk
                    sforceLocator.set(currentLocator);
                    sforceNumberOfRecords.set(-1);
                    // Delete potentially corrupt file before retry
                    if (currentFilePath != null && Files.exists(currentFilePath)) {
                        try { Files.delete(currentFilePath); } catch (IOException ignored) {}
                    }
                    try { Thread.sleep(5000); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                } catch (IOException e) {
                    log.error("Failed to concatenate CSV files: {}", e.getMessage(), e);
                    throw new RuntimeException("CSV concatenation failed", e);
                }
            }

            totalRecords += lastRowInfo.rowCount;

            if (lastRowInfo.lastId != null) {
                result.lastId = lastRowInfo.lastId;
                result.lastModstamp = lastRowInfo.lastSystemModstamp != null
                    ? new java.sql.Timestamp(lastRowInfo.lastSystemModstamp.getTime())
                    : null;
                log.info("Downloaded {} records, lastId={}, lastModstamp={}",
                        totalRecords, lastRowInfo.lastId, lastRowInfo.lastSystemModstamp);
            }

            // Reset for next chunk
            sforceNumberOfRecords.set(-1);
        }

        // Clean up temporary files
        for (Path tempFile : tempFilesToDelete) {
            try {
                if (Files.exists(tempFile)) {
                    Files.delete(tempFile);
                    log.debug("Deleted temporary file: {}", tempFile);
                }
            } catch (IOException e) {
                log.warn("Failed to delete temporary file {}: {}", tempFile, e.getMessage());
            }
        }

        // Set result fields
        result.csvPath = baseFilePath;
        result.recordCount = totalRecords - initialRecordCount;  // Records in THIS batch
        // hasMoreRecords is true if we got a full batch
        int batchSize = getBatchSize(objectName);
        result.hasMoreRecords = result.recordCount >= batchSize;

        return result;
    }

    /**
     * Process a downloaded CSV file to storage (S3 and PostgreSQL).
     * This method handles the storage operations separately from downloading,
     * allowing for pipeline optimization.
     *
     * @param csvDownloadResult The result from downloadBulkQueryCsv()
     */
    public void processCsvToStorage(CsvDownloadResult csvDownloadResult) {
        Path csvPath = csvDownloadResult.csvPath;
        String objectName = csvDownloadResult.objectName;
        boolean isQueryAll = csvDownloadResult.isQueryAll;
        int recordCount = csvDownloadResult.recordCount;

        if (recordCount > 0 && Files.exists(csvPath)) {
            try {
                String s3Key = s3Service.uploadCsvToS3(csvPath, objectName, isQueryAll);
                log.info("Uploaded consolidated CSV to S3: {}", s3Key);

                Set<String> refFields = getReferenceFields(objectName);
                int insertedRecords = postgresService.processCsvToPostgres(csvPath, objectName, s3Key, isQueryAll, refFields);
                log.info("Inserted {} records into PostgreSQL for {}", insertedRecords, objectName);

                // For ContentVersion: Download files (handled separately now)
                if ("ContentVersion".equals(objectName) && !isQueryAll) {
                    log.info("Processing ContentVersion files...");
                    downloadContentVersionFiles(csvPath, objectName);
                }

            } catch (IOException | SQLException e) {
                log.error("Failed to process consolidated CSV for {}: {}", objectName, e.getMessage(), e);
            }
        } else if (recordCount == 0 && Files.exists(csvPath)) {
            try {
                Files.delete(csvPath);
                log.info("Deleted empty CSV file for {} (0 records)", objectName);
            } catch (IOException e) {
                log.warn("Failed to delete empty CSV file {}: {}", csvPath, e.getMessage());
            }
        }
    }

    /**
     * Process a downloaded CSV to archive storage (S3 and PostgreSQL) with explicit archive periods.
     * No ContentVersion file download for archive.
     */
    public void processArchiveCsvToStorage(CsvDownloadResult csvDownloadResult,
                                            int archivePeriodInt, String archivePeriodS3) {
        Path csvPath = csvDownloadResult.csvPath;
        String objectName = csvDownloadResult.objectName;
        int recordCount = csvDownloadResult.recordCount;

        if (recordCount > 0 && Files.exists(csvPath)) {
            try {
                String s3Key = s3Service.uploadCsvToS3(csvPath, objectName, false, archivePeriodS3);
                log.info("Uploaded archive CSV to S3: {}", s3Key);

                Set<String> refFields = getReferenceFields(objectName);
                int insertedRecords = postgresService.processCsvToPostgres(csvPath, objectName, s3Key, false, archivePeriodInt, refFields);
                log.info("Inserted {} archive records into PostgreSQL for {}", insertedRecords, objectName);
            } catch (IOException | SQLException e) {
                log.error("Failed to process archive CSV for {}: {}", objectName, e.getMessage(), e);
            }
        } else if (recordCount == 0 && Files.exists(csvPath)) {
            try {
                Files.delete(csvPath);
                log.info("Deleted empty archive CSV file for {} (0 records)", objectName);
            } catch (IOException e) {
                log.warn("Failed to delete empty archive CSV file {}: {}", csvPath, e.getMessage());
            }
        }
    }

    /**
     * Concatenate CSV file by appending rows from source to destination (skipping header)
     */
    private void concatenateCsvFiles(Path destination, Path source) throws IOException {
        try (var reader = Files.newBufferedReader(source);
             var writer = Files.newBufferedWriter(destination, StandardOpenOption.APPEND)) {

            // Skip header line from source file
            String headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("Source file {} is empty, skipping concatenation", source);
                return;
            }

            // Append all data rows to destination
            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
                lineCount++;
            }

            log.debug("Concatenated {} lines from {} to {}", lineCount, source, destination);
        }
    }

    /**
     * Download all ContentVersion files from CSV and upload to S3 in parallel.
     * Uses 10 concurrent threads for high-throughput processing.
     * Only downloads files that don't exist in S3 or have different checksums.
     * Uses PostgreSQL for checkpoint tracking.
     */
    public void downloadContentVersionFiles(Path csvPath, String objectName) throws IOException {
        if (!"ContentVersion".equals(objectName)) {
            log.debug("Skipping file download - not ContentVersion");
            return;
        }

        log.info("Processing ContentVersion files from CSV: {}", csvPath);

        // Parse CSV upfront to collect all ContentVersion records
        List<ContentVersionRecord> records = parseContentVersionCsv(csvPath);

        if (records.isEmpty()) {
            log.info("No ContentVersion records to process");
            return;
        }

        log.info("Found {} ContentVersion records, starting parallel processing with 10 threads", records.size());
        log.info("Global checksum cache status: {} checksums cached (cache persists across all batches)",
                globalChecksumCache.size());

        // Create or resume backup run in Postgres for fast checkpoint tracking
        long runId;
        try {
            // Check if there's an existing run to resume
            PostgresService.BackupRun existingRun = postgresService.getLatestBackupRun(objectName, false);
            if (existingRun != null) {
                runId = existingRun.runId;
                log.info("Resuming existing backup run: runId={}, lastCheckpoint={}",
                    runId, existingRun.lastCheckpointId);
            } else {
                // Create new run
                runId = postgresService.createBackupRun(objectName, false);
                log.info("Created new backup run: runId={}", runId);

            }
        } catch (SQLException e) {
            log.error("Failed to create/resume backup run in Postgres: {}", e.getMessage());
            throw new IOException("Cannot track backup progress", e);
        }

        // Create progress tracker with Postgres-based checkpoint (every 20 files - fast local DB!)
        final long finalRunId = runId;
        ContentVersionProgressTracker progressTracker = new ContentVersionProgressTracker(
            finalRunId,
            20,  // Checkpoint every 20 files (was 100 with Salesforce API)
            checkpoint -> {
                try {
                    // Fast local Postgres checkpoint (~1ms vs ~300ms for Salesforce API)
                    java.sql.Timestamp modstamp = checkpoint.lastSystemModstamp != null
                        ? new java.sql.Timestamp(checkpoint.lastSystemModstamp.getTime())
                        : null;
                    postgresService.updateBackupRunCheckpoint(
                        finalRunId,
                        checkpoint.lastId,
                        modstamp,
                        checkpoint.completedCount
                    );
                    log.debug("Checkpoint saved to Postgres: lastId={}, completed={}",
                        checkpoint.lastId, checkpoint.completedCount);
                } catch (Exception e) {
                    log.error("Failed to save checkpoint to Postgres: {}", e.getMessage());
                    // Don't throw - checkpoint failure shouldn't stop backup
                }
            }
        );

        // Create thread pool for parallel downloads (10 concurrent downloads)
        ExecutorService executor = Executors.newFixedThreadPool(10, new ThreadFactory() {
            private final AtomicReference<Integer> counter = new AtomicReference<>(0);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("cv-download-" + counter.getAndSet(counter.get() + 1));
                thread.setDaemon(false);
                return thread;
            }
        });

        try {
            // Wrapper class to track future with metadata
            class FutureWithMetadata {
                final Future<ContentVersionResult> future;
                final String recordId;
                final Date systemModstamp;

                FutureWithMetadata(Future<ContentVersionResult> future, String recordId, Date systemModstamp) {
                    this.future = future;
                    this.recordId = recordId;
                    this.systemModstamp = systemModstamp;
                }
            }

            List<FutureWithMetadata> futures = new ArrayList<>();
            int globalCacheHits = 0;
            int submittedCount = 0;
            boolean apiLimitStopped = false;

            // Submit all tasks
            for (ContentVersionRecord record : records) {
                // Check API limit every 100 submissions
                if (submittedCount > 0 && submittedCount % 100 == 0
                        && apiLimitTracker.isPresent() && apiLimitTracker.get().isLimitReached()) {
                    log.warn("API limit reached - stopping ContentVersion downloads after {} submissions ({} in-flight tasks will complete)",
                            submittedCount, futures.size());
                    apiLimitStopped = true;
                    break;
                }

                statisticsService.incrementContentVersionTotal();

                // Skip deleted records
                if (record.isDeleted()) {
                    log.debug("Skipping deleted ContentVersion: {}", record.recordId());
                    statisticsService.incrementContentVersionSkipped();
                    continue;
                }

                // Build checksum key for cache
                String checksumKey = record.checksum() + "." + record.fileExtension();

                // FAST PATH: Check global cache first (no S3 HEAD needed!)
                if (globalChecksumCache.contains(checksumKey)) {
                    statisticsService.incrementContentVersionSkipped();
                    statisticsService.incrementContentVersionSkippedDuplicate();
                    if (record.contentSize() > 0) {
                        statisticsService.addBytesDeduped(record.contentSize());
                    }
                    globalCacheHits++;
                    continue;  // Skip without creating task
                }

                // Convert LocalDateTime to Date for checkpoint tracking
                Date systemModstamp = java.sql.Timestamp.valueOf(record.createdDate());

                // Mark as started in progress tracker
                progressTracker.markStarted(record.recordId(), systemModstamp);

                // Not in cache - create download task (will check S3 and update cache)
                ContentVersionDownloadTask task = new ContentVersionDownloadTask(
                        record.recordId(),
                        record.checksum(),
                        record.fileExtension(),
                        record.createdDate(),
                        record.isDeleted(),
                        record.contentSize(),
                        webClient,
                        salesforceAccessToken.accessToken,
                        salesforceCredentials.getApiVersion(),
                        s3Service,
                        statisticsService,
                        skipFilesBelowKb,
                        globalChecksumCache,
                        apiLimitTracker.orElse(null)
                );

                futures.add(new FutureWithMetadata(executor.submit(task), record.recordId(), systemModstamp));
                submittedCount++;
            }

            if (apiLimitStopped) {
                log.warn("ContentVersion processing stopped early due to API limit - {} of {} records submitted",
                        submittedCount, records.size());
            }

            log.info("Global cache optimization: {} checksums skipped without S3 HEAD (cache size: {})",
                    globalCacheHits, globalChecksumCache.size());

            // Wait for all tasks to complete and collect results
            int successCount = 0;
            int skipCount = 0;
            int failCount = 0;

            for (FutureWithMetadata futureWithMeta : futures) {
                try {
                    // Wait up to 10 minutes per file (generous for 16MB download + upload with retries)
                    ContentVersionResult result = futureWithMeta.future.get(10, TimeUnit.MINUTES);
                    switch (result.status()) {
                        case SUCCESS -> {
                            successCount++;
                            progressTracker.markCompleted(futureWithMeta.recordId, futureWithMeta.systemModstamp);
                        }
                        case SKIPPED_DUPLICATE -> {
                            skipCount++;
                            progressTracker.markCompleted(futureWithMeta.recordId, futureWithMeta.systemModstamp);
                        }
                        case SKIPPED, SKIPPED_SIZE -> {
                            skipCount++;
                            progressTracker.markCompleted(futureWithMeta.recordId, futureWithMeta.systemModstamp);
                        }
                        case FAILED -> {
                            failCount++;
                            progressTracker.markFailed(futureWithMeta.recordId);
                        }
                    }
                } catch (TimeoutException e) {
                    log.error("ContentVersion task timed out after 10 minutes - cancelling task", e);
                    futureWithMeta.future.cancel(true); // Attempt to cancel the hung task
                    failCount++;
                    progressTracker.markFailed(futureWithMeta.recordId);
                    statisticsService.incrementContentVersionFailed("TIMEOUT");
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Interrupted while waiting for ContentVersion task", e);
                    failCount++;
                    progressTracker.markFailed(futureWithMeta.recordId);
                } catch (ExecutionException e) {
                    log.error("Error executing ContentVersion task", e);
                    failCount++;
                    progressTracker.markFailed(futureWithMeta.recordId);
                }
            }

            // Final checkpoint at end of batch
            progressTracker.finalCheckpoint();

            log.info("ContentVersion processing complete: Total={}, Downloaded={}, Skipped={}, Failed={}",
                    records.size(), successCount, skipCount, failCount);

            // Complete backup run in Postgres with final statistics
            try {
                String status = failCount > 0 ? "COMPLETED_WITH_ERRORS" : "SUCCESS";
                long bytesTransferred = statisticsService.getBytesTransferred();
                postgresService.completeBackupRun(
                    finalRunId,
                    status,
                    successCount + skipCount,
                    failCount,
                    bytesTransferred,
                    failCount > 0 ? String.format("%d files failed", failCount) : null
                );

                log.info("Backup run completed with status: {}", status);
            } catch (SQLException e) {
                log.error("Failed to complete backup run in Postgres: {}", e.getMessage());
            }

        } finally {
            // Shutdown executor
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.warn("Executor did not terminate in 60 seconds, forcing shutdown");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                log.error("Interrupted while waiting for executor shutdown", e);
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    /**
     * Parse ContentVersion CSV file into list of records
     */
    private List<ContentVersionRecord> parseContentVersionCsv(Path csvPath) throws IOException {
        List<ContentVersionRecord> records = new ArrayList<>();

        try (var reader = Files.newBufferedReader(csvPath)) {
            // Read header
            String headerLine = readCompleteCsvLine(reader);
            if (headerLine == null) {
                log.warn("CSV is empty: {}", csvPath);
                return records;
            }

            String[] headers = parseCsvLineSimple(headerLine);

            // Find required column indices
            int idIndex = findColumnIndex(headers, "Id");
            int checksumIndex = findColumnIndex(headers, "Checksum");
            int fileTypeIndex = findColumnIndex(headers, "FileType");
            int fileExtensionIndex = findColumnIndex(headers, "FileExtension");
            int createdDateIndex = findColumnIndex(headers, "CreatedDate");
            int isDeletedIndex = findColumnIndex(headers, "IsDeleted");
            int contentSizeIndex = findColumnIndex(headers, "ContentSize");

            if (idIndex < 0) {
                throw new IOException("Missing required column 'Id' in ContentVersion CSV");
            }

            log.debug("ContentVersion CSV columns: Id={}, Checksum={}, FileType={}, FileExtension={}, CreatedDate={}, IsDeleted={}, ContentSize={}",
                    idIndex, checksumIndex, fileTypeIndex, fileExtensionIndex, createdDateIndex, isDeletedIndex, contentSizeIndex);

            // Read all records
            String line;
            while ((line = readCompleteCsvLine(reader)) != null) {
                if (line.trim().isEmpty()) {
                    continue;
                }

                String[] values = parseCsvLineSimple(line);
                if (values.length != headers.length) {
                    log.warn("Skipping malformed line (expected {} columns, got {})", headers.length, values.length);
                    continue;
                }

                try {
                    String recordId = values[idIndex];
                    String checksum = checksumIndex >= 0 ? values[checksumIndex] : null;
                    String fileType = fileTypeIndex >= 0 ? values[fileTypeIndex] : null;
                    String fileExtension = fileExtensionIndex >= 0 ? values[fileExtensionIndex] : null;
                    String createdDateStr = createdDateIndex >= 0 ? values[createdDateIndex] : null;
                    boolean isDeleted = isDeletedIndex >= 0 && "true".equalsIgnoreCase(values[isDeletedIndex]);

                    // Validate required fields
                    if (checksum == null || checksum.isEmpty()) {
                        log.warn("Skipping ContentVersion {} - missing checksum", recordId);
                        continue;
                    }

                    // Use FileExtension if available, fallback to FileType
                    String extension = (fileExtension != null && !fileExtension.isEmpty()) ? fileExtension : fileType;
                    if (extension == null || extension.isEmpty()) {
                        log.warn("Skipping ContentVersion {} - missing file extension", recordId);
                        continue;
                    }

                    // Parse CreatedDate
                    LocalDateTime createdDate;
                    if (createdDateStr != null && !createdDateStr.isEmpty()) {
                        try {
                            createdDate = LocalDateTime.parse(createdDateStr,
                                    java.time.format.DateTimeFormatter.ISO_DATE_TIME);
                        } catch (Exception e) {
                            log.warn("Failed to parse CreatedDate '{}' for {}, using current date", createdDateStr, recordId);
                            createdDate = LocalDateTime.now();
                        }
                    } else {
                        createdDate = LocalDateTime.now();
                    }

                    // Parse ContentSize
                    long contentSize = 0;
                    if (contentSizeIndex >= 0 && values[contentSizeIndex] != null && !values[contentSizeIndex].isEmpty()) {
                        try {
                            contentSize = Long.parseLong(values[contentSizeIndex]);
                        } catch (NumberFormatException e) {
                            log.warn("Failed to parse ContentSize for {}: {}", recordId, values[contentSizeIndex]);
                        }
                    }

                    // Add to records list
                    records.add(new ContentVersionRecord(recordId, checksum, extension, createdDate, isDeleted, contentSize));

                } catch (Exception e) {
                    log.error("Error parsing ContentVersion record: {}", e.getMessage());
                }
            }
        }

        return records;
    }

    /**
     * Record class for ContentVersion data from CSV
     */
    private record ContentVersionRecord(
            String recordId,
            String checksum,
            String fileExtension,
            LocalDateTime createdDate,
            boolean isDeleted,
            long contentSize
    ) {
    }

    /**
     * Find column index by name (case-insensitive)
     */
    private int findColumnIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    public static class SOQLResultObjectBackup extends SOQLResult<Heimdall_Backup_Config__c> {}

    public static class SOQLCountResult {
        public int totalSize;
        public boolean done;
    }

    private BulkQueryResult pollAndFetchResults(BulkQueryRequest request) {
        String jobId = request.getId();
        BulkQueryResult result = null;

        while (true) {
            result = webClient.get()
                    .uri("/services/data/" + salesforceCredentials.getApiVersion() + "/jobs/query/" + jobId)
                    .headers(h -> h.setBearerAuth(salesforceAccessToken.accessToken))
                    .retrieve()
                    .bodyToMono(BulkQueryResult.class)
                    .block();

            if ("JobComplete".equals(result.getState())) {
                break;
            }

            if ("Failed".equals(result.getState())) {
                throw new RuntimeException("Bulk query job failed: " + result.getErrorMessage());
            }

            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        return result;
    }

    private Path writeResultToFile(BulkQueryResult result, Heimdall_Backup_Config__c item, int batchNumber) {
        String filename = String.format("/tmp/%s_%02d.csv", item.getObjectName__c(), batchNumber);
        Path path = Paths.get(filename);

        byte[] bytes = webClient.get()
                .uri(result.getResultUrl()) // t.ex. /services/data/v61.0/jobs/query/<id>/results
                .headers(h -> h.setBearerAuth(salesforceAccessToken.accessToken))
                .retrieve()
                .bodyToMono(byte[].class)
                .block();

        try {
            Files.write(path, bytes);
        } catch (IOException e) {
            throw new RuntimeException("Failed writing result to file " + filename, e);
        }

        return path;
    }

    public DescribeSObjectResult describeSObject(String objectName) {
        try {
            log.info("Describing {}", objectName);

            DescribeSObjectResult result = webClient
                    .get()
                    .uri("/services/data/" + salesforceCredentials.getApiVersion() + "/sobjects/" + objectName + "/describe")
                    .headers(headers ->
                            headers.setBearerAuth(this.salesforceAccessToken.accessToken)
                    )
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(DescribeSObjectResult.class)
                    .block();

            if (result == null) {
                throw new RuntimeException("Null response from describe for " + objectName);
            }

            log.debug("Successfully described {}", objectName);
            return result;
        } catch (WebClientResponseException e) {
            log.error("Salesforce describe call failed for {}: {} - {}", objectName, e.getStatusCode(), e.getResponseBodyAsString());
            throw new RuntimeException("Failed to describe SObject " + objectName, e);
        } catch (Exception e) {
            log.error("Unexpected error describing {}: {}", objectName, e.toString());
            throw new RuntimeException("Unexpected error describing " + objectName, e);
        }
    }

    /**
     * Get reference field names for an object (cached).
     * Uses describe metadata to determine which fields are lookups.
     */
    public Set<String> getReferenceFields(String objectName) {
        return referenceFieldsCache.computeIfAbsent(objectName, name -> {
            try {
                DescribeSObjectResult describe = describeSObject(name);
                return describe.getReferenceFieldNames();
            } catch (Exception e) {
                log.warn("Failed to get reference fields for {}, using empty set: {}", name, e.getMessage());
                return Set.of();
            }
        });
    }

    /**
     * Cache reference fields from an already-fetched describe result.
     */
    public void cacheReferenceFields(String objectName, DescribeSObjectResult describe) {
        if (describe != null) {
            referenceFieldsCache.put(objectName, describe.getReferenceFieldNames());
        }
    }


    /**
     * Authenticate to Salesforce using either JWT or OAuth2 client credentials
     * Automatically detects which method to use based on configuration
     */
    private SalesforceAccessToken loginToSalesforce() {
        String grantType = salesforceCredentials.getGrantType();

        if ("urn:ietf:params:oauth:grant-type:jwt-bearer".equals(grantType)) {
            log.info("Using JWT authentication");
            return loginWithJWT();
        } else {
            log.info("Using OAuth2 client credentials authentication");
            return loginWithClientCredentials();
        }
    }

    /**
     * JWT Bearer Token Flow authentication
     * Used for server-to-server integration (same as sf cli JWT auth)
     */
    private SalesforceAccessToken loginWithJWT() {
        try {
            // 1. Read private key from file
            PrivateKey privateKey = readPrivateKey(salesforceCredentials.getJwtKeyFile());

            // 2. Build JWT token
            long nowMillis = System.currentTimeMillis();
            long expMillis = nowMillis + (5 * 60 * 1000); // 5 minutes expiry

            String jwt = Jwts.builder()
                    .issuer(salesforceCredentials.getClientId())           // iss = Connected App Client ID
                    .subject(salesforceCredentials.getUsername())          // sub = Salesforce username
                    .audience().add(salesforceCredentials.getAudienceUrl()).and() // aud = https://login.salesforce.com (or test.salesforce.com)
                    .expiration(new Date(expMillis))                       // exp = expiration time
                    .signWith(privateKey)                                  // Sign with RSA private key
                    .compact();

            log.debug("Created JWT token for user: {}", salesforceCredentials.getUsername());

            // 3. Exchange JWT for access token
            LinkedMultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
            formData.add("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer");
            formData.add("assertion", jwt);

            SalesforceAccessToken accessToken = webClient
                    .post()
                    .uri("/services/oauth2/token")
                    .header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                    .body(BodyInserters.fromFormData(formData))
                    .accept(MediaType.APPLICATION_JSON)
                    .retrieve()
                    .bodyToMono(SalesforceAccessToken.class)
                    .block();

            log.info("Successfully authenticated with JWT for user: {}", salesforceCredentials.getUsername());
            return accessToken;

        } catch (Exception e) {
            log.error("JWT authentication failed: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to authenticate with JWT", e);
        }
    }

    /**
     * OAuth2 Client Credentials Flow authentication (legacy/fallback method)
     */
    private SalesforceAccessToken loginWithClientCredentials() {
        WebClient.UriSpec<WebClient.RequestBodySpec> uriSpec = webClient.post();
        WebClient.RequestBodySpec bodySpec = uriSpec.uri("/services/oauth2/token");
        LinkedMultiValueMap map = new LinkedMultiValueMap();
        map.add("grant_type", salesforceCredentials.getGrantType());
        map.add("client_id", salesforceCredentials.getClientId());
        map.add("client_secret", salesforceCredentials.getClientSecret());
        WebClient.RequestHeadersSpec<?> headersSpec = bodySpec.body(
                BodyInserters.fromMultipartData(map));
        headersSpec.header(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_FORM_URLENCODED_VALUE)
                .accept(MediaType.APPLICATION_JSON)
                .acceptCharset(StandardCharsets.UTF_8);
        SalesforceAccessToken salesforceAccessToken = headersSpec.retrieve()
                .bodyToMono(SalesforceAccessToken.class).single().block();
        return salesforceAccessToken;
    }

    /**
     * Read RSA private key from PEM file
     * Supports both PKCS#1 and PKCS#8 formats
     */
    private PrivateKey readPrivateKey(String keyFilePath) throws Exception {
        if (keyFilePath == null || keyFilePath.isEmpty()) {
            throw new IllegalArgumentException("JWT key file path is not configured");
        }

        Path keyPath = Paths.get(keyFilePath);
        if (!Files.exists(keyPath)) {
            throw new IOException("JWT key file not found: " + keyFilePath);
        }

        log.debug("Reading private key from: {}", keyFilePath);
        String keyContent = Files.readString(keyPath, StandardCharsets.UTF_8);

        // Remove PEM headers/footers and whitespace
        keyContent = keyContent
                .replaceAll("-----BEGIN.*-----", "")
                .replaceAll("-----END.*-----", "")
                .replaceAll("\\s+", "");

        byte[] keyBytes = Base64.getDecoder().decode(keyContent);

        try {
            // Try PKCS#8 format first (standard format)
            PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
            KeyFactory keyFactory = KeyFactory.getInstance("RSA");
            return keyFactory.generatePrivate(keySpec);
        } catch (Exception e) {
            log.error("Failed to read private key from {}: {}", keyFilePath, e.getMessage());
            throw new RuntimeException("Invalid private key format. Ensure the file is in PKCS#8 PEM format.", e);
        }
    }

    public CustomItemReaderWriter<DescribeGlobalResult> getDescribeGlobalResultIO() {
        return sObjectDescriptionIO;
    }
    public CustomItemReaderWriter<Heimdall_Backup_Config__c> getObjectBackupIO() {
        return objectBackupIO;
    }

    public CustomItemReaderWriter<DescribeSObjectResult> getDescribeSObjectResultIO() {
        return describeSObjectResultIO;
    }

    public Heimdall_Backup_Config__c getObjectBackup(String objectName) {
        return objectBackup__cMap.get(objectName);
    }

    public Map<String, Heimdall_Backup_Config__c> getObjectBackup__cMap() {
        return objectBackup__cMap;
    }

    public Optional<ApiLimitTracker> getApiLimitTracker() {
        return apiLimitTracker;
    }
}
