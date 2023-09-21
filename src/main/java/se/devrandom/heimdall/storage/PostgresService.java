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

import jakarta.annotation.PostConstruct;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class PostgresService {
    private static final Logger log = LoggerFactory.getLogger(PostgresService.class);
    private static final int BATCH_SIZE = 1000;

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private final String orgId;
    private final Environment environment;
    private final Optional<RdsLifecycleService> rdsLifecycleService;

    @Autowired
    public PostgresService(
            @Value("${postgres.backup.url}") String jdbcUrl,
            @Value("${postgres.backup.username}") String username,
            @Value("${postgres.backup.password}") String password,
            @Value("${salesforce.org-id}") String orgId,
            Environment environment,
            Optional<RdsLifecycleService> rdsLifecycleService) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.orgId = orgId;
        this.environment = environment;
        this.rdsLifecycleService = rdsLifecycleService;
        log.info("PostgresService initialized with backup database: {}", jdbcUrl);
    }

    /**
     * Calculate current period as YYMM (e.g., 2511 for November 2025)
     */
    private int getCurrentPeriod() {
        LocalDateTime now = LocalDateTime.now();
        int year = now.getYear() % 100; // Last 2 digits of year
        int month = now.getMonthValue();
        return year * 100 + month;
    }

    /**
     * Process CSV file and insert records into PostgreSQL database
     * Stores only metadata (reference fields) to minimize storage
     *
     * @param csvPath Path to the CSV file
     * @param objectName Salesforce object name
     * @param s3Key S3 key where the CSV was uploaded
     * @param queryAll Whether this is a queryAll (deleted records) backup
     * @return Number of records inserted
     */
    public int processCsvToPostgres(Path csvPath, String objectName, String s3Key, boolean queryAll) throws IOException, SQLException {
        return processCsvToPostgres(csvPath, objectName, s3Key, queryAll, getCurrentPeriod(), null);
    }

    /**
     * Process CSV file with reference fields from describe metadata.
     */
    public int processCsvToPostgres(Path csvPath, String objectName, String s3Key, boolean queryAll,
                                     Set<String> referenceFields) throws IOException, SQLException {
        return processCsvToPostgres(csvPath, objectName, s3Key, queryAll, getCurrentPeriod(), referenceFields);
    }

    /**
     * Process CSV file and insert records into PostgreSQL database with explicit period.
     */
    public int processCsvToPostgres(Path csvPath, String objectName, String s3Key,
                                     boolean queryAll, int periodOverride) throws IOException, SQLException {
        return processCsvToPostgres(csvPath, objectName, s3Key, queryAll, periodOverride, null);
    }

    /**
     * Process CSV file and insert records into PostgreSQL database with explicit period and reference fields.
     *
     * @param csvPath Path to the CSV file
     * @param objectName Salesforce object name
     * @param s3Key S3 key where the CSV was uploaded
     * @param queryAll Whether this is a queryAll (deleted records) backup
     * @param periodOverride Explicit period value
     * @param referenceFields Set of field names that are reference/lookup fields (from describe)
     * @return Number of records inserted
     */
    public int processCsvToPostgres(Path csvPath, String objectName, String s3Key,
                                     boolean queryAll, int periodOverride, Set<String> referenceFields) throws IOException, SQLException {
        if (!Files.exists(csvPath)) {
            throw new IOException("CSV file does not exist: " + csvPath);
        }

        int period = periodOverride;
        int totalRecords = 0;
        int deletedRecords = 0;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            // First, insert CSV file reference
            int csvId = insertCsvFile(conn, period, s3Key);
            log.info("Inserted CSV file reference with csv_id={}", csvId);

            try (BufferedReader reader = Files.newBufferedReader(csvPath)) {
                // Read header line
                String headerLine = readCsvLine(reader);
                if (headerLine == null) {
                    log.warn("CSV file is empty (no header): {}", csvPath);
                    return 0;
                }

                String[] headers = parseCsvLine(headerLine);
                log.info("Processing CSV with {} columns for {}", headers.length, objectName);

                // Find important column indices
                int idIndex = findColumnIndex(headers, "Id");
                int nameIndex = findBestNameIndex(headers);
                int isDeletedIndex = findColumnIndex(headers, "IsDeleted");
                int systemModstampIndex = findColumnIndex(headers, "SystemModstamp");

                if (idIndex < 0) {
                    throw new IOException("CSV missing required 'Id' column");
                }

                if (systemModstampIndex < 0) {
                    log.warn("SystemModstamp column not found in CSV for {} - version tracking disabled", objectName);
                }

                // Prepare batch inserts
                List<RecordData> batch = new ArrayList<>(BATCH_SIZE);

                String line;
                while ((line = readCsvLine(reader)) != null) {
                    if (line.trim().isEmpty()) {
                        continue;
                    }

                    String[] values = parseCsvLine(line);
                    if (values.length != headers.length) {
                        log.warn("Skipping malformed CSV line (expected {} columns, got {}): {}",
                                headers.length, values.length, line.length() > 100 ? line.substring(0, 100) + "..." : line);
                        continue;
                    }

                    // Extract record data
                    String recordId = values[idIndex];
                    if (recordId == null || recordId.isEmpty()) {
                        log.warn("Skipping record without Id in {}", objectName);
                        continue;
                    }

                    // Extract name field (fallback to Id if Name not present)
                    String name = nameIndex >= 0 && values[nameIndex] != null && !values[nameIndex].isEmpty()
                            ? values[nameIndex]
                            : recordId;

                    // Extract is_deleted flag
                    boolean isDeleted = isDeletedIndex >= 0 && "true".equalsIgnoreCase(values[isDeletedIndex]);
                    if (isDeleted) {
                        deletedRecords++;
                    }

                    // Extract version from SystemModstamp (Unix timestamp in seconds)
                    int version = 0;
                    if (systemModstampIndex >= 0 && values[systemModstampIndex] != null && !values[systemModstampIndex].isEmpty()) {
                        try {
                            String systemModstamp = values[systemModstampIndex];
                            ZonedDateTime zdt = ZonedDateTime.parse(systemModstamp);
                            version = (int) zdt.toEpochSecond();
                        } catch (Exception e) {
                            log.warn("Failed to parse SystemModstamp for record {} in {}: '{}' - {}",
                                    recordId, objectName, values[systemModstampIndex], e.getMessage());
                        }
                    }

                    // Build metadata JSON - include reference fields and special fields for ContentVersion
                    JSONObject metadata = new JSONObject();
                    for (int i = 0; i < headers.length && i < values.length; i++) {
                        String header = headers[i];
                        String value = values[i];

                        if (value == null || value.isEmpty()) {
                            continue;
                        }

                        // Include fields ending with "Id" (except the record's own Id field)
                        if (header.endsWith("Id") && !header.equals("Id")) {
                            metadata.put(header, value);
                            continue;
                        }

                        // Include reference fields from describe metadata
                        if (referenceFields != null && referenceFields.contains(header)) {
                            metadata.put(header, value);
                            continue;
                        }

                        // Fallback: include custom fields where value looks like a Salesforce ID
                        // (for when referenceFields is not available)
                        if (referenceFields == null && header.endsWith("__c") && looksLikeSalesforceId(value)) {
                            metadata.put(header, value);
                        }

                        // For ContentVersion: also include Checksum, FileType, and FileExtension
                        if ("ContentVersion".equals(objectName)) {
                            if ((header.equals("Checksum") || header.equals("FileType") ||
                                 header.equals("FileExtension") || header.equals("ContentSize") ||
                                 header.equals("Title") || header.equals("PathOnClient")) &&
                                value != null && !value.isEmpty()) {
                                metadata.put(header, value);
                            }
                        }
                    }

                    RecordData recordData = new RecordData(
                            recordId, period, version, metadata, isDeleted, objectName, name, csvId
                    );
                    batch.add(recordData);

                    // Execute batch when full
                    if (batch.size() >= BATCH_SIZE) {
                        int inserted = insertObjectsBatch(conn, batch);
                        totalRecords += inserted;
                        batch.clear();
                    }
                }

                // Insert remaining records
                if (!batch.isEmpty()) {
                    int inserted = insertObjectsBatch(conn, batch);
                    totalRecords += inserted;
                }

                log.info("Completed CSV processing for {}: {} total records ({} deleted)",
                        objectName, totalRecords, deletedRecords);
            }

        } catch (SQLException e) {
            log.error("Database error processing CSV for {}: {}", objectName, e.getMessage(), e);
            throw e;
        }

        return totalRecords;
    }

    /**
     * Insert CSV file reference and return the generated csv_id
     */
    private int insertCsvFile(Connection conn, int period, String s3Key) throws SQLException {
        String sql = "INSERT INTO csvfiles (period, s3path) VALUES (?, ?) RETURNING csv_id";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setInt(1, period);
            pstmt.setString(2, s3Key);

            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                return rs.getInt("csv_id");
            } else {
                throw new SQLException("Failed to insert CSV file reference");
            }
        }
    }

    /**
     * Insert batch of objects into the objects table
     */
    private int insertObjectsBatch(Connection conn, List<RecordData> batch) throws SQLException {
        String sql = "INSERT INTO objects (id, period, version, metadata, org_id, csv_id, is_deleted, object_name, name) " +
                     "VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?) " +
                     "ON CONFLICT (org_id, period, id, version) DO NOTHING";

        int inserted = 0;
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (RecordData record : batch) {
                pstmt.setString(1, record.id);
                pstmt.setInt(2, record.period);
                pstmt.setInt(3, record.version);
                pstmt.setString(4, record.metadata.toString());
                pstmt.setString(5, orgId);
                pstmt.setInt(6, record.csvId);
                pstmt.setBoolean(7, record.isDeleted);
                pstmt.setString(8, record.objectName);
                pstmt.setString(9, record.name);

                pstmt.addBatch();
            }

            int[] results = pstmt.executeBatch();
            for (int result : results) {
                if (result == Statement.SUCCESS_NO_INFO || result > 0) {
                    inserted++;
                }
            }
        }

        return inserted;
    }

    /**
     * Internal class to hold record data before batch insert
     */
    private static class RecordData {
        final String id;
        final int period;
        final int version;
        final JSONObject metadata;
        final boolean isDeleted;
        final String objectName;
        final String name;
        final int csvId;

        RecordData(String id, int period, int version, JSONObject metadata, boolean isDeleted,
                   String objectName, String name, int csvId) {
            this.id = id;
            this.period = period;
            this.version = version;
            this.metadata = metadata;
            this.isDeleted = isDeleted;
            this.objectName = objectName;
            this.name = name;
            this.csvId = csvId;
        }
    }

    /**
     * Read one CSV line, handling newlines inside quoted fields
     */
    private String readCsvLine(BufferedReader reader) throws IOException {
        StringBuilder line = new StringBuilder();
        boolean inQuotes = false;
        int c;

        while ((c = reader.read()) != -1) {
            char ch = (char) c;

            if (ch == '"') {
                inQuotes = !inQuotes;
                line.append(ch);
            } else if (ch == '\n' && !inQuotes) {
                // End of line (not inside quotes)
                break;
            } else if (ch == '\r') {
                // Skip carriage returns
                continue;
            } else {
                line.append(ch);
            }
        }

        if (line.length() == 0 && c == -1) {
            return null;  // EOF
        }

        return line.toString();
    }

    /**
     * Parse a CSV line handling quoted fields with commas
     */
    private String[] parseCsvLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                inQuotes = !inQuotes;
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
     * Find the best column to use as display name.
     * Tries Name first, then common alternatives for objects that don't have a Name field.
     */
    private int findBestNameIndex(String[] headers) {
        for (String candidate : List.of("Name", "Subject", "Title", "CaseNumber", "DeveloperName", "Label")) {
            int idx = findColumnIndex(headers, candidate);
            if (idx >= 0) return idx;
        }
        return -1;
    }

    /**
     * Find the index of a column in the header array (case-insensitive)
     */
    private int findColumnIndex(String[] headers, String columnName) {
        for (int i = 0; i < headers.length; i++) {
            if (headers[i].equalsIgnoreCase(columnName)) {
                return i;
            }
        }
        return -1;
    }

    /**
     * Check if a value looks like a Salesforce ID (15 or 18 alphanumeric characters)
     */
    private boolean looksLikeSalesforceId(String value) {
        if (value == null) return false;
        int len = value.length();
        if (len != 15 && len != 18) return false;
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (!Character.isLetterOrDigit(c)) return false;
        }
        return true;
    }

    /**
     * Create the necessary database tables if they don't exist
     * Uses the schema from "One Backup to Rule Them All"
     */
    @PostConstruct
    public void initializeDatabase() throws SQLException {
        if (Arrays.asList(environment.getActiveProfiles()).contains("web")) {
            log.info("Web profile active - skipping database schema initialization");
            return;
        }

        rdsLifecycleService.ifPresent(RdsLifecycleService::ensureAvailable);

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             Statement stmt = conn.createStatement()) {

            String createTablesSql = """
                -- CSV files tracking
                CREATE TABLE IF NOT EXISTS public.csvfiles (
                    csv_id INTEGER GENERATED ALWAYS AS IDENTITY,
                    period INTEGER NOT NULL,
                    s3path TEXT NOT NULL,
                    CONSTRAINT csvfiles_pk PRIMARY KEY (csv_id)
                );
                CREATE INDEX IF NOT EXISTS csvfiles_period_index ON csvfiles (period);

                -- Objects metadata storage
                CREATE TABLE IF NOT EXISTS public.objects (
                    id TEXT NOT NULL,
                    period INTEGER NOT NULL,
                    version INTEGER NOT NULL,
                    metadata JSONB,
                    org_id TEXT NOT NULL,
                    csv_id INTEGER,
                    is_deleted BOOLEAN DEFAULT FALSE,
                    object_name TEXT,
                    name TEXT NOT NULL,
                    CONSTRAINT objects_pk PRIMARY KEY (org_id, period, id, version)
                );
                CREATE INDEX IF NOT EXISTS objects_org_id_id_index ON public.objects (org_id, id);
                CREATE INDEX IF NOT EXISTS objects_org_id_object_name_is_deleted_index ON public.objects (org_id, object_name, is_deleted);
                CREATE INDEX IF NOT EXISTS objects_org_id_period_index ON objects (org_id, period);

                -- Backup runs tracking (replaces Salesforce Backup_Run__c for checkpoint management)
                CREATE TABLE IF NOT EXISTS public.backup_runs (
                    run_id BIGSERIAL PRIMARY KEY,
                    object_name TEXT NOT NULL,
                    org_id TEXT NOT NULL,
                    period INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    query_all BOOLEAN DEFAULT FALSE,
                    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    completed_at TIMESTAMP,
                    last_checkpoint_id TEXT,
                    last_checkpoint_modstamp TIMESTAMP,
                    records_processed INTEGER DEFAULT 0,
                    records_failed INTEGER DEFAULT 0,
                    bytes_transferred BIGINT DEFAULT 0,
                    error_message TEXT
                );
                CREATE INDEX IF NOT EXISTS backup_runs_org_object_index ON backup_runs (org_id, object_name, period);
                CREATE INDEX IF NOT EXISTS backup_runs_status_index ON backup_runs (status);

                -- Archive expression column (added for archive feature)
                ALTER TABLE backup_runs ADD COLUMN IF NOT EXISTS archive_expression TEXT;

                -- Functional indexes for ContentDocument delete safety checks
                CREATE INDEX IF NOT EXISTS objects_cdl_content_doc_id
                    ON objects ((metadata->>'ContentDocumentId'))
                    WHERE object_name = 'ContentDocumentLink';
                CREATE INDEX IF NOT EXISTS objects_cv_content_doc_id
                    ON objects ((metadata->>'ContentDocumentId'))
                    WHERE object_name = 'ContentVersion';

                -- ContentDocumentLink by LinkedEntityId (find files for a record)
                CREATE INDEX IF NOT EXISTS objects_cdl_linked_entity_id
                    ON objects ((metadata->>'LinkedEntityId'))
                    WHERE object_name = 'ContentDocumentLink';

                -- Covering index for per-object stats refresh
                CREATE INDEX IF NOT EXISTS objects_org_object_id_deleted
                    ON objects (org_id, object_name) INCLUDE (id, is_deleted);

                -- Pre-computed object statistics for fast dashboard loading
                CREATE TABLE IF NOT EXISTS object_stats (
                    org_id TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    unique_records INTEGER DEFAULT 0,
                    total_versions INTEGER DEFAULT 0,
                    deleted_count INTEGER DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT NOW(),
                    CONSTRAINT object_stats_pk PRIMARY KEY (org_id, object_name)
                );
                """;

            stmt.execute(createTablesSql);
            log.info("Database tables initialized successfully (csvfiles, objects, backup_runs)");

            // pg_trgm extension and index (separate - extension creation may require elevated privileges)
            try {
                stmt.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm");
                stmt.execute("""
                    CREATE INDEX IF NOT EXISTS objects_metadata_trgm_idx
                        ON objects USING GIN ((metadata::text) gin_trgm_ops)
                        WHERE metadata IS NOT NULL
                    """);
                log.info("pg_trgm extension and metadata trigram index created successfully");
            } catch (SQLException e) {
                log.warn("Could not create pg_trgm extension/index (may require superuser): {}", e.getMessage());
            }
        }
    }

    /**
     * Load all unique ContentVersion checksums from database for cache preloading.
     * Returns set of "checksum.extension" keys for fast deduplication.
     *
     * @return Set of checksum keys (e.g., "abc123def456.pdf")
     */
    public Set<String> loadContentVersionChecksums() {
        Set<String> checksums = new HashSet<>();

        String sql = """
            SELECT DISTINCT
                metadata->>'Checksum' AS checksum,
                metadata->>'FileExtension' AS extension
            FROM objects
            WHERE org_id = ?
            AND object_name = 'ContentVersion'
            AND metadata->>'Checksum' IS NOT NULL
            AND metadata->>'FileExtension' IS NOT NULL
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, orgId);

            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    String checksum = rs.getString("checksum");
                    String extension = rs.getString("extension");

                    if (checksum != null && !checksum.isEmpty() &&
                        extension != null && !extension.isEmpty()) {
                        checksums.add(checksum + "." + extension);
                    }
                }
            }

            log.info("Loaded {} unique ContentVersion checksums from database", checksums.size());

        } catch (SQLException e) {
            log.error("Failed to load ContentVersion checksums from database: {}", e.getMessage());
            // Return empty set - cache will be populated progressively
        }

        return checksums;
    }

    /**
     * Backup run data class for tracking backup progress
     */
    public static class BackupRun {
        public long runId;
        public String objectName;
        public String orgId;
        public int period;
        public String status;
        public boolean queryAll;
        public Timestamp startedAt;
        public Timestamp completedAt;
        public String lastCheckpointId;
        public Timestamp lastCheckpointModstamp;
        public int recordsProcessed;
        public int recordsFailed;
        public long bytesTransferred;
        public String errorMessage;
        public String archiveExpression;

        public BackupRun() {}

        public BackupRun(String objectName, String orgId, int period, boolean queryAll) {
            this.objectName = objectName;
            this.orgId = orgId;
            this.period = period;
            this.queryAll = queryAll;
            this.status = "RUNNING";
        }
    }

    /**
     * Create a new backup run in the database
     * Returns the generated run_id
     */
    public long createBackupRun(String objectName, boolean queryAll) throws SQLException {
        int period = getCurrentPeriod();
        String sql = """
            INSERT INTO backup_runs (object_name, org_id, period, status, query_all, started_at)
            VALUES (?, ?, ?, 'RUNNING', ?, NOW())
            RETURNING run_id
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, objectName);
            stmt.setString(2, orgId);
            stmt.setInt(3, period);
            stmt.setBoolean(4, queryAll);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long runId = rs.getLong("run_id");
                log.info("Created backup run: runId={}, object={}, period={}, queryAll={}",
                    runId, objectName, period, queryAll);
                return runId;
            } else {
                throw new SQLException("Failed to create backup run - no ID returned");
            }
        }
    }

    /**
     * Update checkpoint for a running backup
     * This is called frequently (every 10-20 files) so it must be fast
     */
    public void updateBackupRunCheckpoint(long runId, String lastCheckpointId,
                                          Timestamp lastCheckpointModstamp,
                                          int recordsProcessed) throws SQLException {
        String sql = """
            UPDATE backup_runs
            SET last_checkpoint_id = ?,
                last_checkpoint_modstamp = ?,
                records_processed = ?
            WHERE run_id = ?
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, lastCheckpointId);
            stmt.setTimestamp(2, lastCheckpointModstamp);
            stmt.setInt(3, recordsProcessed);
            stmt.setLong(4, runId);

            int updated = stmt.executeUpdate();
            if (updated == 0) {
                log.warn("Failed to update checkpoint for run_id={}", runId);
            } else {
                log.debug("Checkpoint updated: runId={}, lastId={}, processed={}",
                    runId, lastCheckpointId, recordsProcessed);
            }
        }
    }

    /**
     * Complete a backup run with final statistics
     */
    public void completeBackupRun(long runId, String status, int recordsProcessed,
                                   int recordsFailed, long bytesTransferred,
                                   String errorMessage) throws SQLException {
        String sql = """
            UPDATE backup_runs
            SET status = ?,
                completed_at = NOW(),
                records_processed = ?,
                records_failed = ?,
                bytes_transferred = ?,
                error_message = ?
            WHERE run_id = ?
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, status);
            stmt.setInt(2, recordsProcessed);
            stmt.setInt(3, recordsFailed);
            stmt.setLong(4, bytesTransferred);
            stmt.setString(5, errorMessage);
            stmt.setLong(6, runId);

            int updated = stmt.executeUpdate();
            if (updated > 0) {
                log.info("Backup run completed: runId={}, status={}, processed={}, failed={}",
                    runId, status, recordsProcessed, recordsFailed);
            } else {
                log.warn("Failed to complete backup run: runId={}", runId);
            }
        }
    }

    /**
     * Refresh pre-computed statistics for a single object in the object_stats table.
     * Called after each object's backup completes.
     */
    public void refreshObjectStats(String objectName) {
        String sql = """
            INSERT INTO object_stats (org_id, object_name, unique_records, total_versions, deleted_count, updated_at)
            SELECT ?, ?, COUNT(DISTINCT id), COUNT(*), SUM(CASE WHEN is_deleted THEN 1 ELSE 0 END), NOW()
            FROM objects WHERE org_id = ? AND object_name = ?
            ON CONFLICT (org_id, object_name) DO UPDATE SET
                unique_records = EXCLUDED.unique_records,
                total_versions = EXCLUDED.total_versions,
                deleted_count = EXCLUDED.deleted_count,
                updated_at = EXCLUDED.updated_at
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, orgId);
            stmt.setString(2, objectName);
            stmt.setString(3, orgId);
            stmt.setString(4, objectName);

            stmt.executeUpdate();
            log.info("Refreshed object_stats for {}", objectName);

        } catch (SQLException e) {
            log.warn("Failed to refresh object_stats for {}: {}", objectName, e.getMessage());
        }
    }

    /**
     * Get the current archive period as negative YYMM (e.g. -2602 for February 2026).
     */
    public int getArchivePeriod() {
        return -getCurrentPeriod();
    }

    /**
     * Create a new archive run in the database with negative period and archive expression.
     */
    public long createArchiveRun(String objectName, String archiveExpression) throws SQLException {
        int period = getArchivePeriod();
        String sql = """
            INSERT INTO backup_runs (object_name, org_id, period, status, query_all, started_at, archive_expression)
            VALUES (?, ?, ?, 'RUNNING', false, NOW(), ?)
            RETURNING run_id
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, objectName);
            stmt.setString(2, orgId);
            stmt.setInt(3, period);
            stmt.setString(4, archiveExpression);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                long runId = rs.getLong("run_id");
                log.info("Created archive run: runId={}, object={}, period={}, expression={}",
                    runId, objectName, period, archiveExpression);
                return runId;
            } else {
                throw new SQLException("Failed to create archive run - no ID returned");
            }
        }
    }

    /**
     * Get the latest archive checkpoint for an object.
     * Looks across ALL archive periods (period < 0) so the checkpoint is never reset by month changes.
     */
    public BackupRun getLatestArchiveRun(String objectName) throws SQLException {
        String sql = """
            SELECT run_id, object_name, org_id, period, status, query_all,
                   started_at, completed_at, last_checkpoint_id, last_checkpoint_modstamp,
                   records_processed, records_failed, bytes_transferred, error_message,
                   archive_expression
            FROM backup_runs
            WHERE org_id = ? AND object_name = ? AND period < 0 AND query_all = false
              AND last_checkpoint_id IS NOT NULL
              AND last_checkpoint_modstamp IS NOT NULL
            ORDER BY last_checkpoint_modstamp DESC, started_at DESC
            LIMIT 1
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, orgId);
            stmt.setString(2, objectName);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                BackupRun run = new BackupRun();
                run.runId = rs.getLong("run_id");
                run.objectName = rs.getString("object_name");
                run.orgId = rs.getString("org_id");
                run.period = rs.getInt("period");
                run.status = rs.getString("status");
                run.queryAll = rs.getBoolean("query_all");
                run.startedAt = rs.getTimestamp("started_at");
                run.completedAt = rs.getTimestamp("completed_at");
                run.lastCheckpointId = rs.getString("last_checkpoint_id");
                run.lastCheckpointModstamp = rs.getTimestamp("last_checkpoint_modstamp");
                run.recordsProcessed = rs.getInt("records_processed");
                run.recordsFailed = rs.getInt("records_failed");
                run.bytesTransferred = rs.getLong("bytes_transferred");
                run.errorMessage = rs.getString("error_message");
                run.archiveExpression = rs.getString("archive_expression");

                log.info("Found existing archive run to resume: runId={}, period={}, lastCheckpointId={}, expression={}",
                    run.runId, run.period, run.lastCheckpointId, run.archiveExpression);
                return run;
            }

            return null;
        }
    }

    /**
     * Get the latest checkpoint for an object to resume from.
     * Only looks within the CURRENT period - a new period means a fresh full backup.
     * Within a period, looks at ALL runs (SUCCESS, FAILED, etc.) to find the last saved checkpoint,
     * allowing resuming after crashes or restarts.
     */
    public BackupRun getLatestBackupRun(String objectName, boolean queryAll) throws SQLException {
        int period = getCurrentPeriod();
        String sql = """
            SELECT run_id, object_name, org_id, period, status, query_all,
                   started_at, completed_at, last_checkpoint_id, last_checkpoint_modstamp,
                   records_processed, records_failed, bytes_transferred, error_message
            FROM backup_runs
            WHERE org_id = ? AND object_name = ? AND query_all = ? AND period = ?
              AND last_checkpoint_id IS NOT NULL
              AND last_checkpoint_modstamp IS NOT NULL
            ORDER BY last_checkpoint_modstamp DESC, started_at DESC
            LIMIT 1
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, orgId);
            stmt.setString(2, objectName);
            stmt.setBoolean(3, queryAll);
            stmt.setInt(4, period);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                BackupRun run = new BackupRun();
                run.runId = rs.getLong("run_id");
                run.objectName = rs.getString("object_name");
                run.orgId = rs.getString("org_id");
                run.period = rs.getInt("period");
                run.status = rs.getString("status");
                run.queryAll = rs.getBoolean("query_all");
                run.startedAt = rs.getTimestamp("started_at");
                run.completedAt = rs.getTimestamp("completed_at");
                run.lastCheckpointId = rs.getString("last_checkpoint_id");
                run.lastCheckpointModstamp = rs.getTimestamp("last_checkpoint_modstamp");
                run.recordsProcessed = rs.getInt("records_processed");
                run.recordsFailed = rs.getInt("records_failed");
                run.bytesTransferred = rs.getLong("bytes_transferred");
                run.errorMessage = rs.getString("error_message");

                log.info("Found existing backup run to resume: runId={}, period={}, lastCheckpointId={}",
                    run.runId, run.period, run.lastCheckpointId);
                return run;
            }

            return null;
        }
    }

    /**
     * Get backup run summary for an object (for reporting to Salesforce)
     */
    public String getBackupRunSummary(String objectName, boolean queryAll) throws SQLException {
        int period = getCurrentPeriod();
        String sql = """
            SELECT status, records_processed, records_failed, bytes_transferred,
                   EXTRACT(EPOCH FROM (completed_at - started_at)) as duration_seconds
            FROM backup_runs
            WHERE org_id = ? AND object_name = ? AND period = ? AND query_all = ?
            ORDER BY started_at DESC
            LIMIT 1
            """;

        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password);
             PreparedStatement stmt = conn.prepareStatement(sql)) {

            stmt.setString(1, orgId);
            stmt.setString(2, objectName);
            stmt.setInt(3, period);
            stmt.setBoolean(4, queryAll);

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                return String.format(
                    "{\"status\":\"%s\",\"processed\":%d,\"failed\":%d,\"bytes\":%d,\"duration_sec\":%.1f}",
                    rs.getString("status"),
                    rs.getInt("records_processed"),
                    rs.getInt("records_failed"),
                    rs.getLong("bytes_transferred"),
                    rs.getDouble("duration_seconds")
                );
            }

            return null;
        }
    }
}
