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
package se.devrandom.heimdall.web;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import se.devrandom.heimdall.storage.RdsLifecycleService;
import se.devrandom.heimdall.storage.S3Service;

import java.sql.*;
import java.time.Month;
import java.time.format.TextStyle;
import java.util.*;

@Service
public class RestoreService {

    private static final Logger log = LoggerFactory.getLogger(RestoreService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private S3Service s3Service;

    @Autowired
    private Optional<RdsLifecycleService> rdsLifecycleService;

    @Value("${postgres.backup.url}")
    private String dbUrl;

    @Value("${postgres.backup.username}")
    private String dbUsername;

    @Value("${postgres.backup.password}")
    private String dbPassword;

    @Value("${salesforce.org-id}")
    private String orgId;

    private HikariDataSource dataSource;

    @PostConstruct
    private void initPool() {
        rdsLifecycleService.ifPresent(RdsLifecycleService::ensureAvailable);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(dbUrl);
        config.setUsername(dbUsername);
        config.setPassword(dbPassword);
        config.setMaximumPoolSize(5);
        config.setMinimumIdle(1);
        config.setConnectionTimeout(10000);
        dataSource = new HikariDataSource(config);
        log.info("Connection pool initialized (max=5)");

        // Create restore_log table if it doesn't exist
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("""
                CREATE TABLE IF NOT EXISTS restore_log (
                    id SERIAL PRIMARY KEY,
                    original_id VARCHAR(18) NOT NULL,
                    new_id VARCHAR(18) NOT NULL,
                    object_name VARCHAR(255) NOT NULL,
                    period INTEGER NOT NULL,
                    sandbox_name VARCHAR(255) NOT NULL,
                    restored_at TIMESTAMP DEFAULT NOW()
                )
                """);
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_restore_log_original_id ON restore_log(original_id)");
            stmt.execute("ALTER TABLE restore_log ADD COLUMN IF NOT EXISTS restored_fields TEXT");
            log.info("restore_log table ready");
        } catch (SQLException e) {
            log.error("Failed to create restore_log table: {}", e.getMessage());
        }
    }

    @PreDestroy
    private void closePool() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    private Connection getConnection() throws SQLException {
        long start = System.currentTimeMillis();
        Connection conn = dataSource.getConnection();
        log.debug("getConnection took {}ms", System.currentTimeMillis() - start);
        return conn;
    }

    private ResultSet executeQuery(PreparedStatement stmt, String label) throws SQLException {
        long start = System.currentTimeMillis();
        ResultSet rs = stmt.executeQuery();
        log.debug("{} query took {}ms", label, System.currentTimeMillis() - start);
        return rs;
    }

    /**
     * Get all versions of a record by ID
     * Fetches full record data from S3 CSV files
     */
    public List<RecordVersion> getRecordVersions(String recordId) {
        List<RecordVersion> versions = new ArrayList<>();

        // Join with csvfiles to get the S3 path
        String sql = """
            SELECT o.id, o.period, o.version, o.metadata, o.org_id, o.is_deleted,
                   o.object_name, o.name, o.csv_id, c.s3path
            FROM objects o
            LEFT JOIN csvfiles c ON o.csv_id = c.csv_id
            WHERE o.org_id = ? AND o.id = ?
            ORDER BY o.period DESC, o.version DESC
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);
            stmt.setString(2, recordId);

            try (ResultSet rs = executeQuery(stmt, "getRecordVersions")) {
                while (rs.next()) {
                    RecordVersion v = new RecordVersion();
                    v.setId(rs.getString("id"));
                    v.setPeriod(rs.getInt("period"));
                    v.setVersion(rs.getInt("version"));
                    v.setDeleted(rs.getBoolean("is_deleted"));
                    v.setObjectName(rs.getString("object_name"));
                    v.setName(rs.getString("name"));

                    String s3path = rs.getString("s3path");
                    v.setS3path(s3path);

                    // Try to get full record from S3 first
                    Map<String, Object> metadata = null;
                    if (s3path != null && s3Service != null) {
                        try {
                            String s3Key = s3path.replace("s3://" + s3Service.getBucketName() + "/", "");
                            String jsonRecord = s3Service.selectRecordFromCsv(s3Key, recordId);
                            if (jsonRecord != null) {
                                metadata = objectMapper.readValue(
                                    jsonRecord, new TypeReference<Map<String, Object>>() {});
                                log.debug("Loaded full record from S3 for {} (period {})", recordId, v.getPeriod());
                            }
                        } catch (Exception e) {
                            log.warn("Failed to fetch from S3, falling back to metadata: {}", e.getMessage());
                        }
                    }

                    // Fall back to metadata from database if S3 fetch failed
                    if (metadata == null) {
                        String metadataJson = rs.getString("metadata");
                        if (metadataJson != null) {
                            try {
                                metadata = objectMapper.readValue(
                                    metadataJson, new TypeReference<Map<String, Object>>() {});
                            } catch (Exception e) {
                                log.warn("Failed to parse metadata JSON: {}", e.getMessage());
                                metadata = Map.of("_error", "Failed to parse metadata");
                            }
                        }
                    }

                    v.setMetadata(metadata != null ? metadata : new LinkedHashMap<>());
                    versions.add(v);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get record versions: {}", e.getMessage());
        }

        return versions;
    }

    /**
     * Get list of objects that have deleted records
     */
    public List<String> getObjectsWithDeletedRecords() {
        List<String> objects = new ArrayList<>();
        String sql = """
            SELECT DISTINCT object_name
            FROM objects
            WHERE org_id = ? AND is_deleted = true
            ORDER BY object_name
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);

            try (ResultSet rs = executeQuery(stmt, "getObjectsWithDeletedRecords")) {
                while (rs.next()) {
                    objects.add(rs.getString("object_name"));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get objects with deleted records: {}", e.getMessage());
        }

        return objects;
    }

    /**
     * Get deleted records for a specific object type
     */
    public List<DeletedRecord> getDeletedRecords(String objectName, int page, int pageSize) {
        List<DeletedRecord> records = new ArrayList<>();
        String sql = """
            SELECT DISTINCT ON (id) id, period, name, object_name
            FROM objects
            WHERE org_id = ? AND object_name = ? AND is_deleted = true
            ORDER BY id, period DESC
            LIMIT ? OFFSET ?
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);
            stmt.setString(2, objectName);
            stmt.setInt(3, pageSize);
            stmt.setInt(4, page * pageSize);

            try (ResultSet rs = executeQuery(stmt, "getDeletedRecords")) {
                while (rs.next()) {
                    DeletedRecord r = new DeletedRecord();
                    r.setId(rs.getString("id"));
                    r.setPeriod(rs.getInt("period"));
                    r.setName(rs.getString("name"));
                    r.setObjectName(rs.getString("object_name"));
                    records.add(r);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get deleted records: {}", e.getMessage());
        }

        return records;
    }

    /**
     * Count deleted records for an object type
     */
    public int countDeletedRecords(String objectName) {
        String sql = """
            SELECT COUNT(DISTINCT id)
            FROM objects
            WHERE org_id = ? AND object_name = ? AND is_deleted = true
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);
            stmt.setString(2, objectName);

            try (ResultSet rs = executeQuery(stmt, "countDeletedRecords")) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to count deleted records: {}", e.getMessage());
        }

        return 0;
    }

    /**
     * Get list of objects that have archived records (uses pre-computed stats)
     */
    public List<String> getObjectsWithArchivedRecords() {
        List<String> objects = new ArrayList<>();
        String sql = """
            SELECT object_name
            FROM object_stats
            WHERE org_id = ? AND archived_count > 0
            ORDER BY object_name
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);

            try (ResultSet rs = executeQuery(stmt, "getObjectsWithArchivedRecords")) {
                while (rs.next()) {
                    objects.add(rs.getString("object_name"));
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get objects with archived records: {}", e.getMessage());
        }

        return objects;
    }

    /**
     * Get archived records for a specific object type
     */
    public List<DeletedRecord> getArchivedRecords(String objectName, int page, int pageSize) {
        List<DeletedRecord> records = new ArrayList<>();
        String sql = """
            SELECT DISTINCT ON (id) id, period, name, object_name
            FROM objects
            WHERE org_id = ? AND object_name = ? AND period < 0
            ORDER BY id, period DESC
            LIMIT ? OFFSET ?
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);
            stmt.setString(2, objectName);
            stmt.setInt(3, pageSize);
            stmt.setInt(4, page * pageSize);

            try (ResultSet rs = executeQuery(stmt, "getArchivedRecords")) {
                while (rs.next()) {
                    DeletedRecord r = new DeletedRecord();
                    r.setId(rs.getString("id"));
                    r.setPeriod(rs.getInt("period"));
                    r.setName(rs.getString("name"));
                    r.setObjectName(rs.getString("object_name"));
                    records.add(r);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get archived records: {}", e.getMessage());
        }

        return records;
    }

    /**
     * Count archived records for an object type (uses pre-computed stats)
     */
    public int countArchivedRecords(String objectName) {
        String sql = """
            SELECT archived_count
            FROM object_stats
            WHERE org_id = ? AND object_name = ?
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);
            stmt.setString(2, objectName);

            try (ResultSet rs = executeQuery(stmt, "countArchivedRecords")) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to count archived records: {}", e.getMessage());
        }

        return 0;
    }

    /**
     * Count all archived records across all objects
     */
    public int countAllArchivedRecords() {
        String sql = """
            SELECT COUNT(DISTINCT id)
            FROM objects
            WHERE org_id = ? AND period < 0
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);

            try (ResultSet rs = executeQuery(stmt, "countAllArchivedRecords")) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to count all archived records: {}", e.getMessage());
        }

        return 0;
    }

    /**
     * Get object statistics
     */
    public Map<String, ObjectStats> getObjectStatistics() {
        Map<String, ObjectStats> stats = new LinkedHashMap<>();
        String sql = """
            SELECT object_name, unique_records, total_versions, deleted_count, archived_count
            FROM object_stats
            WHERE org_id = ?
            ORDER BY unique_records DESC
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);

            try (ResultSet rs = executeQuery(stmt, "getObjectStatistics")) {
                while (rs.next()) {
                    ObjectStats s = new ObjectStats();
                    s.setObjectName(rs.getString("object_name"));
                    s.setUniqueRecords(rs.getInt("unique_records"));
                    s.setTotalVersions(rs.getInt("total_versions"));
                    s.setDeletedCount(rs.getInt("deleted_count"));
                    s.setArchivedCount(rs.getInt("archived_count"));
                    stats.put(s.getObjectName(), s);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get object statistics: {}", e.getMessage());
        }

        return stats;
    }

    /**
     * Calculate diff between two versions
     */
    public List<FieldChange> calculateDiff(RecordVersion newVersion, RecordVersion oldVersion) {
        List<FieldChange> changes = new ArrayList<>();

        if (newVersion == null || oldVersion == null ||
            newVersion.getMetadata() == null || oldVersion.getMetadata() == null) {
            return changes;
        }

        Map<String, Object> newMeta = newVersion.getMetadata();
        Map<String, Object> oldMeta = oldVersion.getMetadata();

        // Find all unique keys
        Set<String> allKeys = new TreeSet<>();
        allKeys.addAll(newMeta.keySet());
        allKeys.addAll(oldMeta.keySet());

        for (String key : allKeys) {
            Object newVal = newMeta.get(key);
            Object oldVal = oldMeta.get(key);

            String newStr = newVal != null ? String.valueOf(newVal) : "";
            String oldStr = oldVal != null ? String.valueOf(oldVal) : "";

            if (!newStr.equals(oldStr)) {
                FieldChange change = new FieldChange();
                change.setField(key);
                change.setOldValue(oldStr);
                change.setNewValue(newStr);

                if (oldVal == null) {
                    change.setType("diff-added");
                } else if (newVal == null) {
                    change.setType("diff-removed");
                } else {
                    change.setType("diff-changed");
                }

                changes.add(change);
            }
        }

        return changes;
    }

    /**
     * Classify the status of a record from its version history.
     * A record can have multiple statuses simultaneously.
     */
    public RecordStatus classifyRecordStatus(List<RecordVersion> versions) {
        boolean active = false;
        boolean deleted = false;
        boolean archived = false;

        if (versions != null && !versions.isEmpty()) {
            // Check for archive periods (negative period)
            for (RecordVersion v : versions) {
                if (v.getPeriod() < 0) {
                    archived = true;
                    break;
                }
            }

            // Latest version (first in list, ordered by period DESC, version DESC)
            RecordVersion latest = versions.get(0);
            if (latest.isDeleted()) {
                deleted = true;
            }
            if (latest.getPeriod() > 0 && !latest.isDeleted()) {
                active = true;
            }
        }

        return new RecordStatus(active, deleted, archived);
    }

    /**
     * Format a period integer into human-readable form.
     * 2601 -> "Jan 2026", -2602 -> "Archive Feb 2026"
     */
    public static String formatPeriod(int period) {
        boolean isArchive = period < 0;
        int absPeriod = Math.abs(period);
        int year = 2000 + absPeriod / 100;
        int month = absPeriod % 100;

        if (month < 1 || month > 12) {
            return String.valueOf(period);
        }

        String monthName = Month.of(month).getDisplayName(TextStyle.SHORT, Locale.ENGLISH);
        String label = monthName + " " + year;
        return isArchive ? "Archive " + label : label;
    }

    /**
     * Get related files for a record via ContentDocumentLink -> ContentVersion.
     * Split into two queries to avoid self-join on the large objects table.
     */
    public List<RelatedFile> getRelatedFiles(String recordId) {
        List<RelatedFile> files = new ArrayList<>();

        // Step 1: Find ContentDocumentIds from ContentDocumentLink (uses objects_cdl_linked_entity_id index)
        String cdlSql = """
            SELECT DISTINCT metadata->>'ContentDocumentId' AS content_doc_id
            FROM objects
            WHERE org_id = ?
              AND object_name = 'ContentDocumentLink'
              AND metadata->>'LinkedEntityId' = ?
            """;

        List<String> contentDocIds = new ArrayList<>();
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(cdlSql)) {
            stmt.setString(1, orgId);
            stmt.setString(2, recordId);

            try (ResultSet rs = executeQuery(stmt, "getRelatedFiles-CDL")) {
                while (rs.next()) {
                    String docId = rs.getString("content_doc_id");
                    if (docId != null) {
                        contentDocIds.add(docId);
                    }
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get ContentDocumentLinks for record {}: {}", recordId, e.getMessage());
            return files;
        }

        if (contentDocIds.isEmpty()) {
            return files;
        }

        // Step 2: Get latest ContentVersion for each ContentDocumentId (uses objects_cv_content_doc_id index)
        String placeholders = String.join(",", Collections.nCopies(contentDocIds.size(), "?"));
        String cvSql = String.format("""
            SELECT DISTINCT ON (metadata->>'ContentDocumentId')
                   id AS cv_id,
                   metadata->>'ContentDocumentId' AS content_doc_id,
                   name AS cv_name,
                   metadata->>'Title' AS title,
                   metadata->>'PathOnClient' AS path_on_client,
                   metadata->>'FileType' AS file_type,
                   metadata->>'FileExtension' AS file_extension,
                   metadata->>'ContentSize' AS content_size,
                   metadata->>'Checksum' AS checksum
            FROM objects
            WHERE org_id = ?
              AND object_name = 'ContentVersion'
              AND metadata->>'ContentDocumentId' IN (%s)
            ORDER BY metadata->>'ContentDocumentId', period DESC, version DESC
            """, placeholders);

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(cvSql)) {
            stmt.setString(1, orgId);
            for (int i = 0; i < contentDocIds.size(); i++) {
                stmt.setString(i + 2, contentDocIds.get(i));
            }

            try (ResultSet rs = executeQuery(stmt, "getRelatedFiles-CV")) {
                while (rs.next()) {
                    RelatedFile f = new RelatedFile();
                    f.setContentVersionId(rs.getString("cv_id"));
                    f.setContentDocumentId(rs.getString("content_doc_id"));
                    // Prefer Title from metadata, fall back to name column
                    String title = rs.getString("title");
                    String pathOnClient = rs.getString("path_on_client");
                    if (title != null && !title.isEmpty()) {
                        f.setName(title);
                    } else if (pathOnClient != null && !pathOnClient.isEmpty()) {
                        f.setName(pathOnClient);
                    } else {
                        f.setName(rs.getString("cv_name"));
                    }
                    f.setFileType(rs.getString("file_type"));
                    f.setFileExtension(rs.getString("file_extension"));
                    f.setContentSize(rs.getString("content_size"));
                    f.setChecksum(rs.getString("checksum"));
                    files.add(f);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get ContentVersions for record {}: {}", recordId, e.getMessage());
        }

        return files;
    }

    /**
     * Get related child records that reference this record ID in their metadata
     */
    public List<RelatedRecord> getRelatedChildRecords(String recordId) {
        List<RelatedRecord> records = new ArrayList<>();

        String sql = """
            SELECT DISTINCT ON (o.id)
                   o.id, o.name, o.object_name, o.period, o.is_deleted,
                   o.metadata::text AS meta_text
            FROM objects o
            WHERE o.org_id = ?
              AND o.metadata IS NOT NULL
              AND o.object_name NOT IN ('ContentVersion', 'ContentDocumentLink', 'ContentDocument')
              AND o.metadata::text LIKE ?
              AND o.id != ?
            ORDER BY o.id, o.period DESC, o.version DESC
            LIMIT 200
            """;

        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, orgId);
            stmt.setString(2, "%" + recordId + "%");
            stmt.setString(3, recordId);

            try (ResultSet rs = executeQuery(stmt, "getRelatedChildRecords")) {
                while (rs.next()) {
                    String metaText = rs.getString("meta_text");
                    String referencingField = extractReferencingField(metaText, recordId);
                    if (referencingField == null) {
                        continue; // false positive from LIKE match
                    }

                    RelatedRecord r = new RelatedRecord();
                    r.setId(rs.getString("id"));
                    r.setName(rs.getString("name"));
                    r.setObjectName(rs.getString("object_name"));
                    int period = rs.getInt("period");
                    boolean isDeleted = rs.getBoolean("is_deleted");
                    r.setActive(period > 0 && !isDeleted);
                    r.setDeleted(isDeleted);
                    r.setArchived(period < 0);
                    r.setReferencingField(referencingField);
                    records.add(r);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get related child records for {}: {}", recordId, e.getMessage());
        }

        return records;
    }

    /**
     * Extract which metadata key holds the given record ID
     */
    private String extractReferencingField(String metadataJson, String recordId) {
        if (metadataJson == null) return null;
        try {
            Map<String, Object> meta = objectMapper.readValue(
                metadataJson, new com.fasterxml.jackson.core.type.TypeReference<Map<String, Object>>() {});
            for (Map.Entry<String, Object> entry : meta.entrySet()) {
                if (entry.getValue() != null && recordId.equals(String.valueOf(entry.getValue()))) {
                    return entry.getKey();
                }
            }
        } catch (Exception e) {
            log.debug("Failed to parse metadata for referencing field extraction: {}", e.getMessage());
        }
        return null;
    }

    /**
     * Log a restore operation
     */
    public void logRestore(String originalId, String newId, String objectName, int period, String sandboxName, List<String> restoredFields) {
        String sql = "INSERT INTO restore_log (original_id, new_id, object_name, period, sandbox_name, restored_fields) VALUES (?, ?, ?, ?, ?, ?)";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, originalId);
            stmt.setString(2, newId);
            stmt.setString(3, objectName);
            stmt.setInt(4, period);
            stmt.setString(5, sandboxName);
            stmt.setString(6, restoredFields != null ? String.join(", ", restoredFields) : null);
            stmt.executeUpdate();
            log.info("Logged restore: {} -> {} ({}) in sandbox '{}'", originalId, newId, objectName, sandboxName);
        } catch (SQLException e) {
            log.error("Failed to log restore: {}", e.getMessage());
        }
    }

    /**
     * Get restore history for a record
     */
    public List<RestoreLogEntry> getRestoreHistory(String recordId) {
        List<RestoreLogEntry> entries = new ArrayList<>();
        String sql = "SELECT new_id, object_name, period, sandbox_name, restored_at, restored_fields FROM restore_log WHERE original_id = ? ORDER BY restored_at DESC";
        try (Connection conn = getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.setString(1, recordId);
            try (ResultSet rs = executeQuery(stmt, "getRestoreHistory")) {
                while (rs.next()) {
                    RestoreLogEntry e = new RestoreLogEntry();
                    e.setNewId(rs.getString("new_id"));
                    e.setObjectName(rs.getString("object_name"));
                    e.setPeriod(rs.getInt("period"));
                    e.setSandboxName(rs.getString("sandbox_name"));
                    e.setRestoredAt(rs.getTimestamp("restored_at"));
                    e.setRestoredFields(rs.getString("restored_fields"));
                    entries.add(e);
                }
            }
        } catch (SQLException e) {
            log.error("Failed to get restore history: {}", e.getMessage());
        }
        return entries;
    }

    // DTO classes
    public static class RecordStatus {
        private final boolean active;
        private final boolean deleted;
        private final boolean archived;

        public RecordStatus(boolean active, boolean deleted, boolean archived) {
            this.active = active;
            this.deleted = deleted;
            this.archived = archived;
        }

        public boolean isActive() { return active; }
        public boolean isDeleted() { return deleted; }
        public boolean isArchived() { return archived; }
    }

    public static class RelatedFile {
        private String contentVersionId;
        private String contentDocumentId;
        private String name;
        private String fileType;
        private String fileExtension;
        private String contentSize;
        private String checksum;

        public String getContentVersionId() { return contentVersionId; }
        public void setContentVersionId(String contentVersionId) { this.contentVersionId = contentVersionId; }
        public String getContentDocumentId() { return contentDocumentId; }
        public void setContentDocumentId(String contentDocumentId) { this.contentDocumentId = contentDocumentId; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getFileType() { return fileType; }
        public void setFileType(String fileType) { this.fileType = fileType; }
        public String getFileExtension() { return fileExtension; }
        public void setFileExtension(String fileExtension) { this.fileExtension = fileExtension; }
        public String getContentSize() { return contentSize; }
        public void setContentSize(String contentSize) { this.contentSize = contentSize; }
        public String getChecksum() { return checksum; }
        public void setChecksum(String checksum) { this.checksum = checksum; }

        public String getFormattedSize() {
            if (contentSize == null || contentSize.isEmpty()) return "";
            try {
                long bytes = Long.parseLong(contentSize);
                if (bytes < 1024) return bytes + " B";
                if (bytes < 1024 * 1024) return String.format("%.1f KB", bytes / 1024.0);
                if (bytes < 1024 * 1024 * 1024) return String.format("%.1f MB", bytes / (1024.0 * 1024));
                return String.format("%.1f GB", bytes / (1024.0 * 1024 * 1024));
            } catch (NumberFormatException e) {
                return contentSize;
            }
        }
    }

    public static class RelatedRecord {
        private String id;
        private String name;
        private String objectName;
        private boolean active;
        private boolean deleted;
        private boolean archived;
        private String referencingField;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getObjectName() { return objectName; }
        public void setObjectName(String objectName) { this.objectName = objectName; }
        public boolean isActive() { return active; }
        public void setActive(boolean active) { this.active = active; }
        public boolean isDeleted() { return deleted; }
        public void setDeleted(boolean deleted) { this.deleted = deleted; }
        public boolean isArchived() { return archived; }
        public void setArchived(boolean archived) { this.archived = archived; }
        public String getReferencingField() { return referencingField; }
        public void setReferencingField(String referencingField) { this.referencingField = referencingField; }
    }

    public static class RecordVersion {
        private String id;
        private int period;
        private int version;
        private boolean isDeleted;
        private String objectName;
        private String name;
        private String s3path;
        private Map<String, Object> metadata;

        // Getters and setters
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public int getPeriod() { return period; }
        public void setPeriod(int period) { this.period = period; }
        public int getVersion() { return version; }
        public void setVersion(int version) { this.version = version; }
        public boolean isDeleted() { return isDeleted; }
        public void setDeleted(boolean deleted) { isDeleted = deleted; }
        public String getObjectName() { return objectName; }
        public void setObjectName(String objectName) { this.objectName = objectName; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getS3path() { return s3path; }
        public void setS3path(String s3path) { this.s3path = s3path; }
        public Map<String, Object> getMetadata() { return metadata; }
        public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }

        public String getFormattedPeriod() { return RestoreService.formatPeriod(period); }
        public boolean isArchivePeriod() { return period < 0; }
    }

    public static class DeletedRecord {
        private String id;
        private int period;
        private String name;
        private String objectName;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public int getPeriod() { return period; }
        public void setPeriod(int period) { this.period = period; }
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }
        public String getObjectName() { return objectName; }
        public void setObjectName(String objectName) { this.objectName = objectName; }
    }

    public static class ObjectStats {
        private String objectName;
        private int uniqueRecords;
        private int totalVersions;
        private int deletedCount;
        private int archivedCount;

        public String getObjectName() { return objectName; }
        public void setObjectName(String objectName) { this.objectName = objectName; }
        public int getUniqueRecords() { return uniqueRecords; }
        public void setUniqueRecords(int uniqueRecords) { this.uniqueRecords = uniqueRecords; }
        public int getTotalVersions() { return totalVersions; }
        public void setTotalVersions(int totalVersions) { this.totalVersions = totalVersions; }
        public int getDeletedCount() { return deletedCount; }
        public void setDeletedCount(int deletedCount) { this.deletedCount = deletedCount; }
        public int getArchivedCount() { return archivedCount; }
        public void setArchivedCount(int archivedCount) { this.archivedCount = archivedCount; }
    }

    public static class FieldChange {
        private String field;
        private String oldValue;
        private String newValue;
        private String type;

        public String getField() { return field; }
        public void setField(String field) { this.field = field; }
        public String getOldValue() { return oldValue; }
        public void setOldValue(String oldValue) { this.oldValue = oldValue; }
        public String getNewValue() { return newValue; }
        public void setNewValue(String newValue) { this.newValue = newValue; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
    }

    public static class RestoreLogEntry {
        private String newId;
        private String objectName;
        private int period;
        private String sandboxName;
        private Timestamp restoredAt;
        private String restoredFields;

        public String getNewId() { return newId; }
        public void setNewId(String newId) { this.newId = newId; }
        public String getObjectName() { return objectName; }
        public void setObjectName(String objectName) { this.objectName = objectName; }
        public int getPeriod() { return period; }
        public void setPeriod(int period) { this.period = period; }
        public String getSandboxName() { return sandboxName; }
        public void setSandboxName(String sandboxName) { this.sandboxName = sandboxName; }
        public Timestamp getRestoredAt() { return restoredAt; }
        public void setRestoredAt(Timestamp restoredAt) { this.restoredAt = restoredAt; }
        public String getRestoredFields() { return restoredFields; }
        public void setRestoredFields(String restoredFields) { this.restoredFields = restoredFields; }
        public boolean isCreatedNew() { return restoredFields == null; }
        public String getFormattedDate() {
            if (restoredAt == null) return "";
            return restoredAt.toLocalDateTime().format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm"));
        }
    }
}
