package se.devrandom.heimdall.web;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import se.devrandom.heimdall.storage.PostgresService;
import se.devrandom.heimdall.storage.RdsLifecycleService;
import se.devrandom.heimdall.storage.S3Service;
import se.devrandom.heimdall.testutil.PostgresTestBase;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Level 3: Integration tests for RestoreService database queries.
 * Uses real PostgreSQL via Testcontainers with mocked S3Service.
 */
class RestoreServiceIntegrationTest extends PostgresTestBase {

    private static final String ORG_ID = "00D000000000001";

    private RestoreService restoreService;
    private PostgresService postgresService;
    private S3Service mockS3;

    @BeforeEach
    void setUp() throws Exception {
        // Initialize PostgresService to create schema
        postgresService = new PostgresService(
                getJdbcUrl(), getUsername(), getPassword(),
                ORG_ID, new org.springframework.core.env.StandardEnvironment(),
                Optional.empty());
        postgresService.initializeDatabase();

        // Create RestoreService and inject fields via reflection
        restoreService = new RestoreService();
        mockS3 = mock(S3Service.class);
        ReflectionTestUtils.setField(restoreService, "s3Service", mockS3);
        ReflectionTestUtils.setField(restoreService, "rdsLifecycleService", Optional.<RdsLifecycleService>empty());
        ReflectionTestUtils.setField(restoreService, "dbUrl", getJdbcUrl());
        ReflectionTestUtils.setField(restoreService, "dbUsername", getUsername());
        ReflectionTestUtils.setField(restoreService, "dbPassword", getPassword());
        ReflectionTestUtils.setField(restoreService, "orgId", ORG_ID);

        // Invoke @PostConstruct to initialize the connection pool and restore_log table
        Method initPool = RestoreService.class.getDeclaredMethod("initPool");
        initPool.setAccessible(true);
        initPool.invoke(restoreService);

        // Clean tables between tests
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM objects");
            stmt.execute("DELETE FROM csvfiles");
            stmt.execute("DELETE FROM object_stats");
            stmt.execute("DELETE FROM restore_log");
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        Method closePool = RestoreService.class.getDeclaredMethod("closePool");
        closePool.setAccessible(true);
        closePool.invoke(restoreService);
    }

    // ===== getRecordVersions =====

    @Test
    void getRecordVersions_returnsVersionsOrderedByPeriodDesc() throws Exception {
        insertRecord("001X", "Account", "Acme", 2501, 1000, false, "{}");
        insertRecord("001X", "Account", "Acme Updated", 2502, 2000, false, "{\"Phone\":\"123\"}");

        // S3 fetch returns null (fall back to DB metadata)
        when(mockS3.getBucketName()).thenReturn("test-bucket");

        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions("001X");

        assertEquals(2, versions.size());
        // First should be latest period
        assertEquals(2502, versions.get(0).getPeriod());
        assertEquals(2501, versions.get(1).getPeriod());
        assertEquals("Account", versions.get(0).getObjectName());
        assertEquals("Acme Updated", versions.get(0).getName());
    }

    @Test
    void getRecordVersions_returnsEmptyForUnknownId() {
        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions("UNKNOWN");
        assertTrue(versions.isEmpty());
    }

    @Test
    void getRecordVersions_includesMetadataFromDb() throws Exception {
        insertRecord("001X", "Account", "Acme", 2501, 1000, false, "{\"Phone\":\"555-1234\",\"Industry\":\"Tech\"}");

        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions("001X");

        assertEquals(1, versions.size());
        Map<String, Object> metadata = versions.get(0).getMetadata();
        assertEquals("555-1234", metadata.get("Phone"));
        assertEquals("Tech", metadata.get("Industry"));
    }

    @Test
    void getRecordVersions_includesDeletedFlag() throws Exception {
        insertRecord("001X", "Account", "Deleted Co", 2501, 1000, true, "{}");

        List<RestoreService.RecordVersion> versions = restoreService.getRecordVersions("001X");

        assertEquals(1, versions.size());
        assertTrue(versions.get(0).isDeleted());
    }

    // ===== getObjectStatistics =====

    @Test
    void getObjectStatistics_returnsStatsOrderedByRecordCount() throws Exception {
        // Insert stats directly
        insertObjectStats("Account", 100, 200, 5, 0);
        insertObjectStats("Contact", 50, 75, 2, 10);

        Map<String, RestoreService.ObjectStats> stats = restoreService.getObjectStatistics();

        assertEquals(2, stats.size());
        // Ordered by unique_records DESC
        List<String> keys = List.copyOf(stats.keySet());
        assertEquals("Account", keys.get(0));
        assertEquals("Contact", keys.get(1));

        RestoreService.ObjectStats accountStats = stats.get("Account");
        assertEquals(100, accountStats.getUniqueRecords());
        assertEquals(200, accountStats.getTotalVersions());
        assertEquals(5, accountStats.getDeletedCount());
        assertEquals(0, accountStats.getArchivedCount());

        RestoreService.ObjectStats contactStats = stats.get("Contact");
        assertEquals(10, contactStats.getArchivedCount());
    }

    @Test
    void getObjectStatistics_emptyWhenNoData() {
        Map<String, RestoreService.ObjectStats> stats = restoreService.getObjectStatistics();
        assertTrue(stats.isEmpty());
    }

    // ===== getDeletedRecords / countDeletedRecords =====

    @Test
    void getDeletedRecords_returnsDeletedOnly() throws Exception {
        insertRecord("001A", "Account", "Active Co", 2501, 1000, false, "{}");
        insertRecord("001D", "Account", "Deleted Co", 2501, 1000, true, "{}");

        List<RestoreService.DeletedRecord> deleted = restoreService.getDeletedRecords("Account", 0, 50);

        assertEquals(1, deleted.size());
        assertEquals("001D", deleted.get(0).getId());
        assertEquals("Deleted Co", deleted.get(0).getName());
    }

    @Test
    void countDeletedRecords_countsCorrectly() throws Exception {
        insertRecord("001D1", "Account", "Del1", 2501, 1000, true, "{}");
        insertRecord("001D2", "Account", "Del2", 2501, 1000, true, "{}");
        insertRecord("001A", "Account", "Active", 2501, 1000, false, "{}");

        assertEquals(2, restoreService.countDeletedRecords("Account"));
    }

    @Test
    void getDeletedRecords_pagination() throws Exception {
        // Insert 5 deleted records
        for (int i = 1; i <= 5; i++) {
            insertRecord("001D" + i, "Account", "Del" + i, 2501, 1000 + i, true, "{}");
        }

        List<RestoreService.DeletedRecord> page1 = restoreService.getDeletedRecords("Account", 0, 2);
        List<RestoreService.DeletedRecord> page2 = restoreService.getDeletedRecords("Account", 1, 2);

        assertEquals(2, page1.size());
        assertEquals(2, page2.size());
        // Pages should not overlap
        assertNotEquals(page1.get(0).getId(), page2.get(0).getId());
    }

    // ===== getObjectsWithDeletedRecords =====

    @Test
    void getObjectsWithDeletedRecords_returnsDistinctObjects() throws Exception {
        insertRecord("001D", "Account", "Del", 2501, 1000, true, "{}");
        insertRecord("003D", "Contact", "Del", 2501, 1000, true, "{}");
        insertRecord("001A", "Account", "Active", 2501, 2000, false, "{}");

        List<String> objects = restoreService.getObjectsWithDeletedRecords();

        assertEquals(2, objects.size());
        assertTrue(objects.contains("Account"));
        assertTrue(objects.contains("Contact"));
    }

    // ===== getArchivedRecords / countArchivedRecords =====

    @Test
    void getArchivedRecords_returnsRecordsWithNegativePeriod() throws Exception {
        insertRecord("001A", "Account", "Active", 2501, 1000, false, "{}");
        insertRecord("001R", "Account", "Archived", -2501, 1000, false, "{}");

        List<RestoreService.DeletedRecord> archived = restoreService.getArchivedRecords("Account", 0, 50);

        assertEquals(1, archived.size());
        assertEquals("001R", archived.get(0).getId());
        assertTrue(archived.get(0).isArchived());
    }

    @Test
    void countArchivedRecords_usesObjectStats() throws Exception {
        insertObjectStats("Account", 100, 200, 5, 15);

        assertEquals(15, restoreService.countArchivedRecords("Account"));
    }

    @Test
    void countArchivedRecords_zeroWhenNoStats() {
        assertEquals(0, restoreService.countArchivedRecords("NoSuchObject"));
    }

    // ===== getObjectsWithArchivedRecords =====

    @Test
    void getObjectsWithArchivedRecords_usesObjectStats() throws Exception {
        insertObjectStats("Account", 100, 200, 5, 15);
        insertObjectStats("Contact", 50, 75, 2, 0);

        List<String> objects = restoreService.getObjectsWithArchivedRecords();

        assertEquals(1, objects.size());
        assertEquals("Account", objects.get(0));
    }

    // ===== getRecordsForObject =====

    @Test
    void getRecordsForObject_returnsAllRecords() throws Exception {
        insertRecord("001A", "Account", "Active", 2501, 1000, false, "{}");
        insertRecord("001D", "Account", "Deleted", 2501, 1000, true, "{}");

        List<RestoreService.DeletedRecord> records = restoreService.getRecordsForObject("Account", 0, 50);

        assertEquals(2, records.size());
    }

    // ===== countAllArchivedRecords =====

    @Test
    void countAllArchivedRecords_countsAcrossObjects() throws Exception {
        insertRecord("001R1", "Account", "Arch1", -2501, 1000, false, "{}");
        insertRecord("003R1", "Contact", "Arch2", -2501, 1000, false, "{}");
        insertRecord("001A", "Account", "Active", 2501, 1000, false, "{}");

        assertEquals(2, restoreService.countAllArchivedRecords());
    }

    // ===== logRestore / getRestoreHistory =====

    @Test
    void logRestore_insertsEntry() {
        restoreService.logRestore("001X", "001Y", "Account", 2501, "mysandbox", List.of("Name", "Phone"));

        List<RestoreService.RestoreLogEntry> history = restoreService.getRestoreHistory("001X");

        assertEquals(1, history.size());
        RestoreService.RestoreLogEntry entry = history.get(0);
        assertEquals("001Y", entry.getNewId());
        assertEquals("Account", entry.getObjectName());
        assertEquals(2501, entry.getPeriod());
        assertEquals("mysandbox", entry.getSandboxName());
        assertEquals("Name, Phone", entry.getRestoredFields());
        assertFalse(entry.isCreatedNew());
    }

    @Test
    void logRestore_nullFieldsMeansCreatedNew() {
        restoreService.logRestore("001X", "001Z", "Account", 2501, "mysandbox", null);

        List<RestoreService.RestoreLogEntry> history = restoreService.getRestoreHistory("001X");

        assertEquals(1, history.size());
        assertTrue(history.get(0).isCreatedNew());
        assertNull(history.get(0).getRestoredFields());
    }

    @Test
    void getRestoreHistory_orderedByDateDesc() {
        restoreService.logRestore("001X", "001A", "Account", 2501, "sb1", null);
        restoreService.logRestore("001X", "001B", "Account", 2502, "sb1", List.of("Name"));

        List<RestoreService.RestoreLogEntry> history = restoreService.getRestoreHistory("001X");

        assertEquals(2, history.size());
        // Most recent first
        assertEquals("001B", history.get(0).getNewId());
        assertEquals("001A", history.get(1).getNewId());
    }

    @Test
    void getRestoreHistory_emptyForUnknownRecord() {
        List<RestoreService.RestoreLogEntry> history = restoreService.getRestoreHistory("UNKNOWN");
        assertTrue(history.isEmpty());
    }

    @Test
    void restoreLogEntry_formattedDate() {
        restoreService.logRestore("001X", "001Y", "Account", 2501, "sb", null);

        List<RestoreService.RestoreLogEntry> history = restoreService.getRestoreHistory("001X");
        assertFalse(history.isEmpty());
        String formatted = history.get(0).getFormattedDate();
        // Should match yyyy-MM-dd HH:mm pattern
        assertTrue(formatted.matches("\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}"), "Got: " + formatted);
    }

    // ===== getRelatedFiles =====

    @Test
    void getRelatedFiles_emptyWhenNoLinks() {
        List<RestoreService.RelatedFile> files = restoreService.getRelatedFiles("001X");
        assertTrue(files.isEmpty());
    }

    @Test
    void getRelatedFiles_findsLinkedContentVersions() throws Exception {
        // ContentDocumentLink linking record 001X to doc 069D1
        insertRecord("CDL001", "ContentDocumentLink", "link",
                2501, 1000, false,
                "{\"LinkedEntityId\":\"001X\",\"ContentDocumentId\":\"069D1\"}");

        // ContentVersion for that document
        insertRecord("068CV1", "ContentVersion", "report.pdf",
                2501, 1000, false,
                "{\"ContentDocumentId\":\"069D1\",\"Title\":\"Quarterly Report\",\"FileType\":\"PDF\",\"FileExtension\":\"pdf\",\"ContentSize\":\"1024\",\"Checksum\":\"abc123\"}");

        List<RestoreService.RelatedFile> files = restoreService.getRelatedFiles("001X");

        assertEquals(1, files.size());
        RestoreService.RelatedFile file = files.get(0);
        assertEquals("068CV1", file.getContentVersionId());
        assertEquals("069D1", file.getContentDocumentId());
        assertEquals("Quarterly Report", file.getName());
        assertEquals("PDF", file.getFileType());
        assertEquals("pdf", file.getFileExtension());
        assertEquals("1024", file.getContentSize());
        assertEquals("abc123", file.getChecksum());
    }

    // ===== getRelatedChildRecords =====

    @Test
    void getRelatedChildRecords_emptyWhenNoReferences() {
        List<RestoreService.RelatedRecord> records = restoreService.getRelatedChildRecords("001X");
        assertTrue(records.isEmpty());
    }

    @Test
    void getRelatedChildRecords_findsReferencingRecords() throws Exception {
        // Parent Account
        insertRecord("001X", "Account", "Parent Co", 2501, 1000, false, "{}");
        // Contact referencing the Account
        insertRecord("003C1", "Contact", "John Doe", 2501, 1000, false,
                "{\"AccountId\":\"001X\"}");

        List<RestoreService.RelatedRecord> related = restoreService.getRelatedChildRecords("001X");

        assertEquals(1, related.size());
        RestoreService.RelatedRecord rec = related.get(0);
        assertEquals("003C1", rec.getId());
        assertEquals("John Doe", rec.getName());
        assertEquals("Contact", rec.getObjectName());
        assertEquals("AccountId", rec.getReferencingField());
        assertTrue(rec.isActive());
    }

    @Test
    void getRelatedChildRecords_excludesContentObjects() throws Exception {
        insertRecord("001X", "Account", "Parent", 2501, 1000, false, "{}");
        // ContentDocumentLink referencing account - should be excluded
        insertRecord("CDL001", "ContentDocumentLink", "link", 2501, 1000, false,
                "{\"LinkedEntityId\":\"001X\"}");

        List<RestoreService.RelatedRecord> related = restoreService.getRelatedChildRecords("001X");

        assertTrue(related.isEmpty());
    }

    // ===== Helper methods =====

    private void insertRecord(String id, String objectName, String name,
                              int period, int version, boolean isDeleted,
                              String metadataJson) throws Exception {
        // First insert a csvfile reference
        int csvId;
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO csvfiles (period, s3path) VALUES (?, ?) RETURNING csv_id")) {
            stmt.setInt(1, period);
            stmt.setString(2, "s3://test-bucket/test.csv");
            ResultSet rs = stmt.executeQuery();
            rs.next();
            csvId = rs.getInt("csv_id");
        }

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO objects (id, period, version, metadata, org_id, csv_id, is_deleted, object_name, name) " +
                     "VALUES (?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)")) {
            stmt.setString(1, id);
            stmt.setInt(2, period);
            stmt.setInt(3, version);
            stmt.setString(4, metadataJson);
            stmt.setString(5, ORG_ID);
            stmt.setInt(6, csvId);
            stmt.setBoolean(7, isDeleted);
            stmt.setString(8, objectName);
            stmt.setString(9, name);
            stmt.executeUpdate();
        }
    }

    private void insertObjectStats(String objectName, int uniqueRecords, int totalVersions,
                                   int deletedCount, int archivedCount) throws Exception {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "INSERT INTO object_stats (org_id, object_name, unique_records, total_versions, deleted_count, archived_count) " +
                     "VALUES (?, ?, ?, ?, ?, ?)")) {
            stmt.setString(1, ORG_ID);
            stmt.setString(2, objectName);
            stmt.setInt(3, uniqueRecords);
            stmt.setInt(4, totalVersions);
            stmt.setInt(5, deletedCount);
            stmt.setInt(6, archivedCount);
            stmt.executeUpdate();
        }
    }
}
