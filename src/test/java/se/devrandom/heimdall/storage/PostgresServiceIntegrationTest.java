package se.devrandom.heimdall.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.devrandom.heimdall.testutil.PostgresTestBase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.*;
import java.util.Optional;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Level 3: Integration tests for PostgresService with a real PostgreSQL database.
 * Requires Docker running (Testcontainers).
 */
class PostgresServiceIntegrationTest extends PostgresTestBase {

    private static final String ORG_ID = "00D000000000001";
    private PostgresService service;

    @BeforeEach
    void setUp() throws SQLException {
        // Use Spring's Environment mock - pass null, it's only used in @PostConstruct guard
        service = new PostgresService(
                getJdbcUrl(), getUsername(), getPassword(),
                ORG_ID, new org.springframework.core.env.StandardEnvironment(),
                Optional.empty());

        // Initialize database tables
        service.initializeDatabase();

        // Clean tables between tests
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM objects");
            stmt.execute("DELETE FROM csvfiles");
            stmt.execute("DELETE FROM backup_runs");
            stmt.execute("DELETE FROM object_stats");
        }
    }

    // ===== Schema initialization =====

    @Test
    void initializeDatabase_createsTables() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             Statement stmt = conn.createStatement()) {

            // Verify tables exist by querying them
            assertDoesNotThrow(() -> stmt.executeQuery("SELECT COUNT(*) FROM csvfiles"));
            assertDoesNotThrow(() -> stmt.executeQuery("SELECT COUNT(*) FROM objects"));
            assertDoesNotThrow(() -> stmt.executeQuery("SELECT COUNT(*) FROM backup_runs"));
            assertDoesNotThrow(() -> stmt.executeQuery("SELECT COUNT(*) FROM object_stats"));
        }
    }

    @Test
    void initializeDatabase_isIdempotent() throws SQLException {
        // Should not throw on second call
        assertDoesNotThrow(() -> service.initializeDatabase());
    }

    // ===== CSV processing =====

    @Test
    void processCsvToPostgres_insertsRecords() throws IOException, SQLException {
        Path csvFile = createTempCsv(
                "Id,Name,IsDeleted,SystemModstamp\n" +
                "001000000000001,Acme Corp,false,2025-01-15T10:30:00Z\n" +
                "001000000000002,Beta Inc,false,2025-01-15T11:00:00Z\n");

        int count = service.processCsvToPostgres(csvFile, "Account", "s3://bucket/test.csv", false, 2501);
        assertEquals(2, count);

        // Verify records in DB
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT COUNT(*) FROM objects WHERE org_id = ? AND object_name = 'Account'")) {
            stmt.setString(1, ORG_ID);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }
    }

    @Test
    void processCsvToPostgres_handlesDeletedRecords() throws IOException, SQLException {
        Path csvFile = createTempCsv(
                "Id,Name,IsDeleted,SystemModstamp\n" +
                "001000000000001,Deleted Corp,true,2025-01-15T10:30:00Z\n");

        int count = service.processCsvToPostgres(csvFile, "Account", "s3://bucket/test.csv", true, 2501);
        assertEquals(1, count);

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT is_deleted FROM objects WHERE org_id = ? AND id = '001000000000001'")) {
            stmt.setString(1, ORG_ID);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertTrue(rs.getBoolean("is_deleted"));
        }
    }

    @Test
    void processCsvToPostgres_emptyFileReturnsZero() throws IOException, SQLException {
        Path csvFile = createTempCsv("Id,Name\n");
        int count = service.processCsvToPostgres(csvFile, "Account", "s3://bucket/empty.csv", false, 2501);
        assertEquals(0, count);
    }

    @Test
    void processCsvToPostgres_quotedFieldsWithCommas() throws IOException, SQLException {
        Path csvFile = createTempCsv(
                "Id,Name,IsDeleted,SystemModstamp\n" +
                "001000000000001,\"Smith, John & Co\",false,2025-01-15T10:30:00Z\n");

        int count = service.processCsvToPostgres(csvFile, "Account", "s3://bucket/test.csv", false, 2501);
        assertEquals(1, count);

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT name FROM objects WHERE org_id = ? AND id = '001000000000001'")) {
            stmt.setString(1, ORG_ID);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals("Smith, John & Co", rs.getString("name"));
        }
    }

    @Test
    void processCsvToPostgres_missingFileThrows() {
        assertThrows(IOException.class, () ->
                service.processCsvToPostgres(Path.of("/nonexistent.csv"), "Account", "s3://x", false, 2501));
    }

    @Test
    void processCsvToPostgres_onConflictDoesNothing() throws IOException, SQLException {
        Path csvFile = createTempCsv(
                "Id,Name,IsDeleted,SystemModstamp\n" +
                "001000000000001,Acme Corp,false,2025-01-15T10:30:00Z\n");

        // Insert same data twice (same period, same id, same version)
        service.processCsvToPostgres(csvFile, "Account", "s3://bucket/test1.csv", false, 2501);
        int count2 = service.processCsvToPostgres(csvFile, "Account", "s3://bucket/test2.csv", false, 2501);
        // ON CONFLICT DO NOTHING - executeBatch returns 0 for conflict rows
        assertTrue(count2 <= 1);
    }

    @Test
    void processCsvToPostgres_storesMetadataWithReferenceFields() throws IOException, SQLException {
        Path csvFile = createTempCsv(
                "Id,Name,AccountId,OwnerId,IsDeleted,SystemModstamp\n" +
                "003000000000001,John,001000000000001,005000000000001,false,2025-01-15T10:30:00Z\n");

        service.processCsvToPostgres(csvFile, "Contact", "s3://bucket/test.csv", false, 2501);

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT metadata FROM objects WHERE org_id = ? AND id = '003000000000001'")) {
            stmt.setString(1, ORG_ID);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            String metadata = rs.getString("metadata");
            // Fields ending in "Id" (except "Id" itself) should be in metadata
            assertTrue(metadata.contains("AccountId"));
            assertTrue(metadata.contains("OwnerId"));
        }
    }

    // ===== Backup runs =====

    @Test
    void createAndCompleteBackupRun() throws SQLException {
        long runId = service.createBackupRun("Account", false);
        assertTrue(runId > 0);

        // Set a checkpoint (getLatestBackupRun requires checkpoint to be set)
        Timestamp checkpoint = Timestamp.valueOf("2025-01-15 10:30:00");
        service.updateBackupRunCheckpoint(runId, "001000000000099", checkpoint, 50);

        service.completeBackupRun(runId, "SUCCESS", 100, 0, 1024L, null);

        PostgresService.BackupRun run = service.getLatestBackupRun("Account", false);
        assertNotNull(run);
        assertEquals("SUCCESS", run.status);
        assertEquals(100, run.recordsProcessed);
    }

    @Test
    void updateBackupRunCheckpoint() throws SQLException {
        long runId = service.createBackupRun("Account", false);

        Timestamp checkpoint = Timestamp.valueOf("2025-01-15 10:30:00");
        service.updateBackupRunCheckpoint(runId, "001000000000050", checkpoint, 50);

        PostgresService.BackupRun run = service.getLatestBackupRun("Account", false);
        assertNotNull(run);
        assertEquals("001000000000050", run.lastCheckpointId);
        assertEquals(50, run.recordsProcessed);
    }

    @Test
    void getLatestBackupRun_returnsNullWhenNoRuns() throws SQLException {
        assertNull(service.getLatestBackupRun("NoSuchObject", false));
    }

    // ===== Archive runs =====

    @Test
    void createAndGetArchiveRun() throws SQLException {
        long runId = service.createArchiveRun("Account", "CreatedDate < 2024-01-01");
        assertTrue(runId > 0);

        Timestamp checkpoint = Timestamp.valueOf("2025-01-15 10:30:00");
        service.updateBackupRunCheckpoint(runId, "001000000000050", checkpoint, 50);

        PostgresService.BackupRun run = service.getLatestArchiveRun("Account");
        assertNotNull(run);
        assertTrue(run.period < 0, "Archive period should be negative");
        assertEquals("CreatedDate < 2024-01-01", run.archiveExpression);
        assertEquals("001000000000050", run.lastCheckpointId);
    }

    @Test
    void getLatestArchiveRun_returnsNullWhenNoRuns() throws SQLException {
        assertNull(service.getLatestArchiveRun("NoSuchObject"));
    }

    // ===== Object stats =====

    @Test
    void refreshObjectStats_computesStats() throws IOException, SQLException {
        Path csvFile = createTempCsv(
                "Id,Name,IsDeleted,SystemModstamp\n" +
                "001000000000001,Acme,false,2025-01-15T10:30:00Z\n" +
                "001000000000002,Beta,true,2025-01-15T11:00:00Z\n");

        service.processCsvToPostgres(csvFile, "Account", "s3://bucket/test.csv", false, 2501);
        service.refreshObjectStats("Account");

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT unique_records, deleted_count FROM object_stats WHERE org_id = ? AND object_name = 'Account'")) {
            stmt.setString(1, ORG_ID);
            ResultSet rs = stmt.executeQuery();
            assertTrue(rs.next());
            assertEquals(2, rs.getInt("unique_records"));
            assertEquals(1, rs.getInt("deleted_count"));
        }
    }

    // ===== ContentVersion checksums =====

    @Test
    void loadContentVersionChecksums_emptyWhenNoData() {
        Set<String> checksums = service.loadContentVersionChecksums();
        assertTrue(checksums.isEmpty());
    }

    @Test
    void loadContentVersionChecksums_returnsChecksumKeys() throws IOException, SQLException {
        // Insert a ContentVersion record with Checksum and FileExtension in metadata
        Path csvFile = createTempCsv(
                "Id,Name,Checksum,FileExtension,IsDeleted,SystemModstamp\n" +
                "068000000000001,test.pdf,abc123def456,pdf,false,2025-01-15T10:30:00Z\n");

        service.processCsvToPostgres(csvFile, "ContentVersion", "s3://bucket/cv.csv", false, 2501);

        Set<String> checksums = service.loadContentVersionChecksums();
        assertTrue(checksums.contains("abc123def456.pdf"));
    }

    // ===== Helper =====

    private Path createTempCsv(String content) throws IOException {
        Path tempFile = Files.createTempFile("test-csv-", ".csv");
        Files.writeString(tempFile, content);
        tempFile.toFile().deleteOnExit();
        return tempFile;
    }
}
