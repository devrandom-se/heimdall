package se.devrandom.heimdall.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import se.devrandom.heimdall.storage.PostgresService;
import se.devrandom.heimdall.testutil.PostgresTestBase;

import java.sql.*;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Level 3 integration test: retention cleanup against a real PostgreSQL (Testcontainers).
 * Pins the safety invariants — archives (period < 0) and fresh periods survive, and
 * csvfiles belonging to a different org (matched by s3path prefix) are never touched.
 */
class RetentionServiceIntegrationTest extends PostgresTestBase {

    private static final String ORG_ID = "00D000000000001";
    private static final String OTHER_ORG = "00D000000000999";

    private PostgresService postgresService;

    @BeforeEach
    void setUp() throws Exception {
        postgresService = new PostgresService(
                getJdbcUrl(), getUsername(), getPassword(),
                ORG_ID, new org.springframework.core.env.StandardEnvironment(),
                Optional.empty());
        postgresService.initializeDatabase();

        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM objects");
            stmt.execute("DELETE FROM csvfiles");
            stmt.execute("DELETE FROM backup_runs");
        }

        // 2601 = old (eligible), 2605 = fresh (kept), -2601 = archive (kept)
        insertObject("a1", 2601);
        insertObject("a2", 2605);
        insertObject("a3", -2601);

        insertCsv(2601, "OrgId=" + ORG_ID + "/Period=202601/Year=2026/Month=01/Date=15/Account/a.csv");
        insertCsv(2605, "OrgId=" + ORG_ID + "/Period=202605/Year=2026/Month=05/Date=15/Account/a.csv");
        insertCsv(-2601, "OrgId=" + ORG_ID + "/Period=Archive-202601/Year=2026/Month=01/Date=15/Account/a.csv");
        // A different org's old CSV — must never be touched (csvfiles has no org_id column).
        insertCsv(2601, "OrgId=" + OTHER_ORG + "/Period=202601/Year=2026/Month=01/Date=15/Account/a.csv");

        insertBackupRun(2601);
        insertBackupRun(2605);
        insertBackupRun(-2601);
    }

    private void insertObject(String id, int period) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO objects (id, period, version, metadata, org_id, csv_id, is_deleted, object_name, name) " +
                 "VALUES (?, ?, 1, '{}'::jsonb, ?, NULL, false, 'Account', ?)")) {
            stmt.setString(1, id);
            stmt.setInt(2, period);
            stmt.setString(3, ORG_ID);
            stmt.setString(4, id);
            stmt.executeUpdate();
        }
    }

    private void insertCsv(int period, String s3path) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO csvfiles (period, s3path) VALUES (?, ?)")) {
            stmt.setInt(1, period);
            stmt.setString(2, s3path);
            stmt.executeUpdate();
        }
    }

    private void insertBackupRun(int period) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             PreparedStatement stmt = conn.prepareStatement(
                 "INSERT INTO backup_runs (object_name, org_id, period, status) VALUES ('Account', ?, ?, 'SUCCESS')")) {
            stmt.setString(1, ORG_ID);
            stmt.setInt(2, period);
            stmt.executeUpdate();
        }
    }

    private long count(String table, String where) throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table + " WHERE " + where)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    @Test
    void preview_counts_only_eligible_active_periods_for_this_org() {
        PostgresService.CleanupPreview summary = postgresService.getCleanupPreview(2603);
        assertEquals(1, summary.periods().size());
        assertEquals(2601, summary.periods().get(0).period());
        assertEquals(1, summary.periods().get(0).csvFiles());
        assertEquals(1, summary.totalCsvFiles());          // OTHER_ORG csv excluded
    }

    @Test
    void s3PathsToDelete_returns_only_this_orgs_old_paths() {
        List<String> paths = postgresService.getS3PathsToDelete(2603);
        assertEquals(1, paths.size());
        assertTrue(paths.get(0).contains("OrgId=" + ORG_ID));
    }

    @Test
    void delete_removes_old_active_and_preserves_archive_fresh_and_other_org() throws SQLException {
        PostgresService.CleanupDeletionResult result = postgresService.deleteBackupsBefore(2603, 50000);

        assertEquals(1, result.objectRows());
        assertEquals(1, result.csvFiles());
        assertEquals(1, result.backupRuns());

        // objects: archive (-2601) and fresh (2605) survive; old (2601) gone
        assertEquals(0, count("objects", "period = 2601"));
        assertEquals(1, count("objects", "period = 2605"));
        assertEquals(1, count("objects", "period = -2601"));

        // csvfiles: this org's old gone; archive + fresh + OTHER_ORG old survive
        assertEquals(0, count("csvfiles", "period = 2601 AND s3path LIKE 'OrgId=" + ORG_ID + "/%'"));
        assertEquals(1, count("csvfiles", "period = 2605"));
        assertEquals(1, count("csvfiles", "period = -2601"));
        assertEquals(1, count("csvfiles", "s3path LIKE 'OrgId=" + OTHER_ORG + "/%'"));

        // backup_runs: archive + fresh survive
        assertEquals(0, count("backup_runs", "period = 2601"));
        assertEquals(1, count("backup_runs", "period = 2605"));
        assertEquals(1, count("backup_runs", "period = -2601"));
    }

    @Test
    void delete_batches_through_more_rows_than_the_batch_size() throws SQLException {
        // setUp already seeded one old (2601) row; add 5 more → 6 old rows total.
        for (int i = 0; i < 5; i++) {
            insertObject("old" + i, 2601);
        }

        // Batch size 2 forces several chunks; all 6 must still be removed.
        PostgresService.CleanupDeletionResult result = postgresService.deleteBackupsBefore(2603, 2);

        assertEquals(6, result.objectRows());
        assertEquals(0, count("objects", "period = 2601"));
        // archive + fresh untouched
        assertEquals(1, count("objects", "period = 2605"));
        assertEquals(1, count("objects", "period = -2601"));
    }
}
