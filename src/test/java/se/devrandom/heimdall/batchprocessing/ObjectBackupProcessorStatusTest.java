package se.devrandom.heimdall.batchprocessing;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.test.util.ReflectionTestUtils;
import se.devrandom.heimdall.salesforce.ApiLimitTracker;
import se.devrandom.heimdall.salesforce.CsvDownloadResult;
import se.devrandom.heimdall.salesforce.SalesforceService;
import se.devrandom.heimdall.salesforce.objects.BulkQueryRequest;
import se.devrandom.heimdall.salesforce.objects.DescribeSObjectResult;
import se.devrandom.heimdall.salesforce.objects.Heimdall_Backup_Config__c;
import se.devrandom.heimdall.storage.BackupStatisticsService;
import se.devrandom.heimdall.storage.PostgresService;
import se.devrandom.heimdall.testutil.PostgresTestBase;

import java.nio.file.Paths;
import java.sql.*;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Level 3: backup_runs must tell the truth about what happened to an object, and a batch whose
 * storage failed must never be checkpointed (it would otherwise never be fetched again).
 * Salesforce is mocked; PostgreSQL is real (Testcontainers).
 */
class ObjectBackupProcessorStatusTest extends PostgresTestBase {

    private static final String ORG_ID = "00D000000000001";
    private static final String LAST_ID = "001000000000002AAA";

    private PostgresService postgresService;
    private BackupStatisticsService statistics;
    private SalesforceService salesforce;
    private ObjectBackupProcessor processor;

    @BeforeEach
    void setUp() throws Exception {
        postgresService = new PostgresService(
                getJdbcUrl(), getUsername(), getPassword(),
                ORG_ID, true, new StandardEnvironment(), Optional.empty());
        postgresService.initializeDatabase();
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             Statement stmt = conn.createStatement()) {
            stmt.execute("DELETE FROM objects");
            stmt.execute("DELETE FROM backup_runs");
            stmt.execute("DELETE FROM object_stats");
        }

        statistics = new BackupStatisticsService();
        salesforce = mock(SalesforceService.class);

        processor = new ObjectBackupProcessor();
        ReflectionTestUtils.setField(processor, "salesforceService", salesforce);
        ReflectionTestUtils.setField(processor, "statisticsService", statistics);
        ReflectionTestUtils.setField(processor, "postgresService", postgresService);
        ReflectionTestUtils.setField(processor, "bulkQueryWarmupMs", 0L);

        DescribeSObjectResult describe = new DescribeSObjectResult();
        describe.name = "Account";
        describe.fields = List.of();   // no IsDeleted field: the deleted-records phase is skipped
        when(salesforce.describeSObject("Account")).thenReturn(describe);
        when(salesforce.shouldCheckCountFirst(any())).thenReturn(false);
        when(salesforce.getApiLimitTracker()).thenReturn(Optional.empty());
        when(salesforce.getBatchSize("Account")).thenReturn(2000);
        when(salesforce.createBulkQuery(any(), anyBoolean(), anyString(), anyString())).thenReturn(job("JobComplete"));
        when(salesforce.checkBulkQueryStatus(any())).thenAnswer(inv -> inv.getArgument(0));
        when(salesforce.downloadBulkQueryCsv(any(), anyInt())).thenReturn(download(false));
    }

    private static BulkQueryRequest job(String state) {
        BulkQueryRequest job = new BulkQueryRequest();
        job.id = "750TEST";
        job.object = "Account";
        job.operation = "query";
        job.state = state;
        return job;
    }

    private static CsvDownloadResult download(boolean hasMoreRecords) {
        CsvDownloadResult d = new CsvDownloadResult();
        d.csvPath = Paths.get("/tmp/750TEST.csv");
        d.objectName = "Account";
        d.isQueryAll = false;
        d.recordCount = 2;
        d.lastId = LAST_ID;
        d.lastModstamp = Timestamp.valueOf("2026-01-15 10:31:00");
        d.hasMoreRecords = hasMoreRecords;
        return d;
    }

    private record Run(String status, String checkpointId, String error, int records) {}

    private Run latestRun() throws SQLException {
        try (Connection conn = DriverManager.getConnection(getJdbcUrl(), getUsername(), getPassword());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT status, last_checkpoint_id, error_message, records_processed FROM backup_runs " +
                     "WHERE object_name = 'Account' AND query_all = false ORDER BY run_id DESC LIMIT 1")) {
            assertTrue(rs.next(), "expected a backup_runs row for Account");
            return new Run(rs.getString(1), rs.getString(2), rs.getString(3), rs.getInt(4));
        }
    }

    @Test
    void run_is_failed_and_checkpoint_not_written_when_storage_throws() throws SQLException {
        doThrow(new RuntimeException("Failed to store batch for Account: No space left on device"))
                .when(salesforce).processCsvToStorage(any());

        assertNull(processor.process(new Heimdall_Backup_Config__c("Account", "Backup")));

        Run run = latestRun();
        assertEquals("FAILED", run.status());
        assertNull(run.checkpointId(), "a batch that was not stored must not advance the checkpoint");
        assertTrue(run.error().contains("No space left on device"), run.error());
        assertTrue(statistics.getFailedObjects().containsKey("Account"));
    }

    @Test
    void run_is_partial_when_api_limit_is_reached_mid_object() throws SQLException {
        ApiLimitTracker tracker = mock(ApiLimitTracker.class);
        // process() pre-check, then the loop: batch 1 allowed, batch 2 blocked
        when(tracker.isLimitReached()).thenReturn(false, false, true);
        when(salesforce.getApiLimitTracker()).thenReturn(Optional.of(tracker));
        when(salesforce.downloadBulkQueryCsv(any(), anyInt())).thenReturn(download(true));

        assertNotNull(processor.process(new Heimdall_Backup_Config__c("Account", "Backup")));

        Run run = latestRun();
        assertEquals("PARTIAL", run.status());
        assertEquals(LAST_ID, run.checkpointId(), "the stored batch keeps its checkpoint");
        assertEquals(2, run.records());
        assertTrue(run.error().contains("API limit"), run.error());
    }

    @Test
    void run_is_failed_when_bulk_query_fails() throws SQLException {
        when(salesforce.checkBulkQueryStatus(any())).thenReturn(job("Failed"));

        assertNotNull(processor.process(new Heimdall_Backup_Config__c("Account", "Backup")));

        Run run = latestRun();
        assertEquals("FAILED", run.status());
        assertNull(run.checkpointId());
        assertTrue(run.error().contains("Bulk query failed at batch 1"), run.error());
        assertTrue(statistics.getFailedObjects().containsKey("Account"));
        verify(salesforce, never()).processCsvToStorage(any());
    }

    @Test
    void run_is_success_with_record_count_when_all_batches_stored() throws SQLException {
        assertNotNull(processor.process(new Heimdall_Backup_Config__c("Account", "Backup")));

        Run run = latestRun();
        assertEquals("SUCCESS", run.status());
        assertEquals(LAST_ID, run.checkpointId());
        assertEquals(2, run.records());
        assertNull(run.error());
        assertTrue(statistics.getFailedObjects().isEmpty());
    }
}
