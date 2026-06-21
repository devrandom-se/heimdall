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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import se.devrandom.heimdall.storage.PostgresService;
import se.devrandom.heimdall.storage.S3Service;

import java.time.YearMonth;
import java.util.ArrayList;
import java.util.List;

/**
 * Retention cleanup: removes backups older than a configurable window (default 3 months).
 * Deletes the period-partitioned CSV objects from S3 and the corresponding DB rows.
 * Archives (period < 0) are never touched; only active backups (period > 0) are eligible.
 */
@Service
public class RetentionService {

    private static final Logger log = LoggerFactory.getLogger(RetentionService.class);

    private final PostgresService postgresService;
    private final S3Service s3Service;
    private final int defaultRetentionMonths;
    private final int deleteBatchSize;

    public RetentionService(PostgresService postgresService,
                            S3Service s3Service,
                            @Value("${heimdall.retention.months:3}") int defaultRetentionMonths,
                            @Value("${heimdall.retention.delete-batch-size:50000}") int deleteBatchSize) {
        this.postgresService = postgresService;
        this.s3Service = s3Service;
        this.defaultRetentionMonths = defaultRetentionMonths;
        this.deleteBatchSize = deleteBatchSize;
    }

    public int getDefaultRetentionMonths() {
        return defaultRetentionMonths;
    }

    /**
     * Compute the cutoff period (YYMM) for a retention window. Backups with
     * period > 0 AND period < cutoff are eligible for cleanup.
     *
     * period is YYMM-encoded (2606 = June 2026), so the window must be computed via
     * YearMonth — NOT as period - months, which would corrupt the encoding across
     * month/year boundaries.
     */
    static int computeCutoffPeriod(YearMonth current, int months) {
        YearMonth cutoff = current.minusMonths(months);
        return (cutoff.getYear() % 100) * 100 + cutoff.getMonthValue();
    }

    /** A single period's contribution to the preview/result, with a human-readable label. */
    public record PeriodRow(int period, String label, long csvFiles) {}

    /** Dry-run summary of what a cleanup would remove (one CSV file == one S3 object). */
    public record RetentionPreview(int months, int cutoffPeriod, String cutoffLabel,
                                   List<PeriodRow> periods, long totalCsvFiles) {}

    /** Outcome of an executed cleanup. */
    public record RetentionResult(int months, int cutoffPeriod,
                                  int deletedObjectRows, int deletedCsvFiles, int deletedBackupRuns,
                                  int deletedS3Objects, List<String> failedS3Keys) {}

    private void validateMonths(int months) {
        if (months < 1) {
            throw new IllegalArgumentException("retention months must be >= 1, got " + months);
        }
    }

    /**
     * Compute, without mutating anything, what a cleanup with the given window would remove.
     */
    public RetentionPreview preview(int months) {
        validateMonths(months);
        int cutoff = computeCutoffPeriod(YearMonth.now(), months);
        PostgresService.CleanupPreview summary = postgresService.getCleanupPreview(cutoff);

        List<PeriodRow> rows = new ArrayList<>();
        for (PostgresService.CleanupPeriod p : summary.periods()) {
            rows.add(new PeriodRow(p.period(), RestoreService.formatPeriod(p.period()), p.csvFiles()));
        }
        return new RetentionPreview(months, cutoff, RestoreService.formatPeriod(cutoff),
                rows, summary.totalCsvFiles());
    }

    /**
     * Execute the cleanup: delete S3 objects first, then DB rows in one transaction.
     * S3 before DB so a partial S3 failure leaves only orphaned (logged) objects rather
     * than DB rows pointing at deleted files. The whole operation is idempotent.
     */
    public RetentionResult execute(int months) {
        validateMonths(months);
        int cutoff = computeCutoffPeriod(YearMonth.now(), months);

        List<String> s3Paths = postgresService.getS3PathsToDelete(cutoff);
        log.info("Retention cleanup starting: months={}, cutoff={}, {} csv/s3 objects",
                months, cutoff, s3Paths.size());

        List<String> failedS3 = s3Service.deleteObjects(s3Paths);
        if (!failedS3.isEmpty()) {
            log.warn("Retention cleanup: {} S3 objects failed to delete (orphaned, manual cleanup needed): {}",
                    failedS3.size(), failedS3);
        }

        PostgresService.CleanupDeletionResult db = postgresService.deleteBackupsBefore(cutoff, deleteBatchSize);
        int deletedS3 = s3Paths.size() - failedS3.size();

        log.info("Retention cleanup done: cutoff={}, deleted {} object rows, {} csv files, {} backup runs, {} s3 objects ({} failed)",
                cutoff, db.objectRows(), db.csvFiles(), db.backupRuns(), deletedS3, failedS3.size());

        return new RetentionResult(months, cutoff, db.objectRows(), db.csvFiles(), db.backupRuns(),
                deletedS3, failedS3);
    }
}
