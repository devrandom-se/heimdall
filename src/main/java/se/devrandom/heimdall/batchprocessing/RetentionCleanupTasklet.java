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
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import se.devrandom.heimdall.web.RetentionService;

import java.time.LocalDate;

/**
 * Final batch step that runs the retention cleanup. The backup job runs daily, so this is
 * gated to one day of the month (run-on-day) and is opt-in (cleanup-enabled, default false).
 * Reuses the same RetentionService as the web GUI — no duplicated logic.
 */
@Component
public class RetentionCleanupTasklet implements Tasklet {

    private static final Logger log = LoggerFactory.getLogger(RetentionCleanupTasklet.class);

    private final RetentionService retentionService;
    private final boolean enabled;
    private final int runOnDay;
    private final int months;

    @Autowired
    public RetentionCleanupTasklet(
            RetentionService retentionService,
            @Value("${heimdall.retention.cleanup-enabled:false}") boolean enabled,
            @Value("${heimdall.retention.run-on-day:15}") int runOnDay,
            @Value("${heimdall.retention.months:3}") int months) {
        this.retentionService = retentionService;
        this.enabled = enabled;
        this.runOnDay = runOnDay;
        this.months = months;
    }

    /**
     * Gate: cleanup runs only when enabled AND today is the configured day-of-month.
     * Since the batch job runs daily, this keeps the cleanup to once a month.
     */
    static boolean shouldRun(LocalDate date, boolean enabled, int runOnDay) {
        return enabled && date.getDayOfMonth() == runOnDay;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) {
        LocalDate today = LocalDate.now();
        if (!shouldRun(today, enabled, runOnDay)) {
            log.info("Retention cleanup skipped (enabled={}, runOnDay={}, today day-of-month={})",
                    enabled, runOnDay, today.getDayOfMonth());
            return RepeatStatus.FINISHED;
        }

        log.info("Retention cleanup running (months={})", months);
        RetentionService.RetentionResult result = retentionService.execute(months);
        log.info("Retention cleanup complete: cutoff={}, deleted {} object rows, {} csv files, "
                        + "{} backup runs, {} s3 objects ({} failed)",
                result.cutoffPeriod(), result.deletedObjectRows(), result.deletedCsvFiles(),
                result.deletedBackupRuns(), result.deletedS3Objects(), result.failedS3Keys().size());
        return RepeatStatus.FINISHED;
    }
}
