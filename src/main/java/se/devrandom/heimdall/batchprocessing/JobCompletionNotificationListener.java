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
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import se.devrandom.heimdall.salesforce.ApiLimitTracker;
import se.devrandom.heimdall.salesforce.SalesforceService;
import se.devrandom.heimdall.storage.BackupStatisticsService;
import se.devrandom.heimdall.storage.PostgresService;
import se.devrandom.heimdall.storage.RdsLifecycleService;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Component
@ConditionalOnProperty(name = "spring.batch.job.enabled", havingValue = "true", matchIfMissing = true)
public class JobCompletionNotificationListener implements JobExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    /** Exit code for a job that ran to completion but with at least one object failed, partial or skipped. */
    static final int EXIT_COMPLETED_WITH_ERRORS = 2;

    private final SalesforceService salesforceService;
    private final ApplicationContext applicationContext;
    private final BackupStatisticsService statisticsService;
    private final PostgresService postgresService;
    private final Environment environment;
    private final Optional<RdsLifecycleService> rdsLifecycleService;
    private final Optional<ApiLimitTracker> apiLimitTracker;

    public JobCompletionNotificationListener(SalesforceService salesforceService,
                                            ApplicationContext applicationContext,
                                            BackupStatisticsService statisticsService,
                                            PostgresService postgresService,
                                            Environment environment,
                                            Optional<RdsLifecycleService> rdsLifecycleService,
                                            Optional<ApiLimitTracker> apiLimitTracker) {
        this.salesforceService = salesforceService;
        this.applicationContext = applicationContext;
        this.statisticsService = statisticsService;
        this.postgresService = postgresService;
        this.environment = environment;
        this.rdsLifecycleService = rdsLifecycleService;
        this.apiLimitTracker = apiLimitTracker;
    }

    private boolean isWebMode() {
        return Arrays.asList(environment.getActiveProfiles()).contains("web");
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Starting job");
        try {
            List<String> abandoned = postgresService.abandonStaleRuns();
            if (!abandoned.isEmpty()) {
                log.warn("Marked {} stale RUNNING backup run(s) from an earlier job as ABANDONED: {}",
                        abandoned.size(), abandoned);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Could not reconcile stale backup runs", e);
        }
    }

    private void flushPendingUpserts() {
        // Batch upsert all Heimdall_Backup_Config__c records that were prepared during processing
        List<se.devrandom.heimdall.salesforce.objects.Heimdall_Backup_Config__c> configs =
            new java.util.ArrayList<>(salesforceService.getObjectBackup__cMap().values());

        if (!configs.isEmpty()) {
            log.info("Flushing {} pending Heimdall_Backup_Config__c upserts", configs.size());
            salesforceService.batchUpsertHeimdall_Backup_Config__c(configs);
        }
    }

    /**
     * 0 = clean run; 1 = the batch job itself failed or stopped; 2 = the job completed but at least one
     * object failed, stopped early, or was skipped because the API limit was reached.
     */
    static int exitCodeFor(BatchStatus status, boolean anyObjectFailed, boolean apiLimitReached) {
        if (status != BatchStatus.COMPLETED) {
            return 1;
        }
        return (anyObjectFailed || apiLimitReached) ? EXIT_COMPLETED_WITH_ERRORS : 0;
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        BatchStatus status = jobExecution.getStatus();
        boolean apiLimitReached = apiLimitTracker.map(ApiLimitTracker::isLimitReached).orElse(false);
        int exitCode = exitCodeFor(status, !statisticsService.getFailedObjects().isEmpty(), apiLimitReached);

        if (status == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");
            // Flush all pending Heimdall_Backup_Config__c upserts
            flushPendingUpserts();
        } else if (status == BatchStatus.FAILED) {
            log.error("!!! JOB FAILED! Check logs for errors");
        } else {
            log.error("!!! JOB ENDED WITH STATUS {} - treating it as a failure", status);
        }

        // Mark job as complete (even for failed jobs) and generate summary report
        statisticsService.markJobComplete();
        String summary = statisticsService.generateSummaryReport();

        log.info("\n" + "=".repeat(80));
        log.info(status == BatchStatus.COMPLETED ? "BACKUP JOB SUMMARY" : "BACKUP JOB SUMMARY (" + status + ")");
        log.info("=".repeat(80));
        log.info(summary);
        apiLimitTracker.ifPresent(tracker -> {
            tracker.logCurrentUsage();
            if (tracker.isLimitReached()) {
                log.warn("API LIMIT WAS REACHED during this run - some objects may have been skipped");
            }
        });
        if (exitCode == EXIT_COMPLETED_WITH_ERRORS) {
            log.warn("Job completed with errors - exiting with code {}", EXIT_COMPLETED_WITH_ERRORS);
        }
        log.info("=".repeat(80));

        // In web mode, don't exit - keep the web server running
        if (isWebMode()) {
            log.info("Web mode active - keeping server running for restore GUI");
        } else {
            shutdown(exitCode);
        }
    }

    /** Schedule shutdown after Spring Batch has finished updating job metadata. */
    private void shutdown(int exitCode) {
        new Thread(() -> {
            try {
                Thread.sleep(1000); // Wait for Spring Batch to finish updating metadata
                try {
                    rdsLifecycleService.ifPresent(RdsLifecycleService::stopIfWeStarted);
                } catch (Exception e) {
                    log.warn("Failed to stop RDS: {}", e.getMessage());
                }
                log.info("Shutting down application with exit code {}...", exitCode);
                System.exit(SpringApplication.exit(applicationContext, () -> exitCode));
            } catch (InterruptedException e) {
                log.error("Shutdown interrupted", e);
            }
        }).start();
    }
}
