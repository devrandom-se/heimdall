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
import se.devrandom.heimdall.salesforce.SalesforceService;
import se.devrandom.heimdall.storage.BackupStatisticsService;
import se.devrandom.heimdall.storage.RdsLifecycleService;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;

@Component
@ConditionalOnProperty(name = "spring.batch.job.enabled", havingValue = "true", matchIfMissing = true)
public class JobCompletionNotificationListener implements JobExecutionListener {
    private static final Logger log = LoggerFactory.getLogger(JobCompletionNotificationListener.class);

    private final SalesforceService salesforceService;
    private final ApplicationContext applicationContext;
    private final BackupStatisticsService statisticsService;
    private final Environment environment;
    private final Optional<RdsLifecycleService> rdsLifecycleService;

    public JobCompletionNotificationListener(SalesforceService salesforceService,
                                            ApplicationContext applicationContext,
                                            BackupStatisticsService statisticsService,
                                            Environment environment,
                                            Optional<RdsLifecycleService> rdsLifecycleService) {
        this.salesforceService = salesforceService;
        this.applicationContext = applicationContext;
        this.statisticsService = statisticsService;
        this.environment = environment;
        this.rdsLifecycleService = rdsLifecycleService;
    }

    private boolean isWebMode() {
        return Arrays.asList(environment.getActiveProfiles()).contains("web");
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        log.info("Starting job");
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

    @Override
    public void afterJob(JobExecution jobExecution) {
        if(jobExecution.getStatus() == BatchStatus.COMPLETED) {
            log.info("!!! JOB FINISHED! Time to verify the results");

            // Flush all pending Heimdall_Backup_Config__c upserts
            flushPendingUpserts();

            // Mark job as complete in statistics and generate summary report
            statisticsService.markJobComplete();
            String summary = statisticsService.generateSummaryReport();

            // Print comprehensive statistics summary
            log.info("\n" + "=".repeat(80));
            log.info("BACKUP JOB SUMMARY");
            log.info("=".repeat(80));
            log.info(summary);
            log.info("=".repeat(80));

            // In web mode, don't exit - keep the web server running
            if (isWebMode()) {
                log.info("Web mode active - keeping server running for restore GUI");
            } else {
                // Schedule shutdown after Spring Batch has finished updating job metadata
                new Thread(() -> {
                    try {
                        Thread.sleep(1000); // Wait for Spring Batch to finish updating metadata
                        try {
                            rdsLifecycleService.ifPresent(RdsLifecycleService::stopIfWeStarted);
                        } catch (Exception e) {
                            log.warn("Failed to stop RDS: {}", e.getMessage());
                        }
                        log.info("Shutting down application...");
                        int exitCode = SpringApplication.exit(applicationContext, () -> 0);
                        System.exit(exitCode);
                    } catch (InterruptedException e) {
                        log.error("Shutdown interrupted", e);
                    }
                }).start();
            }
        } else if(jobExecution.getStatus() == BatchStatus.FAILED) {
            log.error("!!! JOB FAILED! Check logs for errors");

            // Mark job as complete (even for failed jobs) and generate summary
            statisticsService.markJobComplete();
            String summary = statisticsService.generateSummaryReport();

            // Print summary even for failed jobs
            log.info("\n" + "=".repeat(80));
            log.info("BACKUP JOB SUMMARY (FAILED)");
            log.info("=".repeat(80));
            log.info(summary);
            log.info("=".repeat(80));

            // In web mode, don't exit - keep the web server running
            if (isWebMode()) {
                log.info("Web mode active - keeping server running for restore GUI despite job failure");
            } else {
                // Schedule shutdown with error code
                new Thread(() -> {
                    try {
                        Thread.sleep(1000); // Wait for Spring Batch to finish updating metadata
                        try {
                            rdsLifecycleService.ifPresent(RdsLifecycleService::stopIfWeStarted);
                        } catch (Exception e) {
                            log.warn("Failed to stop RDS: {}", e.getMessage());
                        }
                        log.info("Shutting down application with error status...");
                        int exitCode = SpringApplication.exit(applicationContext, () -> 1);
                        System.exit(exitCode);
                    } catch (InterruptedException e) {
                        log.error("Shutdown interrupted", e);
                    }
                }).start();
            }
        }
    }
}
