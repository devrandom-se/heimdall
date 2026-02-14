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

import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rds.RdsClient;
import software.amazon.awssdk.services.rds.model.*;

@Service
@ConditionalOnExpression("!'${rds.instance-identifier:}'.isEmpty()")
public class RdsLifecycleService {
    private static final Logger log = LoggerFactory.getLogger(RdsLifecycleService.class);

    private static final int POLL_INTERVAL_MS = 15_000;
    private static final int MAX_WAIT_MS = 15 * 60 * 1000;

    private final RdsClient rdsClient;
    private final String instanceIdentifier;
    private volatile boolean weStartedRds = false;

    public RdsLifecycleService(
            @org.springframework.beans.factory.annotation.Value("${rds.instance-identifier}") String instanceIdentifier,
            @org.springframework.beans.factory.annotation.Value("${aws.s3.region:eu-north-1}") String region) {
        this.instanceIdentifier = instanceIdentifier;
        this.rdsClient = RdsClient.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();
        log.info("RdsLifecycleService initialized for instance: {}", instanceIdentifier);
    }

    public void ensureAvailable() {
        String status = getInstanceStatus();
        log.info("RDS instance {} status: {}", instanceIdentifier, status);

        if ("available".equals(status)) {
            return;
        }

        if ("stopped".equals(status)) {
            log.info("RDS is stopped, starting...");
            rdsClient.startDBInstance(StartDbInstanceRequest.builder()
                    .dbInstanceIdentifier(instanceIdentifier)
                    .build());
            weStartedRds = true;
            log.info("StartDBInstance issued, waiting for available status...");
        } else if ("starting".equals(status) || "configuring-enhanced-monitoring".equals(status)) {
            log.info("RDS is already starting, waiting for available status...");
        } else if ("stopping".equals(status)) {
            log.info("RDS is stopping, waiting for it to stop before starting...");
            waitForStatus("stopped");
            log.info("RDS stopped, now starting...");
            rdsClient.startDBInstance(StartDbInstanceRequest.builder()
                    .dbInstanceIdentifier(instanceIdentifier)
                    .build());
            weStartedRds = true;
        } else {
            log.warn("RDS is in unexpected status '{}', attempting to wait for available...", status);
        }

        waitForStatus("available");
        log.info("RDS instance {} is available", instanceIdentifier);
    }

    public void stopIfWeStarted() {
        if (!weStartedRds) {
            log.info("RDS was not started by us, skipping stop");
            return;
        }

        String status = getInstanceStatus();
        if (!"available".equals(status)) {
            log.info("RDS is in status '{}', skipping stop", status);
            return;
        }

        log.info("Stopping RDS instance {} (we started it)...", instanceIdentifier);
        try {
            rdsClient.stopDBInstance(StopDbInstanceRequest.builder()
                    .dbInstanceIdentifier(instanceIdentifier)
                    .build());
            log.info("StopDBInstance issued (fire-and-forget)");
        } catch (Exception e) {
            log.warn("Failed to stop RDS instance: {}", e.getMessage());
        }
    }

    @PreDestroy
    public void onShutdown() {
        stopIfWeStarted();
    }

    private String getInstanceStatus() {
        DescribeDbInstancesResponse response = rdsClient.describeDBInstances(
                DescribeDbInstancesRequest.builder()
                        .dbInstanceIdentifier(instanceIdentifier)
                        .build());
        return response.dbInstances().get(0).dbInstanceStatus();
    }

    private void waitForStatus(String targetStatus) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < MAX_WAIT_MS) {
            try {
                Thread.sleep(POLL_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while waiting for RDS status", e);
            }

            String current = getInstanceStatus();
            log.info("RDS status: {} (waiting for {})", current, targetStatus);
            if (targetStatus.equals(current)) {
                return;
            }
        }
        throw new RuntimeException("Timed out waiting for RDS instance " + instanceIdentifier +
                " to reach status '" + targetStatus + "'");
    }
}
