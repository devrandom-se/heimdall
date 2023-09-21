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
package se.devrandom.heimdall.salesforce;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import se.devrandom.heimdall.storage.BackupStatisticsService;
import se.devrandom.heimdall.storage.S3Service;
import se.devrandom.heimdall.util.RetryUtil;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Task for downloading a single ContentVersion from Salesforce and streaming it to S3.
 * Uses in-memory buffering to avoid temp disk files (acceptable since WebClient has 16MB limit).
 * Implements retry logic for transient failures with exponential backoff.
 */
public class ContentVersionDownloadTask implements Callable<ContentVersionResult> {
    private static final Logger log = LoggerFactory.getLogger(ContentVersionDownloadTask.class);

    private final String recordId;
    private final String checksum;
    private final String fileExtension;
    private final LocalDateTime createdDate;
    private final boolean isDeleted;
    private final long contentSize;

    private final WebClient webClient;
    private final String salesforceAccessToken;
    private final String salesforceApiVersion;
    private final S3Service s3Service;
    private final BackupStatisticsService statisticsService;
    private final int skipFilesBelowKb;
    private final Set<String> globalChecksumCache;

    public ContentVersionDownloadTask(
            String recordId,
            String checksum,
            String fileExtension,
            LocalDateTime createdDate,
            boolean isDeleted,
            long contentSize,
            WebClient webClient,
            String salesforceAccessToken,
            String salesforceApiVersion,
            S3Service s3Service,
            BackupStatisticsService statisticsService,
            int skipFilesBelowKb,
            Set<String> globalChecksumCache) {
        this.recordId = recordId;
        this.checksum = checksum;
        this.fileExtension = fileExtension;
        this.createdDate = createdDate;
        this.isDeleted = isDeleted;
        this.contentSize = contentSize;
        this.webClient = webClient;
        this.salesforceAccessToken = salesforceAccessToken;
        this.salesforceApiVersion = salesforceApiVersion;
        this.s3Service = s3Service;
        this.statisticsService = statisticsService;
        this.skipFilesBelowKb = skipFilesBelowKb;
        this.globalChecksumCache = globalChecksumCache;
    }

    @Override
    public ContentVersionResult call() {
        // NOTE: Global cache already checked in SalesforceService before task creation

        // 1. Size filter
        if (skipFilesBelowKb > 0 && contentSize > 0) {
            long sizeInKb = contentSize / 1024;
            if (sizeInKb < skipFilesBelowKb) {
                log.debug("Skipping {} - too small ({} KB < {} KB threshold)",
                        recordId, sizeInKb, skipFilesBelowKb);
                statisticsService.incrementContentVersionSkipped();
                statisticsService.incrementContentVersionSkippedSize();
                return ContentVersionResult.skippedSize(recordId);
            }
        }

        // 2. Check S3 (file might exist from previous backup period)
        if (s3Service.checksumExistsInS3(checksum, fileExtension)) {
            log.debug("Skipping {} - checksum {} already exists in S3", recordId, checksum);
            statisticsService.incrementContentVersionSkipped();
            statisticsService.incrementContentVersionSkippedDuplicate();
            if (contentSize > 0) {
                statisticsService.addBytesDeduped(contentSize);
            }

            // Add to cache - found in S3!
            String checksumKey = checksum + "." + fileExtension;
            globalChecksumCache.add(checksumKey);

            return ContentVersionResult.skippedDuplicate(recordId, checksum, fileExtension);
        }

        // 3. Download and upload (new file)
        try {
            RetryUtil.executeWithRetry(() -> {
                downloadAndUploadStreaming();
                return null;
            }, 3, 1000, "ContentVersion " + recordId);

            statisticsService.incrementContentVersionDownloaded();

            // Add to cache - successfully uploaded!
            String checksumKey = checksum + "." + fileExtension;
            globalChecksumCache.add(checksumKey);

            log.debug("Successfully processed ContentVersion {}", recordId);
            return ContentVersionResult.success(recordId, checksum, fileExtension);

        } catch (Exception e) {
            log.error("Failed to process ContentVersion {} after retries: {}", recordId, e.getMessage());
            statisticsService.incrementContentVersionFailed(recordId);
            return ContentVersionResult.failed(recordId, e.getMessage());
        }
    }

    /**
     * Downloads ContentVersion from Salesforce and streams directly to S3.
     * Uses in-memory buffering (acceptable since WebClient has 16MB limit).
     * This approach is simpler and more reliable than piped streams.
     */
    private void downloadAndUploadStreaming() throws Exception {
        String uri = String.format("/services/data/%s/sobjects/ContentVersion/%s/VersionData",
                salesforceApiVersion, recordId);

        log.debug("Downloading ContentVersion {} from Salesforce", recordId);

        // Download from Salesforce to memory
        byte[] fileData;
        try {
            Flux<DataBuffer> dataBufferFlux = webClient
                    .get()
                    .uri(uri)
                    .headers(headers -> headers.setBearerAuth(salesforceAccessToken))
                    .retrieve()
                    .bodyToFlux(DataBuffer.class);

            // Collect all data buffers into single DataBuffer, then extract bytes
            DataBuffer dataBuffer = DataBufferUtils.join(dataBufferFlux).block();
            if (dataBuffer == null) {
                throw new IOException("No data received from Salesforce for " + recordId);
            }

            // Extract bytes from DataBuffer
            fileData = new byte[dataBuffer.readableByteCount()];
            dataBuffer.read(fileData);
            DataBufferUtils.release(dataBuffer);

            log.debug("Downloaded {} bytes for ContentVersion {}", fileData.length, recordId);

        } catch (Exception e) {
            throw new IOException("Failed to download ContentVersion from Salesforce: " + e.getMessage(), e);
        }

        // Upload to S3 using checksum-based key
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(fileData)) {
            s3Service.uploadContentVersionFileStreamingByChecksum(
                    inputStream,
                    fileData.length,
                    checksum,
                    fileExtension,
                    recordId
            );

            log.debug("Uploaded {} bytes to S3 (checksum-based) for ContentVersion {}", fileData.length, recordId);

        } catch (Exception e) {
            throw new IOException("Failed to upload ContentVersion to S3: " + e.getMessage(), e);
        }

        // Update statistics
        statisticsService.addBytesTransferred(fileData.length);
    }
}

/**
 * Result of ContentVersion download task
 */
class ContentVersionResult {
    enum Status {
        SUCCESS,           // Downloaded and uploaded successfully
        SKIPPED,           // Already exists in S3 with matching checksum (old path)
        SKIPPED_DUPLICATE, // Deduplicated - checksum already exists
        SKIPPED_SIZE,      // Too small (size filter)
        FAILED             // Failed after all retry attempts
    }

    private final Status status;
    private final String recordId;
    private final String errorMessage;
    private final String checksum;
    private final String fileExtension;

    private ContentVersionResult(Status status, String recordId, String errorMessage,
                                String checksum, String fileExtension) {
        this.status = status;
        this.recordId = recordId;
        this.errorMessage = errorMessage;
        this.checksum = checksum;
        this.fileExtension = fileExtension;
    }

    public static ContentVersionResult success(String recordId, String checksum, String fileExtension) {
        return new ContentVersionResult(Status.SUCCESS, recordId, null, checksum, fileExtension);
    }

    public static ContentVersionResult skipped(String recordId) {
        return new ContentVersionResult(Status.SKIPPED, recordId, null, null, null);
    }

    public static ContentVersionResult skippedDuplicate(String recordId, String checksum, String fileExtension) {
        return new ContentVersionResult(Status.SKIPPED_DUPLICATE, recordId, null, checksum, fileExtension);
    }

    public static ContentVersionResult skippedSize(String recordId) {
        return new ContentVersionResult(Status.SKIPPED_SIZE, recordId, null, null, null);
    }

    public static ContentVersionResult failed(String recordId, String errorMessage) {
        return new ContentVersionResult(Status.FAILED, recordId, errorMessage, null, null);
    }

    public Status status() {
        return status;
    }

    public String recordId() {
        return recordId;
    }

    public String errorMessage() {
        return errorMessage;
    }

    public String checksum() {
        return checksum;
    }

    public String fileExtension() {
        return fileExtension;
    }
}
