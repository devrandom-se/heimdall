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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.retry.RetryPolicy;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.presigner.S3Presigner;
import software.amazon.awssdk.services.s3.presigner.model.GetObjectPresignRequest;
import software.amazon.awssdk.services.s3.presigner.model.PresignedGetObjectRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

@Service
public class S3Service {
    private static final Logger log = LoggerFactory.getLogger(S3Service.class);

    private final S3Client s3Client;
    private final S3Presigner s3Presigner;
    private final String bucketName;
    private final String orgId;

    public S3Service(
            @Value("${aws.s3.bucket-name}") String bucketName,
            @Value("${aws.s3.region:eu-north-1}") String region,
            @Value("${salesforce.org-id}") String orgId) {
        this.bucketName = bucketName;
        this.orgId = orgId;

        // Configure S3Client with timeouts to prevent hanging forever on slow/unreliable networks
        this.s3Client = S3Client.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(5))          // Total timeout for API call (includes retries)
                        .apiCallAttemptTimeout(Duration.ofMinutes(3))   // Timeout per retry attempt
                        .retryPolicy(RetryPolicy.builder()
                                .numRetries(2)                          // Retry twice on failure
                                .build())
                        .build())
                .build();

        this.s3Presigner = S3Presigner.builder()
                .region(Region.of(region))
                .credentialsProvider(DefaultCredentialsProvider.create())
                .build();

        log.info("S3Service initialized with bucket: {} in region: {} (5min timeout, 2 retries)", bucketName, region);
    }

    /**
     * Upload a CSV file to S3 with partitioned path pattern:
     * OrgId={x}/Period={yyyymm}/Year={y}/Month={m}/Date={d}/{ObjectName}/{file}.csv
     *
     * @param csvPath Path to the CSV file to upload
     * @param objectName Salesforce object name (e.g., "Account")
     * @param queryAll Whether this is a queryAll (deleted records) backup
     * @return The S3 key where the file was uploaded
     */
    public String uploadCsvToS3(Path csvPath, String objectName, boolean queryAll) throws IOException {
        String period = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMM"));
        return uploadCsvToS3(csvPath, objectName, queryAll, period);
    }

    /**
     * Upload a CSV file to S3 with partitioned path pattern and explicit period:
     * OrgId={x}/Period={period}/Year={y}/Month={m}/Date={d}/{ObjectName}/{file}.csv
     *
     * @param csvPath Path to the CSV file to upload
     * @param objectName Salesforce object name (e.g., "Account")
     * @param queryAll Whether this is a queryAll (deleted records) backup
     * @param periodOverride Period string to use in S3 key (e.g., "202602" or "Archive-202602")
     * @return The S3 key where the file was uploaded
     */
    public String uploadCsvToS3(Path csvPath, String objectName, boolean queryAll, String periodOverride) throws IOException {
        if (!Files.exists(csvPath)) {
            throw new IOException("CSV file does not exist: " + csvPath);
        }

        LocalDate now = LocalDate.now();
        String year = String.valueOf(now.getYear());
        String month = String.format("%02d", now.getMonthValue());
        String day = String.format("%02d", now.getDayOfMonth());

        // Build S3 key with partition pattern
        String fileName = csvPath.getFileName().toString();
        String suffix = queryAll ? "_deleted" : "";
        String key = String.format("OrgId=%s/Period=%s/Year=%s/Month=%s/Date=%s/%s%s/%s",
                orgId, periodOverride, year, month, day, objectName, suffix, fileName);

        log.info("Uploading CSV to S3: bucket={}, key={}, size={} bytes",
                bucketName, key, Files.size(csvPath));

        try {
            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType("text/csv")
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromFile(csvPath));
            log.info("Successfully uploaded CSV to S3: {}", key);
            return key;

        } catch (Exception e) {
            log.error("Failed to upload CSV to S3: {}", key, e);
            throw new IOException("S3 upload failed: " + e.getMessage(), e);
        }
    }

    /**
     * Get checksum from S3 object metadata
     * Returns null if file doesn't exist or has no checksum metadata
     */
    public String getFileChecksum(String s3Key) {
        try {
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            HeadObjectResponse headResponse = s3Client.headObject(headRequest);
            Map<String, String> metadata = headResponse.metadata();

            String checksum = metadata.get("checksum");
            log.debug("Retrieved checksum from S3 {}: {}", s3Key, checksum);
            return checksum;

        } catch (NoSuchKeyException e) {
            log.debug("File does not exist in S3: {}", s3Key);
            return null;
        } catch (Exception e) {
            log.warn("Failed to get checksum from S3 {}: {}", s3Key, e.getMessage());
            return null;
        }
    }

    /**
     * Upload ContentVersion file to S3 with checksum in metadata
     * Uses Period=0 and CreatedDate for path structure
     *
     * @param localFile Local file path
     * @param recordId ContentVersion Id
     * @param fileType File extension (e.g., "pdf", "jpg")
     * @param checksum MD5 checksum from Salesforce
     * @param createdDate CreatedDate from ContentVersion record
     * @return The S3 key where the file was uploaded
     */
    public String uploadContentVersionFile(Path localFile, String recordId, String fileType,
                                          String checksum, LocalDateTime createdDate) throws IOException {
        if (!Files.exists(localFile)) {
            throw new IOException("File does not exist: " + localFile);
        }

        // Build S3 key: Period=0, use CreatedDate for path
        String year = String.valueOf(createdDate.getYear());
        String month = String.format("%02d", createdDate.getMonthValue());
        String day = String.format("%02d", createdDate.getDayOfMonth());
        String fileName = recordId + "." + fileType;

        String key = String.format("OrgId=%s/Period=0/Year=%s/Month=%s/Date=%s/ContentVersionFile/%s",
                orgId, year, month, day, fileName);

        log.debug("Uploading ContentVersion file to S3: bucket={}, key={}, size={} bytes, checksum={}",
                bucketName, key, Files.size(localFile), checksum);

        try {
            // Add checksum and record ID to S3 object metadata
            Map<String, String> metadata = new HashMap<>();
            metadata.put("checksum", checksum);
            metadata.put("salesforce-id", recordId);
            metadata.put("upload-date", LocalDateTime.now().toString());

            // Determine content type based on file extension
            String contentType = getContentType(fileType);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(contentType)
                    .metadata(metadata)
                    .build();

            s3Client.putObject(putObjectRequest, RequestBody.fromFile(localFile));
            log.debug("Successfully uploaded ContentVersion file to S3: {}", key);
            return key;

        } catch (Exception e) {
            log.error("Failed to upload ContentVersion file to S3: {}", key, e);
            throw new IOException("S3 upload failed: " + e.getMessage(), e);
        }
    }

    /**
     * Upload ContentVersion file to S3 using streaming (no temp file)
     * Streams directly from InputStream to S3 for memory-efficient parallel uploads
     *
     * @param inputStream Stream of file data from Salesforce
     * @param contentLength File size in bytes (from Content-Length header)
     * @param recordId ContentVersion Id
     * @param fileType File extension (e.g., "pdf", "jpg")
     * @param checksum MD5 checksum from Salesforce
     * @param createdDate CreatedDate from ContentVersion record
     * @return The S3 key where the file was uploaded
     */
    public String uploadContentVersionFileStreaming(InputStream inputStream, long contentLength,
                                                   String recordId, String fileType,
                                                   String checksum, LocalDateTime createdDate) throws IOException {
        // Build S3 key using same pattern as uploadContentVersionFile
        String key = buildContentVersionS3Key(recordId, fileType, createdDate);

        log.debug("Uploading ContentVersion file (streaming) to S3: bucket={}, key={}, size={} bytes",
                bucketName, key, contentLength);

        try {
            // Add checksum and record ID to S3 object metadata
            Map<String, String> metadata = new HashMap<>();
            metadata.put("checksum", checksum);
            metadata.put("salesforce-id", recordId);
            metadata.put("upload-date", LocalDateTime.now().toString());

            // Determine content type based on file extension
            String contentType = getContentType(fileType);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(contentType)
                    .contentLength(contentLength)
                    .metadata(metadata)
                    .build();

            // Stream directly from InputStream (no temp file)
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
            log.debug("Successfully uploaded ContentVersion file (streaming) to S3: {}", key);
            return key;

        } catch (Exception e) {
            log.error("Failed to upload ContentVersion file (streaming) to S3: {}", key, e);
            throw new IOException("S3 streaming upload failed: " + e.getMessage(), e);
        }
    }

    /**
     * Build S3 key for ContentVersion file (without uploading)
     * Used to check if file exists before downloading from Salesforce
     */
    public String buildContentVersionS3Key(String recordId, String fileType, LocalDateTime createdDate) {
        String year = String.valueOf(createdDate.getYear());
        String month = String.format("%02d", createdDate.getMonthValue());
        String day = String.format("%02d", createdDate.getDayOfMonth());
        String fileName = recordId + "." + fileType;

        return String.format("OrgId=%s/Period=0/Year=%s/Month=%s/Date=%s/ContentVersionFile/%s",
                orgId, year, month, day, fileName);
    }

    /**
     * Build S3 key for ContentVersion file using checksum (content-addressable storage)
     * All files with same checksum share same S3 location for true deduplication
     * Uses hash bucketing (first 4 chars) to organize files into directories for better navigability
     *
     * @param checksum MD5 checksum from Salesforce
     * @param fileType File extension (e.g., "pdf", "jpg")
     * @return S3 key: OrgId=X/Period=0/files/{bucket}/{checksum}.{ext}
     *         Example: OrgId=X/Period=0/files/b896/b89618dc1c6772fa6c8b4a37d4214e80.pdf
     */
    public String buildContentVersionS3KeyByChecksum(String checksum, String fileType) {
        String fileName = checksum + "." + fileType;
        // Use first 4 characters as bucket prefix (e.g., "b896" from "b89618dc...")
        // With 1.7M files and 4-char hex bucketing (16^4 = 65,536 buckets), ~26 files per bucket
        String bucket = checksum.length() >= 4 ? checksum.substring(0, 4) : checksum;
        return String.format("OrgId=%s/Period=0/files/%s/%s", orgId, bucket, fileName);
    }

    /**
     * Check if a file with given checksum exists in S3
     * Used for deduplication - skip download if file already exists
     *
     * @param checksum MD5 checksum from Salesforce
     * @param fileType File extension (e.g., "pdf", "jpg")
     * @return true if file exists in S3, false otherwise
     */
    public boolean checksumExistsInS3(String checksum, String fileType) {
        String s3Key = buildContentVersionS3KeyByChecksum(checksum, fileType);
        try {
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();
            s3Client.headObject(headRequest);
            log.debug("File exists in S3: {}", s3Key);
            return true;
        } catch (NoSuchKeyException e) {
            log.debug("File does not exist in S3: {}", s3Key);
            return false;
        } catch (Exception e) {
            log.warn("Failed to check S3 for checksum {}: {}", s3Key, e.getMessage());
            return false;  // Assume doesn't exist on error
        }
    }

    /**
     * Upload ContentVersion file to S3 using checksum-based key (streaming, no temp file)
     * Enables content-addressable storage for true deduplication
     *
     * @param inputStream Stream of file data from Salesforce
     * @param contentLength File size in bytes
     * @param checksum MD5 checksum from Salesforce
     * @param fileType File extension (e.g., "pdf", "jpg")
     * @param recordId ContentVersion Id (stored in metadata only)
     * @return The S3 key where the file was uploaded
     */
    public String uploadContentVersionFileStreamingByChecksum(
            InputStream inputStream,
            long contentLength,
            String checksum,
            String fileType,
            String recordId) throws IOException {

        String key = buildContentVersionS3KeyByChecksum(checksum, fileType);

        log.debug("Uploading ContentVersion file (checksum-based) to S3: bucket={}, key={}, size={} bytes",
                bucketName, key, contentLength);

        try {
            // Add checksum and record ID to S3 object metadata
            Map<String, String> metadata = new HashMap<>();
            metadata.put("checksum", checksum);
            metadata.put("salesforce-id", recordId);
            metadata.put("upload-date", LocalDateTime.now().toString());

            // Determine content type based on file extension
            String contentType = getContentType(fileType);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(key)
                    .contentType(contentType)
                    .contentLength(contentLength)
                    .metadata(metadata)
                    .build();

            // Stream directly from InputStream (no temp file)
            s3Client.putObject(putObjectRequest, RequestBody.fromInputStream(inputStream, contentLength));
            log.debug("Successfully uploaded ContentVersion file (checksum-based) to S3: {}", key);
            return key;

        } catch (Exception e) {
            log.error("Failed to upload ContentVersion file (checksum-based) to S3: {}", key, e);
            throw new IOException("S3 streaming upload failed: " + e.getMessage(), e);
        }
    }

    /**
     * Determine MIME type based on file extension
     */
    private String getContentType(String fileExtension) {
        if (fileExtension == null) {
            return "application/octet-stream";
        }

        return switch (fileExtension.toLowerCase()) {
            case "pdf" -> "application/pdf";
            case "jpg", "jpeg" -> "image/jpeg";
            case "png" -> "image/png";
            case "gif" -> "image/gif";
            case "doc" -> "application/msword";
            case "docx" -> "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
            case "xls" -> "application/vnd.ms-excel";
            case "xlsx" -> "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
            case "ppt" -> "application/vnd.ms-powerpoint";
            case "pptx" -> "application/vnd.openxmlformats-officedocument.presentationml.presentation";
            case "txt" -> "text/plain";
            case "csv" -> "text/csv";
            case "json" -> "application/json";
            case "xml" -> "application/xml";
            case "zip" -> "application/zip";
            default -> "application/octet-stream";
        };
    }

    /**
     * Fetch a specific record from a CSV file stored in S3
     * Downloads the CSV and parses it to find the matching record
     *
     * @param s3Key The S3 key of the CSV file
     * @param recordId The Salesforce record ID to find
     * @return JSON string with the record data, or null if not found
     */
    public String selectRecordFromCsv(String s3Key, String recordId) {
        log.debug("Fetching record {} from CSV {} (streaming)", recordId, s3Key);
        try {
            GetObjectRequest getRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            try (var responseStream = s3Client.getObject(getRequest);
                 var reader = new BufferedReader(new InputStreamReader(responseStream, java.nio.charset.StandardCharsets.UTF_8))) {

                // Parse header line
                String headerLine = readCsvLine(reader);
                if (headerLine == null) {
                    return null; // Empty file
                }
                String[] headers = parseCsvLine(headerLine);

                // Find the Id column index
                int idIndex = -1;
                for (int i = 0; i < headers.length; i++) {
                    if ("Id".equalsIgnoreCase(headers[i].trim())) {
                        idIndex = i;
                        break;
                    }
                }

                if (idIndex == -1) {
                    log.warn("No 'Id' column found in CSV: {}", s3Key);
                    return null;
                }

                // Stream through rows until we find the matching record
                String line;
                while ((line = readCsvLine(reader)) != null) {
                    String[] values = parseCsvLine(line);
                    if (values.length > idIndex && recordId.equals(values[idIndex].trim())) {
                        // Build JSON object from headers and values
                        StringBuilder json = new StringBuilder("{");
                        for (int j = 0; j < headers.length && j < values.length; j++) {
                            if (j > 0) json.append(",");
                            json.append("\"").append(escapeJson(headers[j].trim())).append("\":");
                            json.append("\"").append(escapeJson(values[j])).append("\"");
                        }
                        json.append("}");
                        log.debug("Found record {} in CSV {}", recordId, s3Key);
                        return json.toString();
                    }
                }

                return null; // Record not found
            }

        } catch (Exception e) {
            log.error("Failed to fetch/parse CSV from S3: {}", e.getMessage());
            return null;
        }
    }

    /**
     * Read one CSV line from a stream, handling newlines inside quoted fields.
     * Returns null at EOF.
     */
    private String readCsvLine(BufferedReader reader) throws IOException {
        StringBuilder line = new StringBuilder();
        boolean inQuotes = false;
        int c;

        while ((c = reader.read()) != -1) {
            char ch = (char) c;

            if (ch == '"') {
                inQuotes = !inQuotes;
                line.append(ch);
            } else if (ch == '\n' && !inQuotes) {
                break;
            } else if (ch == '\r') {
                continue;
            } else {
                line.append(ch);
            }
        }

        if (line.length() == 0 && c == -1) {
            return null;
        }

        return line.toString();
    }

    /**
     * Simple CSV line parser that handles quoted fields
     */
    private String[] parseCsvLine(String line) {
        java.util.List<String> values = new java.util.ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c == '"') {
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    current.append('"');
                    i++; // Skip escaped quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (c == ',' && !inQuotes) {
                values.add(current.toString());
                current = new StringBuilder();
            } else {
                current.append(c);
            }
        }
        values.add(current.toString());

        return values.toArray(new String[0]);
    }

    /**
     * Escape special characters for JSON
     */
    private String escapeJson(String value) {
        if (value == null) return "";
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * Generate a presigned download URL for a ContentVersion file stored by checksum.
     *
     * @param checksum MD5 checksum from Salesforce
     * @param fileExtension File extension (e.g., "pdf", "jpg")
     * @param expiration How long the URL should be valid
     * @return Presigned URL string, or null if the file doesn't exist
     */
    public String generatePresignedDownloadUrl(String checksum, String fileExtension, Duration expiration) {
        String s3Key = buildContentVersionS3KeyByChecksum(checksum, fileExtension);

        try {
            // Verify the file exists first
            HeadObjectRequest headRequest = HeadObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();
            s3Client.headObject(headRequest);

            GetObjectRequest getObjectRequest = GetObjectRequest.builder()
                    .bucket(bucketName)
                    .key(s3Key)
                    .build();

            GetObjectPresignRequest presignRequest = GetObjectPresignRequest.builder()
                    .signatureDuration(expiration)
                    .getObjectRequest(getObjectRequest)
                    .build();

            PresignedGetObjectRequest presignedRequest = s3Presigner.presignGetObject(presignRequest);
            String url = presignedRequest.url().toString();
            log.info("Generated presigned URL for {}, expires in {}", s3Key, expiration);
            return url;

        } catch (NoSuchKeyException e) {
            log.warn("File not found in S3 for presigned URL: {}", s3Key);
            return null;
        } catch (Exception e) {
            log.error("Failed to generate presigned URL for {}: {}", s3Key, e.getMessage());
            return null;
        }
    }

    public String getBucketName() {
        return bucketName;
    }

    /**
     * Close the S3 client and presigner when the service is destroyed
     */
    public void close() {
        if (s3Presigner != null) {
            s3Presigner.close();
        }
        if (s3Client != null) {
            s3Client.close();
            log.info("S3Client closed");
        }
    }
}
