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
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Flux;
import se.devrandom.heimdall.salesforce.objects.BulkQueryRequest;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.function.ToIntFunction;

/**
 * Downloads the result set of a finished Bulk API 2.0 query into one local CSV file.
 *
 * Results arrive in chunks (Sforce-Locator paging). Each chunk is written to its own temp file and
 * validated (parseable last row with a well-formed Id, row count equal to Sforce-NumberOfRecords)
 * before it is folded into the base file. A chunk that fails validation is re-requested; because it
 * never touched the base file, a retry can neither duplicate rows nor lose the base file.
 */
public class BulkQueryResultDownloader {
    private static final Logger log = LoggerFactory.getLogger(BulkQueryResultDownloader.class);
    private static final int MAX_RECORDS = 2000;
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long DEFAULT_RETRY_DELAY_MS = 5000;

    private final WebClient webClient;
    private final String apiVersion;
    private final Supplier<String> accessToken;
    private final Optional<ApiLimitTracker> apiLimitTracker;
    private final ToIntFunction<String> batchSizeLookup;
    private final Path tempDir;
    private final long retryDelayMs;

    public BulkQueryResultDownloader(WebClient webClient, String apiVersion, Supplier<String> accessToken,
                                     Optional<ApiLimitTracker> apiLimitTracker,
                                     ToIntFunction<String> batchSizeLookup, Path tempDir) {
        this(webClient, apiVersion, accessToken, apiLimitTracker, batchSizeLookup, tempDir, DEFAULT_RETRY_DELAY_MS);
    }

    BulkQueryResultDownloader(WebClient webClient, String apiVersion, Supplier<String> accessToken,
                              Optional<ApiLimitTracker> apiLimitTracker,
                              ToIntFunction<String> batchSizeLookup, Path tempDir, long retryDelayMs) {
        this.webClient = webClient;
        this.apiVersion = apiVersion;
        this.accessToken = accessToken;
        this.apiLimitTracker = apiLimitTracker;
        this.batchSizeLookup = batchSizeLookup;
        this.tempDir = tempDir;
        this.retryDelayMs = retryDelayMs;
    }

    static class CsvLastRow {
        String lastId;
        java.util.Date lastSystemModstamp;
        int rowCount;
        boolean valid = true;
        String validationError;
    }

    /**
     * Exception thrown when CSV validation fails (truncated data, missing Id, etc.)
     */
    public static class CsvValidationException extends RuntimeException {
        public CsvValidationException(String message) {
            super(message);
        }
    }

    /**
     * Download query results from Bulk API without processing to storage.
     * Downloads and validates each CSV chunk, concatenates them into one file and extracts checkpoint info.
     *
     * @param bulkQueryRequest The bulk query request (must be finished)
     * @param initialRecordCount Starting record count (for resuming)
     * @return CsvDownloadResult with CSV path and checkpoint info
     */
    public CsvDownloadResult download(BulkQueryRequest bulkQueryRequest, int initialRecordCount) {
        AtomicReference<String> sforceLocator = new AtomicReference<>("");
        AtomicReference<Boolean> downloading = new AtomicReference<>(true);
        AtomicReference<Integer> sforceNumberOfRecords = new AtomicReference<>(-1);
        int totalRecords = initialRecordCount;
        boolean isQueryAll = "queryAll".equals(bulkQueryRequest.operation);
        String objectName = bulkQueryRequest.object;

        CsvDownloadResult result = new CsvDownloadResult();
        result.objectName = objectName;
        result.isQueryAll = isQueryAll;
        result.recordCount = 0;

        Path baseFilePath = tempDir.resolve(bulkQueryRequest.id + ".csv");
        int chunkIndex = 0;
        Path chunkPath = null;

        try {
            while (downloading.get()) {
                String sforceLocatorString = !sforceLocator.get().isEmpty() ? "&locator=" + sforceLocator.get() : "";
                String currentLocator = sforceLocator.get(); // Saved so a failed chunk can be re-requested
                chunkPath = tempDir.resolve(bulkQueryRequest.id + ".csv.chunk" + chunkIndex);

                // Retry loop for chunk download and validation
                CsvLastRow lastRowInfo = null;
                for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
                    try {
                        Flux<DataBuffer> dataBufferFlux = webClient
                                .get()
                                .uri(String.format("/services/data/%s/jobs/query/%s/results?maxRecords=%d%s",
                                        apiVersion,
                                        bulkQueryRequest.id,
                                        MAX_RECORDS,
                                        sforceLocatorString))
                                .headers(httpHeaders -> httpHeaders.setBearerAuth(accessToken.get()))
                                .accept(MediaType.APPLICATION_JSON)
                                .exchangeToFlux(response -> {
                                    if (response.statusCode().equals(HttpStatus.OK)) {
                                        final String headerSforceLocator = response.headers().header("Sforce-Locator").get(0);
                                        if (headerSforceLocator.equals("null")) {
                                            downloading.set(false);
                                        } else {
                                            sforceLocator.set(headerSforceLocator);
                                        }
                                        // Capture Sforce-NumberOfRecords header for validation
                                        // Note: Header name case changed with Hyperforce migration, so we check multiple variants
                                        String numRecordsValue = null;
                                        for (String headerName : List.of("Sforce-NumberOfRecords", "sforce-numberofrecords", "SFORCE-NUMBEROFRECORDS")) {
                                            List<String> headerValues = response.headers().header(headerName);
                                            if (!headerValues.isEmpty()) {
                                                numRecordsValue = headerValues.get(0);
                                                break;
                                            }
                                        }
                                        if (numRecordsValue != null) {
                                            try {
                                                sforceNumberOfRecords.set(Integer.parseInt(numRecordsValue));
                                            } catch (NumberFormatException e) {
                                                log.warn("Failed to parse Sforce-NumberOfRecords header: {}", numRecordsValue);
                                            }
                                        }
                                        // Update API limit tracker from response header
                                        apiLimitTracker.ifPresent(tracker -> {
                                            List<String> limitHeader = response.headers().header("Sforce-Limit-Info");
                                            if (!limitHeader.isEmpty()) {
                                                tracker.updateFromHeader(limitHeader.get(0));
                                            }
                                        });
                                        return response.bodyToFlux(DataBuffer.class);
                                    }
                                    // Non-2xx: surface status, reason and body instead of an empty RuntimeException
                                    return response.createException().flatMapMany(Flux::error);
                                });

                        Files.deleteIfExists(chunkPath);
                        log.info("Writing chunk {} to {}", chunkIndex, chunkPath);
                        DataBufferUtils.write(dataBufferFlux, chunkPath, StandardOpenOption.CREATE).block();

                        // Extract checkpoint info from last row and validate CSV integrity
                        lastRowInfo = extractLastRowInfo(chunkPath, isQueryAll);

                        // VALIDATION 1: Check that CSV parsing succeeded and Id was valid
                        if (!lastRowInfo.valid) {
                            throw new CsvValidationException(String.format(
                                "CSV validation failed for %s: %s", objectName, lastRowInfo.validationError));
                        }

                        // VALIDATION 2: Check that row count matches Sforce-NumberOfRecords header
                        int expectedRecords = sforceNumberOfRecords.get();
                        if (expectedRecords >= 0 && lastRowInfo.rowCount != expectedRecords) {
                            throw new CsvValidationException(String.format(
                                "CSV record count mismatch for %s: expected %d (from Sforce-NumberOfRecords header), got %d. File may be truncated.",
                                objectName, expectedRecords, lastRowInfo.rowCount));
                        }

                        log.debug("CSV validated: {} records match Sforce-NumberOfRecords header", lastRowInfo.rowCount);
                        break; // Success - exit retry loop

                    } catch (CsvValidationException e) {
                        if (attempt == MAX_RETRY_ATTEMPTS) {
                            log.error("CSV validation failed after {} attempts for {}: {}", MAX_RETRY_ATTEMPTS, objectName, e.getMessage());
                            throw e;
                        }
                        log.warn("CSV validation failed for {} (attempt {}/{}), retrying in {} ms: {}",
                            objectName, attempt, MAX_RETRY_ATTEMPTS, retryDelayMs, e.getMessage());
                        // Reset locator to retry same chunk; the base file is untouched
                        sforceLocator.set(currentLocator);
                        sforceNumberOfRecords.set(-1);
                        Files.deleteIfExists(chunkPath);
                        try { Thread.sleep(retryDelayMs); } catch (InterruptedException ignored) { Thread.currentThread().interrupt(); }
                    }
                }

                // Only a validated chunk reaches the base file
                if (chunkIndex == 0) {
                    Files.move(chunkPath, baseFilePath, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    log.info("Concatenating {} to {}", chunkPath, baseFilePath);
                    concatenateCsvFiles(baseFilePath, chunkPath);
                    Files.delete(chunkPath);
                }
                chunkIndex++;

                totalRecords += lastRowInfo.rowCount;

                if (lastRowInfo.lastId != null) {
                    result.lastId = lastRowInfo.lastId;
                    result.lastModstamp = lastRowInfo.lastSystemModstamp != null
                        ? new java.sql.Timestamp(lastRowInfo.lastSystemModstamp.getTime())
                        : null;
                    log.info("Downloaded {} records, lastId={}, lastModstamp={}",
                            totalRecords, lastRowInfo.lastId, lastRowInfo.lastSystemModstamp);
                }

                // Reset for next chunk
                sforceNumberOfRecords.set(-1);
            }
        } catch (IOException e) {
            deleteQuietly(baseFilePath);
            deleteQuietly(chunkPath);
            throw new RuntimeException("CSV assembly failed for " + objectName + ": " + e.getMessage(), e);
        } catch (WebClientResponseException e) {
            deleteQuietly(baseFilePath);
            deleteQuietly(chunkPath);
            throw new RuntimeException("Bulk API result download failed for " + objectName + ": " + e.getMessage()
                    + " - " + e.getResponseBodyAsString(), e);
        } catch (RuntimeException e) {
            deleteQuietly(baseFilePath);
            deleteQuietly(chunkPath);
            throw e;
        }

        // Set result fields
        result.csvPath = baseFilePath;
        result.recordCount = totalRecords - initialRecordCount;  // Records in THIS batch
        // hasMoreRecords is true if we got a full batch
        int batchSize = batchSizeLookup.applyAsInt(objectName);
        result.hasMoreRecords = result.recordCount >= batchSize;

        return result;
    }

    private static void deleteQuietly(Path path) {
        if (path == null) {
            return;
        }
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            log.warn("Failed to delete temporary file {}: {}", path, e.getMessage());
        }
    }

    /**
     * Concatenate CSV file by appending rows from source to destination (skipping header)
     */
    private static void concatenateCsvFiles(Path destination, Path source) throws IOException {
        try (var reader = Files.newBufferedReader(source);
             var writer = Files.newBufferedWriter(destination, StandardOpenOption.APPEND)) {

            // Skip header line from source file
            String headerLine = reader.readLine();
            if (headerLine == null) {
                log.warn("Source file {} is empty, skipping concatenation", source);
                return;
            }

            // Append all data rows to destination
            String line;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                writer.write(line);
                writer.newLine();
                lineCount++;
            }

            log.debug("Concatenated {} lines from {} to {}", lineCount, source, destination);
        }
    }

    static CsvLastRow extractLastRowInfo(Path csvPath, boolean isQueryAll) {
        CsvLastRow result = new CsvLastRow();
        result.rowCount = 0;

        try (BufferedReader reader = Files.newBufferedReader(csvPath, StandardCharsets.UTF_8)) {
            // Read header as a complete record
            String headerLine = readCsvRecord(reader);
            if (headerLine == null) {
                return result;
            }

            String[] headers = parseCsvLineSimple(headerLine);
            int idIndex = -1;
            int systemModstampIndex = -1;

            for (int i = 0; i < headers.length; i++) {
                if (headers[i].trim().equalsIgnoreCase("Id")) {
                    idIndex = i;
                }
                if (headers[i].trim().equalsIgnoreCase("SystemModstamp")) {
                    systemModstampIndex = i;
                }
            }

            if (idIndex < 0) {
                log.warn("Id column not found in CSV for {}", csvPath.getFileName());
            }
            if (systemModstampIndex < 0) {
                log.warn("SystemModstamp column not found in CSV for {}", csvPath.getFileName());
            }

            // Read all records to find the last one
            String lastRecord = null;
            String record;
            while ((record = readCsvRecord(reader)) != null) {
                if (!record.trim().isEmpty()) {
                    result.rowCount++;
                    lastRecord = record;
                }
            }

            if (lastRecord != null && idIndex >= 0) {
                String[] values = parseCsvLineSimple(lastRecord);

                // VALIDATION: Id column (now last in SOQL) must exist and not be empty
                // If CSV was truncated, the last column will be missing or empty
                if (idIndex >= values.length) {
                    result.valid = false;
                    result.validationError = String.format(
                        "CSV truncated: Id column index %d >= values length %d. Last row: %s",
                        idIndex, values.length, lastRecord.length() > 100 ? lastRecord.substring(0, 100) + "..." : lastRecord);
                    log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
                    return result;
                }

                result.lastId = values[idIndex].trim();

                // VALIDATION: Id must not be empty (indicates truncated data)
                if (result.lastId.isEmpty()) {
                    result.valid = false;
                    result.validationError = String.format(
                        "CSV truncated: Id column is empty on last row. Last row: %s",
                        lastRecord.length() > 100 ? lastRecord.substring(0, 100) + "..." : lastRecord);
                    log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
                    return result;
                }

                // VALIDATION: Id must look like a Salesforce ID (15 or 18 alphanumeric chars)
                if (!result.lastId.matches("^[a-zA-Z0-9]{15}([a-zA-Z0-9]{3})?$")) {
                    result.valid = false;
                    result.validationError = String.format(
                        "CSV corrupted: Invalid Id format '%s' on last row", result.lastId);
                    log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
                    return result;
                }

                log.debug("Extracted and validated lastId: {}", result.lastId);

                if (systemModstampIndex >= 0 && systemModstampIndex < values.length) {
                    String modstampStr = values[systemModstampIndex].trim();
                    try {
                        // Try multiple date formats
                        java.text.SimpleDateFormat formatter;
                        if (modstampStr.contains("+")) {
                            formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSXXX");
                        } else if (modstampStr.endsWith("Z")) {
                            formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
                        } else {
                            formatter = new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
                        }
                        formatter.setTimeZone(java.util.TimeZone.getTimeZone("UTC"));
                        result.lastSystemModstamp = formatter.parse(modstampStr);
                        log.debug("Extracted lastSystemModstamp: {}", result.lastSystemModstamp);
                    } catch (java.text.ParseException e) {
                        log.warn("Failed to parse SystemModstamp '{}': {}", modstampStr, e.getMessage());
                    }
                }
            } else if (result.rowCount > 0 && idIndex >= 0) {
                // We had records but couldn't extract last record - something is wrong
                result.valid = false;
                result.validationError = "Had " + result.rowCount + " records but lastRecord was null";
                log.error("CSV validation failed for {}: {}", csvPath.getFileName(), result.validationError);
            }
        } catch (IOException e) {
            log.error("Error reading CSV file {}: {}", csvPath, e.getMessage());
            result.valid = false;
            result.validationError = "IO error: " + e.getMessage();
        }

        return result;
    }

    /**
     * Reads a complete CSV record (which may span multiple lines if fields contain newlines)
     */
    static String readCsvRecord(BufferedReader reader) throws IOException {
        StringBuilder record = new StringBuilder();
        boolean inQuotes = false;
        int c;

        while ((c = reader.read()) != -1) {
            char ch = (char) c;

            if (ch == '"') {
                // Check for escaped quote ("")
                reader.mark(1);
                int next = reader.read();
                if (next == '"' && inQuotes) {
                    // Escaped quote - add both to record
                    record.append(ch).append((char) next);
                } else {
                    // Regular quote - toggle inQuotes
                    record.append(ch);
                    inQuotes = !inQuotes;
                    // Put back the character we read ahead
                    if (next != -1) {
                        reader.reset();
                    }
                }
            } else if (ch == '\n' && !inQuotes) {
                // End of record
                return record.toString();
            } else if (ch == '\r') {
                // Handle \r\n line endings - check if next is \n
                reader.mark(1);
                int next = reader.read();
                if (next == '\n' && !inQuotes) {
                    // End of record (Windows line ending)
                    return record.toString();
                } else {
                    // Not end of record, add \r to record
                    record.append(ch);
                    if (next != -1) {
                        reader.reset();
                    }
                }
            } else {
                record.append(ch);
            }
        }

        // End of file - return what we have if anything
        return record.length() > 0 ? record.toString() : null;
    }

    /**
     * Simple CSV parser that handles quoted fields and escaped quotes ("")
     */
    static String[] parseCsvLineSimple(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (c == '"') {
                // Check for escaped quote ("")
                if (inQuotes && i + 1 < line.length() && line.charAt(i + 1) == '"') {
                    // This is an escaped quote - add one quote to current value
                    current.append('"');
                    i++; // Skip the next quote
                } else {
                    // This is a field delimiter quote - toggle inQuotes
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
}
