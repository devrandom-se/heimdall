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
package se.devrandom.heimdall.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 * Utility class for executing operations with retry logic and exponential backoff.
 * Only retries transient errors (network issues, 5xx errors, S3 throttling).
 * Does not retry client errors (4xx) or application-level failures.
 */
public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);

    /**
     * Executes the given operation with retry logic and exponential backoff.
     *
     * @param operation      The operation to execute
     * @param maxAttempts    Maximum number of attempts (e.g., 3)
     * @param initialDelayMs Initial delay in milliseconds (e.g., 1000 for 1 second)
     * @param operationName  Name of the operation for logging purposes
     * @param <T>            Return type of the operation
     * @return The result from the operation
     * @throws Exception if all retry attempts are exhausted or a non-retryable error occurs
     */
    public static <T> T executeWithRetry(
            Callable<T> operation,
            int maxAttempts,
            long initialDelayMs,
            String operationName) throws Exception {

        Exception lastException = null;

        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                return operation.call();
            } catch (Exception e) {
                lastException = e;

                // Check if we've exhausted all attempts
                if (attempt == maxAttempts) {
                    log.error("{} failed after {} attempts", operationName, maxAttempts, e);
                    throw e;
                }

                // Check if error is retryable
                if (!isRetryable(e)) {
                    log.error("{} failed with non-retryable error: {}",
                        operationName, e.getClass().getSimpleName(), e);
                    throw e;
                }

                // Calculate exponential backoff delay: 1s, 2s, 4s, 8s, ...
                long delayMs = initialDelayMs * (1L << (attempt - 1));

                log.warn("{} attempt {}/{} failed, retrying in {}ms: {}",
                    operationName, attempt, maxAttempts, delayMs, e.getMessage());

                // Wait before retrying
                try {
                    Thread.sleep(delayMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Retry interrupted", ie);
                }
            }
        }

        // Should not reach here, but throw last exception just in case
        throw lastException;
    }

    /**
     * Determines if an exception is retryable (transient error) or not.
     *
     * Retryable errors:
     * - IOException (network timeouts, connection failures)
     * - WebClientResponseException with 5xx status (server errors)
     * - S3Exception (throttling, temporary failures)
     *
     * Non-retryable errors:
     * - WebClientResponseException with 4xx status (client errors: 401, 404, etc.)
     * - Other application-level exceptions
     *
     * @param e The exception to check
     * @return true if the error is retryable, false otherwise
     */
    private static boolean isRetryable(Exception e) {
        // Network and I/O errors are always retryable
        if (e instanceof IOException) {
            return true;
        }

        // WebClient HTTP errors - retry only 5xx server errors
        if (e instanceof WebClientResponseException) {
            WebClientResponseException webEx = (WebClientResponseException) e;
            int statusCode = webEx.getStatusCode().value();

            // 5xx server errors are retryable (500, 502, 503, etc.)
            if (statusCode >= 500 && statusCode < 600) {
                return true;
            }

            // 429 Too Many Requests is retryable (rate limiting)
            if (statusCode == 429) {
                return true;
            }

            // 4xx client errors are NOT retryable (401, 403, 404, etc.)
            if (statusCode >= 400 && statusCode < 500) {
                log.debug("Non-retryable HTTP {} error: {}", statusCode, webEx.getMessage());
                return false;
            }
        }

        // S3 errors - retry throttling and temporary failures
        if (e instanceof S3Exception) {
            S3Exception s3Ex = (S3Exception) e;
            String errorCode = s3Ex.awsErrorDetails() != null
                ? s3Ex.awsErrorDetails().errorCode()
                : null;

            // Retry S3 throttling and temporary errors
            if ("SlowDown".equals(errorCode) ||
                "RequestTimeout".equals(errorCode) ||
                "ServiceUnavailable".equals(errorCode) ||
                "InternalError".equals(errorCode)) {
                return true;
            }

            // Other S3 errors (e.g., AccessDenied, NoSuchBucket) are not retryable
            log.debug("Non-retryable S3 error: {} ({})", errorCode, s3Ex.getMessage());
            return false;
        }

        // RuntimeException wrapping retryable exceptions
        if (e instanceof RuntimeException && e.getCause() != null) {
            Throwable cause = e.getCause();
            if (cause instanceof IOException ||
                cause instanceof WebClientResponseException ||
                cause instanceof S3Exception) {
                // Recursively check the cause
                return isRetryable((Exception) cause);
            }
        }

        // Unknown exceptions are not retryable by default (fail-fast)
        log.debug("Non-retryable exception type: {}", e.getClass().getName());
        return false;
    }
}
