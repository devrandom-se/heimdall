package se.devrandom.heimdall.util;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.web.reactive.function.client.WebClientResponseException;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Level 1: Pure unit tests for RetryUtil.
 * Tests retry logic, backoff, and error classification without any Spring context.
 */
class RetryUtilTest {

    // ===== Successful call =====

    @Test
    void executeWithRetry_successOnFirstAttempt() throws Exception {
        String result = RetryUtil.executeWithRetry(
                () -> "success", 3, 10, "test-op");
        assertEquals("success", result);
    }

    @Test
    void executeWithRetry_successAfterRetries() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);

        String result = RetryUtil.executeWithRetry(() -> {
            if (attempts.incrementAndGet() < 3) {
                throw new IOException("transient failure");
            }
            return "success";
        }, 3, 10, "test-op");

        assertEquals("success", result);
        assertEquals(3, attempts.get());
    }

    // ===== Retries on transient errors =====

    @Test
    void executeWithRetry_retriesOnIOException() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(IOException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw new IOException("network error");
                }, 3, 10, "test-op"));

        assertEquals(3, attempts.get());
    }

    @Test
    void executeWithRetry_retriesOn5xx() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(WebClientResponseException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw WebClientResponseException.create(
                            500, "Internal Server Error",
                            HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
                }, 3, 10, "test-op"));

        assertEquals(3, attempts.get());
    }

    @Test
    void executeWithRetry_retriesOn429() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(WebClientResponseException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw WebClientResponseException.create(
                            429, "Too Many Requests",
                            HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
                }, 3, 10, "test-op"));

        assertEquals(3, attempts.get());
    }

    // ===== Fails fast on non-retryable errors =====

    @Test
    void executeWithRetry_failsFastOn4xx() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(WebClientResponseException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw WebClientResponseException.create(
                            404, "Not Found",
                            HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
                }, 3, 10, "test-op"));

        assertEquals(1, attempts.get(), "Should fail immediately on 4xx");
    }

    @Test
    void executeWithRetry_failsFastOn401() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(WebClientResponseException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw WebClientResponseException.create(
                            401, "Unauthorized",
                            HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8);
                }, 3, 10, "test-op"));

        assertEquals(1, attempts.get());
    }

    @Test
    void executeWithRetry_failsFastOnUnknownException() {
        AtomicInteger attempts = new AtomicInteger(0);

        assertThrows(IllegalArgumentException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw new IllegalArgumentException("bad arg");
                }, 3, 10, "test-op"));

        assertEquals(1, attempts.get());
    }

    // ===== Max attempts exhaustion =====

    @Test
    void executeWithRetry_exhaustsMaxAttempts() {
        AtomicInteger attempts = new AtomicInteger(0);

        IOException thrown = assertThrows(IOException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw new IOException("persistent failure");
                }, 5, 10, "test-op"));

        assertEquals(5, attempts.get());
        assertEquals("persistent failure", thrown.getMessage());
    }

    // ===== isRetryable classification =====

    @Test
    void isRetryable_ioException() {
        assertTrue(RetryUtil.isRetryable(new IOException("timeout")));
    }

    @Test
    void isRetryable_5xxWebClient() {
        assertTrue(RetryUtil.isRetryable(WebClientResponseException.create(
                502, "Bad Gateway", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8)));
        assertTrue(RetryUtil.isRetryable(WebClientResponseException.create(
                503, "Service Unavailable", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8)));
    }

    @Test
    void isRetryable_429WebClient() {
        assertTrue(RetryUtil.isRetryable(WebClientResponseException.create(
                429, "Too Many Requests", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8)));
    }

    @Test
    void isRetryable_4xxNotRetryable() {
        assertFalse(RetryUtil.isRetryable(WebClientResponseException.create(
                400, "Bad Request", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8)));
        assertFalse(RetryUtil.isRetryable(WebClientResponseException.create(
                403, "Forbidden", HttpHeaders.EMPTY, new byte[0], StandardCharsets.UTF_8)));
    }

    @Test
    void isRetryable_wrappedIOException() {
        RuntimeException wrapped = new RuntimeException("wrap", new IOException("inner"));
        assertTrue(RetryUtil.isRetryable(wrapped));
    }

    @Test
    void isRetryable_unknownException() {
        assertFalse(RetryUtil.isRetryable(new IllegalStateException("unknown")));
    }

    // ===== Exponential backoff timing =====

    @Test
    void executeWithRetry_exponentialBackoff() throws Exception {
        AtomicInteger attempts = new AtomicInteger(0);
        long startTime = System.currentTimeMillis();

        assertThrows(IOException.class, () ->
                RetryUtil.executeWithRetry(() -> {
                    attempts.incrementAndGet();
                    throw new IOException("fail");
                }, 3, 50, "backoff-test"));

        long elapsed = System.currentTimeMillis() - startTime;
        // 3 attempts with delays: 50ms (after 1st), 100ms (after 2nd) = 150ms minimum
        // Allow some slack for scheduling
        assertTrue(elapsed >= 100, "Should have backoff delays, elapsed: " + elapsed + "ms");
    }
}
