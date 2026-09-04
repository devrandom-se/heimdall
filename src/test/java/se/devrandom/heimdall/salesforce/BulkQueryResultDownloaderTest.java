package se.devrandom.heimdall.salesforce;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.http.HttpStatus;
import org.springframework.web.reactive.function.client.ClientRequest;
import org.springframework.web.reactive.function.client.ClientResponse;
import org.springframework.web.reactive.function.client.ExchangeFunction;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Mono;
import se.devrandom.heimdall.salesforce.objects.BulkQueryRequest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Level 1: Unit tests for the Bulk API result download with a scripted fake HTTP exchange.
 * Pins the invariant that a chunk which fails validation never reaches the assembled CSV,
 * so a retry can neither duplicate rows nor lose the file built so far.
 */
class BulkQueryResultDownloaderTest {

    private static final String HEADER = "Name,SystemModstamp,Id\n";
    private static final String ROW1 = "Alpha,2026-01-15T10:30:00.000Z,001000000000001AAA\n";
    private static final String ROW2 = "Beta,2026-01-15T10:31:00.000Z,001000000000002AAA\n";
    private static final String ROW3 = "Gamma,2026-01-15T10:32:00.000Z,001000000000003AAA\n";
    private static final String TRUNCATED_ROW = "Beta,2026-01-15T10:31:00.000Z,0010000";

    @TempDir
    Path tempDir;

    private final Deque<ClientResponse> responses = new ArrayDeque<>();
    private final List<ClientRequest> requests = new ArrayList<>();

    private BulkQueryResultDownloader downloader() {
        ExchangeFunction fake = request -> {
            requests.add(request);
            return Mono.just(responses.pop());
        };
        WebClient client = WebClient.builder().baseUrl("https://sf.example").exchangeFunction(fake).build();
        return new BulkQueryResultDownloader(client, "v60.0", () -> "token", Optional.empty(),
                name -> 2000, tempDir, 0);
    }

    private static ClientResponse chunk(String locator, int numberOfRecords, String body) {
        return ClientResponse.create(HttpStatus.OK)
                .header("Sforce-Locator", locator)
                .header("Sforce-NumberOfRecords", String.valueOf(numberOfRecords))
                .body(body)
                .build();
    }

    private static BulkQueryRequest finishedJob() {
        BulkQueryRequest job = new BulkQueryRequest();
        job.id = "750TEST";
        job.object = "Account";
        job.operation = "query";
        job.state = "JobComplete";
        return job;
    }

    private List<String> tempFiles() throws IOException {
        try (Stream<Path> files = Files.list(tempDir)) {
            return files.map(p -> p.getFileName().toString()).sorted().toList();
        }
    }

    @Test
    void two_valid_chunks_are_assembled_in_order_without_duplicates() throws IOException {
        responses.add(chunk("LOC2", 2, HEADER + ROW1 + ROW2));
        responses.add(chunk("null", 1, HEADER + ROW3));

        CsvDownloadResult result = downloader().download(finishedJob(), 0);

        assertEquals(HEADER + ROW1 + ROW2 + ROW3, Files.readString(result.csvPath));
        assertEquals(3, result.recordCount);
        assertEquals("001000000000003AAA", result.lastId);
        assertFalse(result.hasMoreRecords);
        assertEquals(List.of("750TEST.csv"), tempFiles());
        assertEquals(2, requests.size());
        assertTrue(requests.get(1).url().toString().contains("locator=LOC2"));
    }

    @Test
    void truncated_second_chunk_is_retried_without_duplicating_the_first() throws IOException {
        responses.add(chunk("LOC2", 2, HEADER + ROW1 + ROW2));
        responses.add(chunk("null", 1, HEADER + TRUNCATED_ROW));   // fails validation (bad Id)
        responses.add(chunk("null", 1, HEADER + ROW3));

        CsvDownloadResult result = downloader().download(finishedJob(), 0);

        assertEquals(HEADER + ROW1 + ROW2 + ROW3, Files.readString(result.csvPath));
        assertEquals(3, result.recordCount);
        assertEquals(3, requests.size());
        assertTrue(requests.get(2).url().toString().contains("locator=LOC2"), "retry re-requests the same chunk");
        assertEquals(List.of("750TEST.csv"), tempFiles());
    }

    @Test
    void truncated_first_chunk_is_retried_and_base_is_built_from_the_valid_copy() throws IOException {
        responses.add(chunk("LOC2", 2, HEADER + ROW1 + TRUNCATED_ROW));   // fails validation
        responses.add(chunk("LOC2", 2, HEADER + ROW1 + ROW2));
        responses.add(chunk("null", 1, HEADER + ROW3));

        CsvDownloadResult result = downloader().download(finishedJob(), 0);

        assertEquals(HEADER + ROW1 + ROW2 + ROW3, Files.readString(result.csvPath));
        assertEquals(3, result.recordCount);
        assertFalse(requests.get(1).url().toString().contains("locator="), "first chunk is re-requested without a locator");
        assertEquals(List.of("750TEST.csv"), tempFiles());
    }

    @Test
    void row_count_mismatch_with_sforce_header_is_treated_as_truncation() throws IOException {
        responses.add(chunk("null", 2, HEADER + ROW1));   // header promises 2 rows, body has 1
        responses.add(chunk("null", 2, HEADER + ROW1 + ROW2));

        CsvDownloadResult result = downloader().download(finishedJob(), 0);

        assertEquals(HEADER + ROW1 + ROW2, Files.readString(result.csvPath));
        assertEquals(2, requests.size());
    }

    @Test
    void validation_failure_after_max_attempts_propagates_and_leaves_no_temp_files() throws IOException {
        responses.add(chunk("LOC2", 2, HEADER + ROW1 + ROW2));
        responses.add(chunk("null", 1, HEADER + TRUNCATED_ROW));
        responses.add(chunk("null", 1, HEADER + TRUNCATED_ROW));
        responses.add(chunk("null", 1, HEADER + TRUNCATED_ROW));

        BulkQueryResultDownloader downloader = downloader();
        assertThrows(BulkQueryResultDownloader.CsvValidationException.class,
                () -> downloader.download(finishedJob(), 0));

        assertEquals(4, requests.size());
        assertEquals(List.of(), tempFiles());
    }

    @Test
    void http_error_message_carries_status_and_body() throws IOException {
        responses.add(ClientResponse.create(HttpStatus.BAD_REQUEST)
                .header("Content-Type", "application/json")
                .body("[{\"message\":\"Invalid locator\"}]")
                .build());

        BulkQueryResultDownloader downloader = downloader();
        RuntimeException ex = assertThrows(RuntimeException.class, () -> downloader.download(finishedJob(), 0));

        assertTrue(ex.getMessage().contains("400"), ex.getMessage());
        assertTrue(ex.getMessage().contains("Invalid locator"), ex.getMessage());
        assertEquals(List.of(), tempFiles());
    }
}
