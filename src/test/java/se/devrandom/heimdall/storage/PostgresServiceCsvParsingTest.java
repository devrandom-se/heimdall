package se.devrandom.heimdall.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Level 1: Pure unit tests for PostgresService CSV parsing and utility methods.
 * No Spring context, no mocking, no database.
 */
class PostgresServiceCsvParsingTest {

    private PostgresService service;

    @BeforeEach
    void setUp() {
        // Construct with dummy values - we only test pure methods
        service = new PostgresService(
                "jdbc:postgresql://localhost/test", "user", "pass",
                "00D000000000001", null, java.util.Optional.empty());
    }

    // ===== parseCsvLine tests =====

    @Test
    void parseCsvLine_simpleFields() {
        String[] result = service.parseCsvLine("Id,Name,Email");
        assertArrayEquals(new String[]{"Id", "Name", "Email"}, result);
    }

    @Test
    void parseCsvLine_quotedFieldWithComma() {
        String[] result = service.parseCsvLine("Id,\"Smith, John\",Email");
        assertEquals(3, result.length);
        assertEquals("Id", result[0]);
        assertEquals("Smith, John", result[1]);
        assertEquals("Email", result[2]);
    }

    @Test
    void parseCsvLine_emptyFields() {
        String[] result = service.parseCsvLine("Id,,Email,");
        assertEquals(4, result.length);
        assertEquals("Id", result[0]);
        assertEquals("", result[1]);
        assertEquals("Email", result[2]);
        assertEquals("", result[3]);
    }

    @Test
    void parseCsvLine_singleField() {
        String[] result = service.parseCsvLine("OnlyField");
        assertArrayEquals(new String[]{"OnlyField"}, result);
    }

    @Test
    void parseCsvLine_emptyString() {
        String[] result = service.parseCsvLine("");
        assertArrayEquals(new String[]{""}, result);
    }

    @Test
    void parseCsvLine_multipleQuotedFields() {
        String[] result = service.parseCsvLine("\"A,B\",\"C,D\",\"E\"");
        assertEquals(3, result.length);
        assertEquals("A,B", result[0]);
        assertEquals("C,D", result[1]);
        assertEquals("E", result[2]);
    }

    // ===== readCsvLine tests =====

    @Test
    void readCsvLine_simpleLine() throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader("line1\nline2\n"));
        assertEquals("line1", service.readCsvLine(reader));
        assertEquals("line2", service.readCsvLine(reader));
    }

    @Test
    void readCsvLine_multilineQuotedField() throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(
                "\"multi\nline\",value\nnextline\n"));
        String line1 = service.readCsvLine(reader);
        // The quoted newline should be preserved within the line
        assertTrue(line1.contains("value"));
        assertEquals("nextline", service.readCsvLine(reader));
    }

    @Test
    void readCsvLine_eofReturnsNull() throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader(""));
        assertNull(service.readCsvLine(reader));
    }

    @Test
    void readCsvLine_lastLineNoNewline() throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader("lastline"));
        assertEquals("lastline", service.readCsvLine(reader));
        assertNull(service.readCsvLine(reader));
    }

    @Test
    void readCsvLine_carriageReturnStripped() throws IOException {
        BufferedReader reader = new BufferedReader(new StringReader("hello\r\nworld\r\n"));
        assertEquals("hello", service.readCsvLine(reader));
        assertEquals("world", service.readCsvLine(reader));
    }

    // ===== findColumnIndex tests =====

    @Test
    void findColumnIndex_found() {
        String[] headers = {"Id", "Name", "Email"};
        assertEquals(1, service.findColumnIndex(headers, "Name"));
    }

    @Test
    void findColumnIndex_caseInsensitive() {
        String[] headers = {"id", "NAME", "email"};
        assertEquals(0, service.findColumnIndex(headers, "Id"));
        assertEquals(1, service.findColumnIndex(headers, "name"));
    }

    @Test
    void findColumnIndex_notFound() {
        String[] headers = {"Id", "Name"};
        assertEquals(-1, service.findColumnIndex(headers, "Missing"));
    }

    // ===== findBestNameIndex tests =====

    @Test
    void findBestNameIndex_prefersName() {
        String[] headers = {"Id", "Subject", "Name", "Title"};
        assertEquals(2, service.findBestNameIndex(headers));
    }

    @Test
    void findBestNameIndex_fallsBackToSubject() {
        String[] headers = {"Id", "Subject", "Email"};
        assertEquals(1, service.findBestNameIndex(headers));
    }

    @Test
    void findBestNameIndex_fallsBackToTitle() {
        String[] headers = {"Id", "Title", "Email"};
        assertEquals(1, service.findBestNameIndex(headers));
    }

    @Test
    void findBestNameIndex_fallsBackToCaseNumber() {
        String[] headers = {"Id", "CaseNumber", "Email"};
        assertEquals(1, service.findBestNameIndex(headers));
    }

    @Test
    void findBestNameIndex_returnsNegativeWhenNoneFound() {
        String[] headers = {"Id", "Email", "Phone"};
        assertEquals(-1, service.findBestNameIndex(headers));
    }

    // ===== looksLikeSalesforceId tests =====

    @Test
    void looksLikeSalesforceId_valid15Char() {
        assertTrue(service.looksLikeSalesforceId("001000000000001"));
    }

    @Test
    void looksLikeSalesforceId_valid18Char() {
        assertTrue(service.looksLikeSalesforceId("001000000000001AAA"));
    }

    @Test
    void looksLikeSalesforceId_alphanumeric() {
        assertTrue(service.looksLikeSalesforceId("a0B5g00000AbCdE"));
    }

    @Test
    void looksLikeSalesforceId_tooShort() {
        assertFalse(service.looksLikeSalesforceId("00100000"));
    }

    @Test
    void looksLikeSalesforceId_tooLong() {
        assertFalse(service.looksLikeSalesforceId("0010000000000010000"));
    }

    @Test
    void looksLikeSalesforceId_wrongLength16() {
        assertFalse(service.looksLikeSalesforceId("0010000000000010"));
    }

    @Test
    void looksLikeSalesforceId_specialChars() {
        assertFalse(service.looksLikeSalesforceId("001-000-000-0001"));
    }

    @Test
    void looksLikeSalesforceId_null() {
        assertFalse(service.looksLikeSalesforceId(null));
    }

    @Test
    void looksLikeSalesforceId_empty() {
        assertFalse(service.looksLikeSalesforceId(""));
    }

    // ===== getCurrentPeriod / getArchivePeriod tests =====

    @Test
    void getCurrentPeriod_returnsYYMM() {
        int period = service.getCurrentPeriod();
        // Should be a 4-digit number: YYMM
        assertTrue(period >= 2500 && period <= 9912,
                "Period should be YYMM format, got: " + period);
        int month = period % 100;
        assertTrue(month >= 1 && month <= 12,
                "Month part should be 1-12, got: " + month);
    }

    @Test
    void getArchivePeriod_isNegative() {
        int archivePeriod = service.getArchivePeriod();
        assertTrue(archivePeriod < 0, "Archive period should be negative");
        assertEquals(-service.getCurrentPeriod(), archivePeriod);
    }
}
