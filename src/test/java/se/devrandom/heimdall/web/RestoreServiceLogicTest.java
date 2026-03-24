package se.devrandom.heimdall.web;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Level 1: Pure unit tests for RestoreService static/pure methods.
 * No Spring context, no mocking, no database.
 */
class RestoreServiceLogicTest {

    // ===== formatPeriod tests =====

    @ParameterizedTest
    @CsvSource({
            "2601, Jan 2026",
            "2512, Dec 2025",
            "2506, Jun 2025",
            "2501, Jan 2025",
            "3001, Jan 2030"
    })
    void formatPeriod_normalPeriods(int period, String expected) {
        assertEquals(expected, RestoreService.formatPeriod(period));
    }

    @ParameterizedTest
    @CsvSource({
            "-2601, Archive Jan 2026",
            "-2512, Archive Dec 2025",
            "-2506, Archive Jun 2025"
    })
    void formatPeriod_archivePeriods(int period, String expected) {
        assertEquals(expected, RestoreService.formatPeriod(period));
    }

    @Test
    void formatPeriod_invalidMonth0() {
        // Month 0 is invalid, should return raw value
        assertEquals("2500", RestoreService.formatPeriod(2500));
    }

    @Test
    void formatPeriod_invalidMonth13() {
        assertEquals("2513", RestoreService.formatPeriod(2513));
    }

    // ===== calculateDiff tests =====

    private RestoreService restoreService() {
        // We only call calculateDiff which is a pure instance method
        // Can't easily construct without Spring, but it's a simple POJO method
        // Use reflection or test via the static-like behavior
        return null; // We'll test via direct instantiation workaround
    }

    @Test
    void calculateDiff_detectsChangedField() {
        RestoreService.RecordVersion newV = makeVersion(Map.of("Name", "New Name", "Email", "same@test.com"));
        RestoreService.RecordVersion oldV = makeVersion(Map.of("Name", "Old Name", "Email", "same@test.com"));

        // calculateDiff is an instance method, create minimal RestoreService
        RestoreService svc = new RestoreService();
        List<RestoreService.FieldChange> changes = svc.calculateDiff(newV, oldV);

        assertEquals(1, changes.size());
        assertEquals("Name", changes.get(0).getField());
        assertEquals("Old Name", changes.get(0).getOldValue());
        assertEquals("New Name", changes.get(0).getNewValue());
        assertEquals("diff-changed", changes.get(0).getType());
    }

    @Test
    void calculateDiff_detectsAddedField() {
        RestoreService.RecordVersion newV = makeVersion(Map.of("Name", "Test", "Phone", "12345"));
        RestoreService.RecordVersion oldV = makeVersion(Map.of("Name", "Test"));

        RestoreService svc = new RestoreService();
        List<RestoreService.FieldChange> changes = svc.calculateDiff(newV, oldV);

        assertEquals(1, changes.size());
        assertEquals("Phone", changes.get(0).getField());
        assertEquals("diff-added", changes.get(0).getType());
        assertEquals("", changes.get(0).getOldValue());
        assertEquals("12345", changes.get(0).getNewValue());
    }

    @Test
    void calculateDiff_detectsRemovedField() {
        RestoreService.RecordVersion newV = makeVersion(Map.of("Name", "Test"));
        RestoreService.RecordVersion oldV = makeVersion(Map.of("Name", "Test", "Phone", "12345"));

        RestoreService svc = new RestoreService();
        List<RestoreService.FieldChange> changes = svc.calculateDiff(newV, oldV);

        assertEquals(1, changes.size());
        assertEquals("Phone", changes.get(0).getField());
        assertEquals("diff-removed", changes.get(0).getType());
        assertEquals("12345", changes.get(0).getOldValue());
        assertEquals("", changes.get(0).getNewValue());
    }

    @Test
    void calculateDiff_noChanges() {
        Map<String, Object> data = Map.of("Name", "Same", "Email", "same@test.com");
        RestoreService.RecordVersion newV = makeVersion(data);
        RestoreService.RecordVersion oldV = makeVersion(data);

        RestoreService svc = new RestoreService();
        List<RestoreService.FieldChange> changes = svc.calculateDiff(newV, oldV);

        assertTrue(changes.isEmpty());
    }

    @Test
    void calculateDiff_nullVersionsReturnEmpty() {
        RestoreService svc = new RestoreService();
        assertTrue(svc.calculateDiff(null, makeVersion(Map.of())).isEmpty());
        assertTrue(svc.calculateDiff(makeVersion(Map.of()), null).isEmpty());
    }

    // ===== classifyRecordStatus tests =====

    @Test
    void classifyRecordStatus_activeRecord() {
        RestoreService.RecordVersion v = new RestoreService.RecordVersion();
        v.setPeriod(2601);
        v.setDeleted(false);

        RestoreService svc = new RestoreService();
        RestoreService.RecordStatus status = svc.classifyRecordStatus(List.of(v));

        assertTrue(status.isActive());
        assertFalse(status.isDeleted());
        assertFalse(status.isArchived());
    }

    @Test
    void classifyRecordStatus_deletedRecord() {
        RestoreService.RecordVersion v = new RestoreService.RecordVersion();
        v.setPeriod(2601);
        v.setDeleted(true);

        RestoreService svc = new RestoreService();
        RestoreService.RecordStatus status = svc.classifyRecordStatus(List.of(v));

        assertFalse(status.isActive());
        assertTrue(status.isDeleted());
        assertFalse(status.isArchived());
    }

    @Test
    void classifyRecordStatus_archivedRecord() {
        RestoreService.RecordVersion v = new RestoreService.RecordVersion();
        v.setPeriod(-2601);
        v.setDeleted(false);

        RestoreService svc = new RestoreService();
        RestoreService.RecordStatus status = svc.classifyRecordStatus(List.of(v));

        assertFalse(status.isActive());
        assertFalse(status.isDeleted());
        assertTrue(status.isArchived());
    }

    @Test
    void classifyRecordStatus_archivedAndActive() {
        RestoreService.RecordVersion active = new RestoreService.RecordVersion();
        active.setPeriod(2601);
        active.setDeleted(false);

        RestoreService.RecordVersion archived = new RestoreService.RecordVersion();
        archived.setPeriod(-2601);
        archived.setDeleted(false);

        RestoreService svc = new RestoreService();
        // List is ordered by period DESC, so active comes first
        RestoreService.RecordStatus status = svc.classifyRecordStatus(List.of(active, archived));

        assertTrue(status.isActive());
        assertFalse(status.isDeleted());
        assertTrue(status.isArchived());
    }

    @Test
    void classifyRecordStatus_emptyVersions() {
        RestoreService svc = new RestoreService();
        RestoreService.RecordStatus status = svc.classifyRecordStatus(List.of());
        assertFalse(status.isActive());
        assertFalse(status.isDeleted());
        assertFalse(status.isArchived());
    }

    @Test
    void classifyRecordStatus_nullVersions() {
        RestoreService svc = new RestoreService();
        RestoreService.RecordStatus status = svc.classifyRecordStatus(null);
        assertFalse(status.isActive());
        assertFalse(status.isDeleted());
        assertFalse(status.isArchived());
    }

    // ===== RelatedFile.getFormattedSize tests =====

    @Test
    void formattedSize_bytes() {
        RestoreService.RelatedFile f = new RestoreService.RelatedFile();
        f.setContentSize("500");
        assertEquals("500 B", f.getFormattedSize());
    }

    @Test
    void formattedSize_kilobytes() {
        RestoreService.RelatedFile f = new RestoreService.RelatedFile();
        f.setContentSize("2048");
        assertTrue(f.getFormattedSize().matches("2[.,]0 KB"));
    }

    @Test
    void formattedSize_megabytes() {
        RestoreService.RelatedFile f = new RestoreService.RelatedFile();
        f.setContentSize(String.valueOf(5 * 1024 * 1024));
        assertTrue(f.getFormattedSize().matches("5[.,]0 MB"));
    }

    @Test
    void formattedSize_gigabytes() {
        RestoreService.RelatedFile f = new RestoreService.RelatedFile();
        f.setContentSize(String.valueOf(2L * 1024 * 1024 * 1024));
        assertTrue(f.getFormattedSize().matches("2[.,]0 GB"));
    }

    @Test
    void formattedSize_nullOrEmpty() {
        RestoreService.RelatedFile f = new RestoreService.RelatedFile();
        f.setContentSize(null);
        assertEquals("", f.getFormattedSize());
        f.setContentSize("");
        assertEquals("", f.getFormattedSize());
    }

    @Test
    void formattedSize_nonNumeric() {
        RestoreService.RelatedFile f = new RestoreService.RelatedFile();
        f.setContentSize("not_a_number");
        assertEquals("not_a_number", f.getFormattedSize());
    }

    // ===== DeletedRecord convenience methods =====

    @Test
    void deletedRecord_isArchived() {
        RestoreService.DeletedRecord r = new RestoreService.DeletedRecord();
        r.setPeriod(-2601);
        assertTrue(r.isArchived());
        r.setPeriod(2601);
        assertFalse(r.isArchived());
    }

    @Test
    void deletedRecord_isActive() {
        RestoreService.DeletedRecord r = new RestoreService.DeletedRecord();
        r.setPeriod(2601);
        r.setDeleted(false);
        assertTrue(r.isActive());
        r.setDeleted(true);
        assertFalse(r.isActive());
        r.setDeleted(false);
        r.setPeriod(-2601);
        assertFalse(r.isActive());
    }

    @Test
    void deletedRecord_formattedPeriod() {
        RestoreService.DeletedRecord r = new RestoreService.DeletedRecord();
        r.setPeriod(2601);
        assertEquals("Jan 2026", r.getFormattedPeriod());
    }

    // ===== RecordVersion convenience methods =====

    @Test
    void recordVersion_formattedPeriod() {
        RestoreService.RecordVersion v = new RestoreService.RecordVersion();
        v.setPeriod(2512);
        assertEquals("Dec 2025", v.getFormattedPeriod());
    }

    @Test
    void recordVersion_isArchivePeriod() {
        RestoreService.RecordVersion v = new RestoreService.RecordVersion();
        v.setPeriod(-2601);
        assertTrue(v.isArchivePeriod());
        v.setPeriod(2601);
        assertFalse(v.isArchivePeriod());
    }

    // ===== Helper methods =====

    private RestoreService.RecordVersion makeVersion(Map<String, Object> metadata) {
        RestoreService.RecordVersion v = new RestoreService.RecordVersion();
        v.setMetadata(new LinkedHashMap<>(metadata));
        return v;
    }
}
