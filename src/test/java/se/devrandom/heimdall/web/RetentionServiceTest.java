package se.devrandom.heimdall.web;

import org.junit.jupiter.api.Test;

import java.time.YearMonth;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Unit tests for the retention cutoff math. period is YYMM-encoded, so the window
 * must be computed via YearMonth — naive subtraction would corrupt it across
 * month/year boundaries. These tests pin that invariant.
 */
class RetentionServiceTest {

    @Test
    void cutoff_threeMonthsBack_sameYear() {
        // June 2026 minus 3 months = March 2026
        assertEquals(2603, RetentionService.computeCutoffPeriod(YearMonth.of(2026, 6), 3));
    }

    @Test
    void cutoff_crossesYearBoundary_januaryMinusThree() {
        // Jan 2026 minus 3 months = Oct 2025 — must NOT be 2026 - 3 = 2598
        assertEquals(2510, RetentionService.computeCutoffPeriod(YearMonth.of(2026, 1), 3));
    }

    @Test
    void cutoff_marchMinusThree_isDecemberPreviousYear() {
        assertEquals(2512, RetentionService.computeCutoffPeriod(YearMonth.of(2026, 3), 3));
    }

    @Test
    void cutoff_oneMonth() {
        assertEquals(2605, RetentionService.computeCutoffPeriod(YearMonth.of(2026, 6), 1));
    }

    @Test
    void cutoff_twelveMonths_isSameMonthPreviousYear() {
        assertEquals(2506, RetentionService.computeCutoffPeriod(YearMonth.of(2026, 6), 12));
    }

    @Test
    void yymm_periods_are_monotonic_across_year_boundary() {
        // The whole design relies on `period < cutoff` meaning "older". Confirm the
        // YYMM encoding is monotonic over a year boundary: Dec 2025 < Mar 2026.
        int dec2025 = RetentionService.computeCutoffPeriod(YearMonth.of(2026, 1), 1); // 2512
        int mar2026 = RetentionService.computeCutoffPeriod(YearMonth.of(2026, 4), 1); // 2603
        assertEquals(2512, dec2025);
        assertEquals(2603, mar2026);
        org.junit.jupiter.api.Assertions.assertTrue(dec2025 < mar2026);
    }
}
