package se.devrandom.heimdall.batchprocessing;

import org.junit.jupiter.api.Test;
import org.springframework.batch.repeat.RepeatStatus;
import se.devrandom.heimdall.web.RetentionService;

import java.time.LocalDate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Unit tests for the monthly cleanup gate. The batch job runs daily; the gate must let cleanup
 * through exactly once a month (on run-on-day) and only when explicitly enabled.
 */
class RetentionCleanupTaskletTest {

    @Test
    void disabled_never_runs_even_on_the_right_day() {
        assertFalse(RetentionCleanupTasklet.shouldRun(LocalDate.of(2026, 6, 15), false, 15));
    }

    @Test
    void enabled_does_not_run_on_other_days() {
        assertFalse(RetentionCleanupTasklet.shouldRun(LocalDate.of(2026, 6, 14), true, 15));
        assertFalse(RetentionCleanupTasklet.shouldRun(LocalDate.of(2026, 6, 16), true, 15));
    }

    @Test
    void enabled_runs_on_the_configured_day() {
        assertTrue(RetentionCleanupTasklet.shouldRun(LocalDate.of(2026, 6, 15), true, 15));
    }

    @Test
    void gate_respects_a_different_configured_day() {
        assertTrue(RetentionCleanupTasklet.shouldRun(LocalDate.of(2026, 6, 1), true, 1));
        assertFalse(RetentionCleanupTasklet.shouldRun(LocalDate.of(2026, 6, 15), true, 1));
    }

    @Test
    void execute_is_a_noop_and_never_deletes_when_disabled() {
        RetentionService service = mock(RetentionService.class);
        RetentionCleanupTasklet tasklet = new RetentionCleanupTasklet(service, false, 15, 3);

        assertEquals(RepeatStatus.FINISHED, tasklet.execute(null, null));
        verifyNoInteractions(service); // disabled → execute() must not touch the retention service
    }
}
