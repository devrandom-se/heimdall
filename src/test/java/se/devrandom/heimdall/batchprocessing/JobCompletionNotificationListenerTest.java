package se.devrandom.heimdall.batchprocessing;

import org.junit.jupiter.api.Test;
import org.springframework.batch.core.BatchStatus;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Level 1: The process exit code must tell the scheduler and the alerting whether every object was backed up.
 */
class JobCompletionNotificationListenerTest {

    @Test
    void exit_code_is_0_for_a_clean_completed_job() {
        assertEquals(0, JobCompletionNotificationListener.exitCodeFor(BatchStatus.COMPLETED, false, false));
    }

    @Test
    void exit_code_is_2_when_an_object_failed_or_api_limit_was_hit() {
        assertEquals(2, JobCompletionNotificationListener.exitCodeFor(BatchStatus.COMPLETED, true, false));
        assertEquals(2, JobCompletionNotificationListener.exitCodeFor(BatchStatus.COMPLETED, false, true));
    }

    @Test
    void exit_code_is_1_for_any_non_completed_batch_status() {
        assertEquals(1, JobCompletionNotificationListener.exitCodeFor(BatchStatus.FAILED, false, false));
        assertEquals(1, JobCompletionNotificationListener.exitCodeFor(BatchStatus.STOPPED, false, false));
        assertEquals(1, JobCompletionNotificationListener.exitCodeFor(BatchStatus.UNKNOWN, true, true));
    }
}
