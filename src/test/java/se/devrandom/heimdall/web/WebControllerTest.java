package se.devrandom.heimdall.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.ui.ConcurrentModel;
import org.springframework.ui.Model;
import se.devrandom.heimdall.storage.S3Service;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Level 2: Unit tests for WebController with mocked dependencies.
 * Uses Mockito without Spring context.
 */
@ExtendWith(MockitoExtension.class)
class WebControllerTest {

    @Mock
    private RestoreService restoreService;

    @Mock
    private S3Service s3Service;

    @InjectMocks
    private WebController controller;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(controller, "demoMode", false);
    }

    // ===== /health =====

    @Test
    void health_returnsOk() {
        assertEquals("OK", controller.health());
    }

    // ===== / (index) =====

    @Test
    void index_populatesModelWithStats() {
        Map<String, RestoreService.ObjectStats> stats = new LinkedHashMap<>();
        RestoreService.ObjectStats accountStats = new RestoreService.ObjectStats();
        accountStats.setObjectName("Account");
        accountStats.setUniqueRecords(100);
        accountStats.setDeletedCount(5);
        accountStats.setArchivedCount(10);
        stats.put("Account", accountStats);

        when(restoreService.getObjectStatistics()).thenReturn(stats);

        Model model = new ConcurrentModel();
        String view = controller.index(model);

        assertEquals("index", view);
        @SuppressWarnings("unchecked")
        Map<String, Object> modelStats = (Map<String, Object>) model.getAttribute("stats");
        assertNotNull(modelStats);
        assertEquals(1, modelStats.get("totalObjects"));
        assertEquals(100, modelStats.get("totalRecords"));
        assertEquals(5, modelStats.get("deletedRecords"));
        assertEquals(10, modelStats.get("archivedRecords"));
    }

    // ===== /objects =====

    @Test
    void objectBrowser_returnsObjectsView() {
        when(restoreService.getObjectStatistics()).thenReturn(new LinkedHashMap<>());

        Model model = new ConcurrentModel();
        String view = controller.objectBrowser(model);

        assertEquals("objects", view);
        assertNotNull(model.getAttribute("objectStats"));
    }

    // ===== /deleted =====

    @Test
    void deletedRecords_noObject_showsList() {
        when(restoreService.getObjectsWithDeletedRecords()).thenReturn(List.of("Account", "Contact"));

        Model model = new ConcurrentModel();
        String view = controller.deletedRecords(null, 0, model);

        assertEquals("deleted", view);
        assertEquals(List.of("Account", "Contact"), model.getAttribute("objects"));
        assertEquals(List.of(), model.getAttribute("records"));
    }

    @Test
    void deletedRecords_withObject_showsRecords() {
        when(restoreService.getObjectsWithDeletedRecords()).thenReturn(List.of("Account"));
        RestoreService.DeletedRecord r = new RestoreService.DeletedRecord();
        r.setId("001");
        r.setName("Test");
        when(restoreService.getDeletedRecords("Account", 0, 50)).thenReturn(List.of(r));
        when(restoreService.countDeletedRecords("Account")).thenReturn(1);

        Model model = new ConcurrentModel();
        String view = controller.deletedRecords("Account", 0, model);

        assertEquals("deleted", view);
        assertEquals("Account", model.getAttribute("selectedObject"));
        @SuppressWarnings("unchecked")
        List<RestoreService.DeletedRecord> records = (List<RestoreService.DeletedRecord>) model.getAttribute("records");
        assertEquals(1, records.size());
    }

    // ===== /archived =====

    @Test
    void archivedRecords_noObject_showsList() {
        when(restoreService.getObjectsWithArchivedRecords()).thenReturn(List.of("Case"));

        Model model = new ConcurrentModel();
        String view = controller.archivedRecords(null, 0, model);

        assertEquals("archived", view);
        assertEquals(List.of("Case"), model.getAttribute("objects"));
    }

    // ===== /records =====

    @Test
    void recordsForObject_populatesModel() {
        when(restoreService.getRecordsForObject("Account", 0, 50)).thenReturn(List.of());
        Map<String, RestoreService.ObjectStats> stats = new LinkedHashMap<>();
        RestoreService.ObjectStats s = new RestoreService.ObjectStats();
        s.setObjectName("Account");
        s.setUniqueRecords(200);
        stats.put("Account", s);
        when(restoreService.getObjectStatistics()).thenReturn(stats);

        Model model = new ConcurrentModel();
        String view = controller.recordsForObject("Account", 0, model);

        assertEquals("records", view);
        assertEquals("Account", model.getAttribute("objectName"));
        assertEquals(200, model.getAttribute("totalCount"));
        assertEquals(4, model.getAttribute("totalPages")); // ceil(200/50)
    }

    // ===== /record =====

    @Test
    void recordHistory_noVersions() {
        when(restoreService.getRecordVersions("001X")).thenReturn(List.of());
        when(restoreService.classifyRecordStatus(List.of())).thenReturn(
                new RestoreService.RecordStatus(false, false, false));
        when(restoreService.getRestoreHistory("001X")).thenReturn(List.of());

        Model model = new ConcurrentModel();
        String view = controller.recordHistory("001X", 0, model);

        assertEquals("record", view);
        assertEquals("001X", model.getAttribute("recordId"));
    }

    @Test
    void recordHistory_withVersions() {
        RestoreService.RecordVersion v = new RestoreService.RecordVersion();
        v.setId("001X");
        v.setPeriod(2601);
        v.setObjectName("Account");
        v.setName("Test Account");
        v.setMetadata(Map.of("Name", "Test Account"));
        v.setDeleted(false);

        List<RestoreService.RecordVersion> versions = List.of(v);
        when(restoreService.getRecordVersions("001X")).thenReturn(versions);
        when(restoreService.classifyRecordStatus(versions)).thenReturn(
                new RestoreService.RecordStatus(true, false, false));
        when(restoreService.getRelatedFiles("001X")).thenReturn(List.of());
        when(restoreService.getRelatedChildRecords("001X")).thenReturn(List.of());
        when(restoreService.getRestoreHistory("001X")).thenReturn(List.of());

        Model model = new ConcurrentModel();
        String view = controller.recordHistory("001X", 0, model);

        assertEquals("record", view);
        assertEquals("Account", model.getAttribute("objectName"));
        assertEquals("Test Account", model.getAttribute("recordName"));
    }

    // ===== /api/compare =====

    @Test
    void compareRecord_missingParams() {
        var response = controller.compareRecord(Map.of());
        assertEquals(400, response.getStatusCode().value());
    }

    @Test
    void compareRecord_sandboxNotConfigured() {
        ReflectionTestUtils.setField(controller, "sandboxRestoreClient", null);

        var response = controller.compareRecord(Map.of("recordId", "001X", "period", 2601));
        assertEquals(503, response.getStatusCode().value());
    }

    @Test
    void compareRecord_demoMode() {
        ReflectionTestUtils.setField(controller, "sandboxRestoreClient", null);
        ReflectionTestUtils.setField(controller, "demoMode", true);

        var response = controller.compareRecord(Map.of("recordId", "001X", "period", 2601));
        assertEquals(503, response.getStatusCode().value());
        assertTrue(response.getBody().get("error").toString().contains("demo mode"));
    }

    // ===== /api/restore =====

    @Test
    void restoreRecord_missingParams() {
        var response = controller.restoreRecord(Map.of());
        assertEquals(400, response.getStatusCode().value());
    }

    @Test
    void restoreRecord_sandboxNotConfigured() {
        ReflectionTestUtils.setField(controller, "sandboxRestoreClient", null);

        var response = controller.restoreRecord(Map.of("recordId", "001X", "period", 2601));
        assertEquals(503, response.getStatusCode().value());
    }

    @Test
    void restoreRecord_demoMode() {
        ReflectionTestUtils.setField(controller, "sandboxRestoreClient", null);
        ReflectionTestUtils.setField(controller, "demoMode", true);

        var response = controller.restoreRecord(Map.of("recordId", "001X", "period", 2601));
        assertEquals(503, response.getStatusCode().value());
        assertTrue(response.getBody().get("error").toString().contains("demo mode"));
    }

    @Test
    void restoreRecord_recordNotFound() {
        SandboxRestoreClient mockClient = mock(SandboxRestoreClient.class);
        when(mockClient.isConfigured()).thenReturn(true);
        ReflectionTestUtils.setField(controller, "sandboxRestoreClient", mockClient);

        when(restoreService.getRecordVersions("001X")).thenReturn(List.of());

        var response = controller.restoreRecord(Map.of("recordId", "001X", "period", 2601));
        assertEquals(404, response.getStatusCode().value());
    }
}
